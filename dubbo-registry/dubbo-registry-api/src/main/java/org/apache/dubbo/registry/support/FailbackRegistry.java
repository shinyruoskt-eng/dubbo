/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.registry.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.ProviderFirstParams;
import org.apache.dubbo.registry.retry.FailedRegisteredTask;
import org.apache.dubbo.registry.retry.FailedSubscribedTask;
import org.apache.dubbo.registry.retry.FailedUnregisteredTask;
import org.apache.dubbo.registry.retry.FailedUnsubscribedTask;
import org.apache.dubbo.remoting.Constants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.IS_EXTRA;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_FAILED_NOTIFY_EVENT;
import static org.apache.dubbo.registry.Constants.DEFAULT_REGISTRY_RETRY_PERIOD;
import static org.apache.dubbo.registry.Constants.REGISTRY_RETRY_PERIOD_KEY;

/**
 * ============================================================================
 * 文件概述：提供自动失败重试能力的注册中心抽象模板类
 * ============================================================================
 * 
 * 本文件在AbstractRegistry基础上增加了失败自动重试机制，是Zookeeper、Nacos等
 * 注册中心实现的直接父类。当注册/订阅操作失败时（网络故障、注册中心暂时不可用），
 * 会自动在后台定时重试，直到成功为止，大大提升了系统的容错能力。
 * 
 * 【关键】核心设计理念：
 * 
 * 1. 失败重试vs立即失败：
 *    立即失败（AbstractRegistry）：操作失败立即抛异常（check=true时）
 *    失败重试（FailbackRegistry）：操作失败记录任务，后台定时重试
 *    设计哲学："网络故障是暂时的，重试能解决大多数问题"
 * 
 * 2. 四种失败任务独立管理：
 *    - FailedRegisteredTask：注册失败任务
 *    - FailedUnregisteredTask：注销失败任务
 *    - FailedSubscribedTask：订阅失败任务
 *    - FailedUnsubscribedTask：取消订阅失败任务
 *    原因：不同操作的重试逻辑可能不同，独立管理更灵活
 * 
 * 3. 时间轮定时器（HashedWheelTimer）：
 *    - 高效：O(1)时间复杂度添加/删除任务
 *    - 精度：毫秒级定时精度
 *    - 内存：128个时间槽，适合中等数量的任务
 *    - 对比：相比JDK的ScheduledExecutorService，内存和CPU开销更小
 * 
 * 【困难】重试机制详解：
 * 
 * 重试流程（以注册为例）：
 * <pre>
 * register(url)调用失败
 *   ↓
 * addFailedRegistered(url)创建重试任务
 *   ↓
 * retryTimer.newTimeout(task, 5秒)调度任务
 *   ↓
 * 5秒后执行task.run() -> doRegister(url)
 *   ↓
 * 成功：从failedRegistered移除任务
 * 失败：再次调度5秒后重试（无限循环）
 * </pre>
 * 
 * 任务去重策略：
 * - 同一个URL只能有一个重试任务
 * - 使用putIfAbsent保证并发安全
 * - 重复添加会被忽略（避免重复重试）
 * 
 * 任务取消条件：
 * - 手动调用成功（如register成功）
 * - 调用反向操作（如先register失败，后调用unregister）
 * - Registry销毁
 * 
 * 【中等】时间轮参数说明：
 * 
 * retryPeriod（重试周期）：
 * - 默认值：5000ms（5秒）
 * - 配置：registry.retry.period参数
 * - 建议：不要设置太小（避免过度重试给注册中心压力）
 * 
 * ticksPerWheel（时间槽数量）：
 * - 固定值：128
 * - 含义：时间轮被分为128个槽
 * - 精度：tickDuration = retryPeriod / 128
 * - 权衡：槽数越多精度越高，但内存占用越大
 * 
 * 【中等】典型使用场景：
 * 
 * 场景1：注册中心临时故障
 * <pre>
 * Provider启动 -> register(url)
 *   ↓
 * Zookeeper连接超时（网络抖动）
 *   ↓
 * addFailedRegistered(url)启动重试
 *   ↓
 * 5秒后网络恢复，重试成功
 * </pre>
 * 
 * 场景2：注册中心宕机
 * <pre>
 * Provider启动 -> register(url)
 *   ↓
 * Zookeeper不可用
 *   ↓
 * 后台持续重试（每5秒一次）
 *   ↓
 * Zookeeper恢复后自动注册成功
 * </pre>
 * 
 * 场景3：订阅重试
 * <pre>
 * Consumer启动 -> subscribe(url, listener)
 *   ↓
 * 订阅失败（注册中心压力大）
 *   ↓
 * 后台重试 -> 最终订阅成功 -> 收到Provider列表
 * </pre>
 * 
 * 【关键】与AbstractRegistry的协作：
 * 
 * AbstractRegistry职责：
 * - 本地缓存管理
 * - 文件持久化
 * - 通知分发
 * 
 * FailbackRegistry职责：
 * - 失败重试
 * - 重试任务管理
 * - 定时器调度
 * 
 * 子类（如ZookeeperRegistry）职责：
 * - 实现doRegister/doUnregister/doSubscribe/doUnsubscribe
 * - 与具体注册中心交互
 * 
 * 【中等】性能和资源消耗：
 * 
 * 内存开销：
 * - 每个失败任务：~200字节（Task对象 + URL + Listener）
 * - HashedWheelTimer：~10KB（128个槽）
 * - 正常情况下失败任务很少，内存影响可忽略
 * 
 * CPU开销：
 * - 时间轮运转：恒定开销，与任务数量无关
 * - 重试执行：取决于网络IO（连接注册中心）
 * 
 * (SPI, Prototype, ThreadSafe)
 */
public abstract class FailbackRegistry extends AbstractRegistry {

    // ===== 失败重试任务映射（四种独立的任务类型） =====

    /**
     * 【关键】注册失败任务映射
     * 结构：URL -> FailedRegisteredTask
     * 场景：register(url)失败时添加，成功后移除
     * 去重：同一URL只保留一个任务
     */
    private final ConcurrentMap<URL, FailedRegisteredTask> failedRegistered = new ConcurrentHashMap<>();

    /**
     * 【关键】注销失败任务映射
     * 结构：URL -> FailedUnregisteredTask
     * 场景：unregister(url)失败时添加
     * 特殊处理：如果URL有注册失败任务，会先取消注册任务
     */
    private final ConcurrentMap<URL, FailedUnregisteredTask> failedUnregistered = new ConcurrentHashMap<>();

    /**
     * 【关键】订阅失败任务映射
     * 结构：Holder(URL, Listener) -> FailedSubscribedTask
     * 场景：subscribe(url, listener)失败时添加
     * 键设计：使用Holder包装URL和Listener，因为同一URL可能有多个Listener
     */
    private final ConcurrentMap<Holder, FailedSubscribedTask> failedSubscribed = new ConcurrentHashMap<>();

    /**
     * 【关键】取消订阅失败任务映射
     * 结构：Holder(URL, Listener) -> FailedUnsubscribedTask
     * 场景：unsubscribe(url, listener)失败时添加
     */
    private final ConcurrentMap<Holder, FailedUnsubscribedTask> failedUnsubscribed = new ConcurrentHashMap<>();

    /**
     * 【中等】重试周期（毫秒）
     * 默认值：5000ms（5秒）
     * 配置项：registry.retry.period
     */
    private final int retryPeriod;

    /**
     * 【困难】时间轮定时器：用于调度重试任务
     * 
     * 工作原理：
     * 1. 时间轮分为128个槽（ticksPerWheel=128）
     * 2. 指针每隔tickDuration移动一格
     * 3. 任务添加到对应槽位，指针扫到时执行
     * 
     * 性能特点：
     * - 添加任务：O(1)
     * - 删除任务：O(1)
     * - 执行任务：O(1)（每个槽位的任务数量很少）
     * 
     * 线程模型：
     * - 单线程执行任务（DubboRegistryRetryTimer线程）
     * - 守护线程：JVM退出时自动停止
     */
    private final HashedWheelTimer retryTimer;

    public FailbackRegistry(URL url) {
        super(url);
        this.retryPeriod = url.getParameter(REGISTRY_RETRY_PERIOD_KEY, DEFAULT_REGISTRY_RETRY_PERIOD);

        // since the retry task will not be very much. 128 ticks is enough.
        retryTimer = new HashedWheelTimer(
                new NamedThreadFactory("DubboRegistryRetryTimer", true), retryPeriod, TimeUnit.MILLISECONDS, 128);
    }

    public void removeFailedRegisteredTask(URL url) {
        failedRegistered.remove(url);
    }

    public void removeFailedUnregisteredTask(URL url) {
        failedUnregistered.remove(url);
    }

    public void removeFailedSubscribedTask(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        failedSubscribed.remove(h);
    }

    public void removeFailedUnsubscribedTask(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        failedUnsubscribed.remove(h);
    }

    private void addFailedRegistered(URL url) {
        FailedRegisteredTask oldOne = failedRegistered.get(url);
        if (oldOne != null) {
            return;
        }
        FailedRegisteredTask newTask = new FailedRegisteredTask(url, this);
        oldOne = failedRegistered.putIfAbsent(url, newTask);
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    private void removeFailedRegistered(URL url) {
        FailedRegisteredTask f = failedRegistered.remove(url);
        if (f != null) {
            f.cancel();
        }
    }

    private void addFailedUnregistered(URL url) {
        FailedUnregisteredTask oldOne = failedUnregistered.get(url);
        if (oldOne != null) {
            return;
        }
        FailedUnregisteredTask newTask = new FailedUnregisteredTask(url, this);
        oldOne = failedUnregistered.putIfAbsent(url, newTask);
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    private void removeFailedUnregistered(URL url) {
        FailedUnregisteredTask f = failedUnregistered.remove(url);
        if (f != null) {
            f.cancel();
        }
    }

    protected void addFailedSubscribed(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        FailedSubscribedTask oldOne = failedSubscribed.get(h);
        if (oldOne != null) {
            return;
        }
        FailedSubscribedTask newTask = new FailedSubscribedTask(url, this, listener);
        oldOne = failedSubscribed.putIfAbsent(h, newTask);
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    public void removeFailedSubscribed(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        FailedSubscribedTask f = failedSubscribed.remove(h);
        if (f != null) {
            f.cancel();
        }
        removeFailedUnsubscribed(url, listener);
    }

    private void addFailedUnsubscribed(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        FailedUnsubscribedTask oldOne = failedUnsubscribed.get(h);
        if (oldOne != null) {
            return;
        }
        FailedUnsubscribedTask newTask = new FailedUnsubscribedTask(url, this, listener);
        oldOne = failedUnsubscribed.putIfAbsent(h, newTask);
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    private void removeFailedUnsubscribed(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        FailedUnsubscribedTask f = failedUnsubscribed.remove(h);
        if (f != null) {
            f.cancel();
        }
    }

    protected URL removeParamsFromConsumer(URL consumer) {
        Set<ProviderFirstParams> providerFirstParams = consumer.getOrDefaultApplicationModel()
                .getExtensionLoader(ProviderFirstParams.class)
                .getSupportedExtensionInstances();
        if (CollectionUtils.isEmpty(providerFirstParams)) {
            return consumer;
        }

        for (ProviderFirstParams paramsFilter : providerFirstParams) {
            consumer = consumer.removeParameters(paramsFilter.params());
        }
        return consumer;
    }

    ConcurrentMap<URL, FailedRegisteredTask> getFailedRegistered() {
        return failedRegistered;
    }

    ConcurrentMap<URL, FailedUnregisteredTask> getFailedUnregistered() {
        return failedUnregistered;
    }

    ConcurrentMap<Holder, FailedSubscribedTask> getFailedSubscribed() {
        return failedSubscribed;
    }

    ConcurrentMap<Holder, FailedUnsubscribedTask> getFailedUnsubscribed() {
        return failedUnsubscribed;
    }

    @Override
    public void register(URL url) {
        if (!shouldRegister(url)) {
            return;
        }
        super.register(url);
        removeFailedRegistered(url);
        removeFailedUnregistered(url);
        try {
            // Sending a registration request to the server side
            doRegister(url);
        } catch (Exception e) {
            Throwable t = e;

            // If the startup detection is opened, the Exception is thrown directly.
            boolean check = getUrl().getParameter(Constants.CHECK_KEY, true)
                    && url.getParameter(Constants.CHECK_KEY, true)
                    && (url.getPort() != 0);
            boolean skipFailback = t instanceof SkipFailbackWrapperException;
            if (check || skipFailback) {
                if (skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException(
                        "Failed to register " + url + " to registry " + getUrl().getAddress() + ", cause: "
                                + t.getMessage(),
                        t);
            } else {
                logger.error(
                        INTERNAL_ERROR,
                        "unknown error in registry module",
                        "",
                        "Failed to register " + url + ", waiting for retry, cause: " + t.getMessage(),
                        t);
            }

            // Record a failed registration request to a failed list, retry regularly
            addFailedRegistered(url);
        }
    }

    protected boolean shouldRegister(URL providerURL) {
        // extra protocol url must not be registered for interface based service discovery
        if (providerURL.getParameter(IS_EXTRA, false)) {
            return false;
        }
        if (!acceptable(providerURL)) {
            logger.info("URL " + providerURL + " will not be registered to Registry. Registry " + this.getUrl()
                    + " does not accept service of this protocol type.");
            return false;
        }
        return true;
    }

    @Override
    public void reExportRegister(URL url) {
        if (!acceptable(url)) {
            logger.info("URL " + url + " will not be registered to Registry. Registry " + url
                    + " does not accept service of this protocol type.");
            return;
        }
        super.register(url);
        removeFailedRegistered(url);
        removeFailedUnregistered(url);
        try {
            // Sending a registration request to the server side
            doRegister(url);
        } catch (Exception e) {
            if (!(e instanceof SkipFailbackWrapperException)) {
                throw new IllegalStateException(
                        "Failed to register (re-export) " + url + " to registry " + getUrl().getAddress() + ", cause: "
                                + e.getMessage(),
                        e);
            }
        }
    }

    @Override
    public void unregister(URL url) {
        super.unregister(url);
        removeFailedRegistered(url);
        removeFailedUnregistered(url);
        try {
            // Sending a cancellation request to the server side
            doUnregister(url);
        } catch (Exception e) {
            Throwable t = e;

            // If the startup detection is opened, the Exception is thrown directly.
            boolean check = getUrl().getParameter(Constants.CHECK_KEY, true)
                    && url.getParameter(Constants.CHECK_KEY, true)
                    && (url.getPort() != 0);
            boolean skipFailback = t instanceof SkipFailbackWrapperException;
            if (check || skipFailback) {
                if (skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException(
                        "Failed to unregister " + url + " to registry " + getUrl().getAddress() + ", cause: "
                                + t.getMessage(),
                        t);
            } else {
                logger.error(
                        INTERNAL_ERROR,
                        "unknown error in registry module",
                        "",
                        "Failed to unregister " + url + ", waiting for retry, cause: " + t.getMessage(),
                        t);
            }

            // Record a failed registration request to a failed list, retry regularly
            addFailedUnregistered(url);
        }
    }

    @Override
    public void reExportUnregister(URL url) {
        super.unregister(url);
        removeFailedRegistered(url);
        removeFailedUnregistered(url);
        try {
            // Sending a cancellation request to the server side
            doUnregister(url);
        } catch (Exception e) {
            if (!(e instanceof SkipFailbackWrapperException)) {
                throw new IllegalStateException(
                        "Failed to unregister(re-export) " + url + " to registry " + getUrl().getAddress() + ", cause: "
                                + e.getMessage(),
                        e);
            }
        }
    }

    @Override
    public void subscribe(URL url, NotifyListener listener) {
        super.subscribe(url, listener);
        removeFailedSubscribed(url, listener);
        try {
            // Sending a subscription request to the server side
            doSubscribe(url, listener);
        } catch (Exception e) {
            Throwable t = e;

            List<URL> urls = getCacheUrls(url);
            if (CollectionUtils.isNotEmpty(urls)) {
                notify(url, listener, urls);
                logger.error(
                        REGISTRY_FAILED_NOTIFY_EVENT,
                        "",
                        "",
                        "Failed to subscribe " + url + ", Using cached list: " + urls + " from cache file: "
                                + getCacheFile().getName() + ", cause: " + t.getMessage(),
                        t);
            } else {
                // If the startup detection is opened, the Exception is thrown directly.
                boolean check =
                        getUrl().getParameter(Constants.CHECK_KEY, true) && url.getParameter(Constants.CHECK_KEY, true);
                boolean skipFailback = t instanceof SkipFailbackWrapperException;
                if (check || skipFailback) {
                    if (skipFailback) {
                        t = t.getCause();
                    }
                    throw new IllegalStateException("Failed to subscribe " + url + ", cause: " + t.getMessage(), t);
                } else {
                    logger.error(
                            REGISTRY_FAILED_NOTIFY_EVENT,
                            "",
                            "",
                            "Failed to subscribe " + url + ", waiting for retry, cause: " + t.getMessage(),
                            t);
                }
            }

            // Record a failed registration request to a failed list, retry regularly
            addFailedSubscribed(url, listener);
        }
    }

    @Override
    public void unsubscribe(URL url, NotifyListener listener) {
        super.unsubscribe(url, listener);
        removeFailedSubscribed(url, listener);
        try {
            // Sending a canceling subscription request to the server side
            doUnsubscribe(url, listener);
        } catch (Exception e) {
            Throwable t = e;

            // If the startup detection is opened, the Exception is thrown directly.
            boolean check =
                    getUrl().getParameter(Constants.CHECK_KEY, true) && url.getParameter(Constants.CHECK_KEY, true);
            boolean skipFailback = t instanceof SkipFailbackWrapperException;
            if (check || skipFailback) {
                if (skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException(
                        "Failed to unsubscribe " + url + " to registry " + getUrl().getAddress() + ", cause: "
                                + t.getMessage(),
                        t);
            } else {
                logger.error(
                        REGISTRY_FAILED_NOTIFY_EVENT,
                        "",
                        "",
                        "Failed to unsubscribe " + url + ", waiting for retry, cause: " + t.getMessage(),
                        t);
            }

            // Record a failed registration request to a failed list, retry regularly
            addFailedUnsubscribed(url, listener);
        }
    }

    @Override
    protected void notify(URL url, NotifyListener listener, List<URL> urls) {
        if (url == null) {
            throw new IllegalArgumentException("notify url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        try {
            doNotify(url, listener, urls);
        } catch (Exception t) {
            // Record a failed registration request to a failed list
            logger.error(
                    REGISTRY_FAILED_NOTIFY_EVENT,
                    "",
                    "",
                    "Failed to notify addresses for subscribe " + url + ", cause: " + t.getMessage(),
                    t);
        }
    }

    protected void doNotify(URL url, NotifyListener listener, List<URL> urls) {
        super.notify(url, listener, urls);
    }

    @Override
    protected void recover() throws Exception {
        // register
        Set<URL> recoverRegistered = new HashSet<>(getRegistered());
        if (!recoverRegistered.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover register url " + recoverRegistered);
            }
            for (URL url : recoverRegistered) {
                // remove fail registry or unRegistry task first.
                removeFailedRegistered(url);
                removeFailedUnregistered(url);
                addFailedRegistered(url);
            }
        }
        // subscribe
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover subscribe url " + recoverSubscribed.keySet());
            }
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    // First remove other tasks to ensure that addFailedSubscribed can succeed.
                    removeFailedSubscribed(url, listener);
                    addFailedSubscribed(url, listener);
                }
            }
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        retryTimer.stop();
    }

    // ==== Template method ====

    public abstract void doRegister(URL url);

    public abstract void doUnregister(URL url);

    public abstract void doSubscribe(URL url, NotifyListener listener);

    public abstract void doUnsubscribe(URL url, NotifyListener listener);

    static class Holder {

        private final URL url;

        private final NotifyListener notifyListener;

        Holder(URL url, NotifyListener notifyListener) {
            if (url == null || notifyListener == null) {
                throw new IllegalArgumentException();
            }
            this.url = url;
            this.notifyListener = notifyListener;
        }

        @Override
        public int hashCode() {
            return url.hashCode() + notifyListener.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Holder) {
                Holder h = (Holder) obj;
                return this.url.equals(h.url) && this.notifyListener.equals(h.notifyListener);
            } else {
                return false;
            }
        }
    }
}
