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
package org.apache.dubbo.registry;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;

import java.util.List;

/**
 * ============================================================================
 * 文件概述：服务变更通知监听器接口
 * ============================================================================
 * 
 * 本接口定义了注册中心数据变更的回调契约，是Dubbo服务发现机制中Consumer感知
 * Provider变化的核心接口。Consumer订阅服务时提供NotifyListener实现，当Provider
 * 上下线、配置变更时，注册中心会回调notify方法推送最新数据。
 * 
 * 【关键】核心设计原则：
 * 
 * 1. 推送式通知（Push模式）：
 *    - 注册中心主动推送变更，Consumer被动接收
 *    - 对比Pull模式（轮询）：实时性更好，资源消耗更低
 *    - 首次订阅：同步阻塞通知（保证subscribe返回时已有数据）
 *    - 后续变更：异步推送通知
 * 
 * 2. 全量通知原则（最重要的设计约束）：
 *    - 每次通知必须是完整的数据集，不是增量变更
 *    - Consumer无需缓存上次结果进行diff，直接用最新数据替换
 *    - 设计考量：简化Consumer逻辑，避免数据不一致
 * 
 * 【关键】通知契约（必须严格遵守）：
 * 
 * 1. 按服务和数据类型全量通知：
 *    - 维度：单个服务接口 + 单个数据类型（category）
 *    - 含义：不会通知某个服务的部分Provider，要么全通知要么不通知
 *    - 示例：com.foo.BarService的providers变更，通知该服务的所有Provider
 *    - 好处：Consumer用新列表直接替换旧列表，无需合并逻辑
 * 
 * 2. 首次订阅必须全量通知：
 *    - 要求：subscribe()首次调用notify时，必须包含所有类型的数据
 *    - 示例：首次通知包含providers、routers、configurators三类数据
 *    - 原因：Consumer启动时需要完整的配置才能正常工作
 * 
 * 3. 后续变更允许分类通知：
 *    - providers变更：只通知providers类别（不需要同时通知routers）
 *    - routers变更：只通知routers类别
 *    - 单类全量：该类别的数据必须是全量的，不能是增量
 * 
 * 4. 空数据的empty协议表示：
 *    - 场景：某个类别的数据为空（如所有Provider下线）
 *    - 处理：通知包含empty://协议的URL，带category参数标识类别
 *    - 示例：empty://0.0.0.0/com.foo.BarService?category=providers
 *    - 区分：empty协议 ≠ 空列表，empty表示服务存在但无Provider
 * 
 * 5. 通知顺序保证：
 *    - 要求：同一个订阅的多次通知必须按时间顺序
 *    - 实现：单线程推送、队列序列化、版本号比较
 *    - 原因：避免新通知被旧通知覆盖导致数据回退
 * 
 * 【困难】典型通知场景示例：
 * 
 * 场景1：首次订阅（全量通知）
 * <pre>
 * registry.subscribe(consumerUrl, listener);
 * // notify()被调用3次（分类通知）：
 * notify([provider1, provider2])           // providers类别
 * notify([router1])                        // routers类别
 * notify([configurator1, configurator2])   // configurators类别
 * // subscribe()方法返回（Consumer已获取完整配置）
 * </pre>
 * 
 * 场景2：Provider上线（单类通知）
 * <pre>
 * Provider3启动注册
 *   ↓
 * 注册中心推送providers变更
 *   ↓
 * notify([provider1, provider2, provider3])  // 全量providers，包括新增的
 * // 注意：不通知routers和configurators（它们没变）
 * </pre>
 * 
 * 场景3：所有Provider下线（empty通知）
 * <pre>
 * 最后一个Provider下线
 *   ↓
 * notify([empty://0.0.0.0/com.foo.BarService?category=providers])
 * // Consumer收到empty协议，知道服务暂不可用但仍保留引用
 * </pre>
 * 
 * 场景4：路由规则变更（分类通知）
 * <pre>
 * 管理员修改路由规则
 *   ↓
 * notify([router1, router2])  // 只通知routers，providers不变
 * </pre>
 * 
 * 【中等】实现要点和注意事项：
 * 
 * 线程安全：
 * - notify()可能在不同线程被调用（注册中心的推送线程）
 * - 实现必须是线程安全的，建议使用ConcurrentHashMap
 * 
 * 性能考虑：
 * - notify()内不应有耗时操作（避免阻塞注册中心推送线程）
 * - 建议异步处理：接收数据 -> 放入队列 -> 后台线程处理
 * 
 * 异常处理：
 * - notify()抛出异常不应影响注册中心的其他操作
 * - AbstractRegistry会捕获异常并记录日志
 * 
 * (API, Prototype, ThreadSafe)
 * 
 * @see org.apache.dubbo.registry.RegistryService#subscribe(URL, NotifyListener)
 */
public interface NotifyListener {

    /**
     * 【关键】接收服务变更通知（核心回调方法）
     * 
     * ═══════════════════════════════════════════════════════════════════════
     * 核心职责：当订阅的服务数据发生变更时，注册中心调用此方法推送最新数据
     * ═══════════════════════════════════════════════════════════════════════
     * 
     * 【关键】调用时机和场景：
     * 
     * 时机1：首次订阅（同步阻塞）
     * - 何时：subscribe()方法内部，在返回前调用
     * - 特点：阻塞当前线程，必须处理完才返回
     * - 数据：全量的所有category数据
     * 
     * 时机2：Provider上下线（异步推送）
     * - 何时：Provider调用register/unregister时
     * - 特点：异步回调，可能在任意线程
     * - 数据：providers类别的全量数据
     * 
     * 时机3：配置动态变更（异步推送）
     * - 何时：路由规则、配置覆盖等变更时
     * - 特点：异步回调
     * - 数据：相应category的全量数据
     * 
     * 时机4：注册中心重连（自动恢复）
     * - 何时：与注册中心断线重连后
     * - 特点：重新拉取全量数据通知
     * - 数据：所有category的全量数据（确保数据一致）
     * 
     * 【困难】通知契约详解（务必理解）：
     * 
     * 契约1：始终按服务+类型维度全量通知
     * 
     * 正确示例：
     * notify([provider1, provider2, provider3])  // 服务A的所有Provider
     * 
     * 错误示例：
     * notify([provider3])  // ❌ 只通知新增的Provider（增量通知）
     * 
     * 原因：Consumer无法判断这是增量还是全量，可能误删provider1和provider2
     * 
     * 契约2：首次订阅全量，后续可分类
     * 
     * 首次订阅：
     * notify([provider1, provider2])     // providers
     * notify([router1])                  // routers
     * notify([configurator1])            // configurators
     * 
     * 后续Provider变更：
     * notify([provider1, provider2, provider3])  // 只通知providers（全量）
     * 
     * 后续路由变更：
     * notify([router1, router2])  // 只通知routers（全量）
     * 
     * 契约3：空数据必须用empty协议
     * 
     * 错误示例：
     * notify([])  // ❌ 空列表，Consumer无法区分服务不存在还是Provider下线
     * 
     * 正确示例：
     * notify([URL.valueOf("empty://0.0.0.0/com.foo.BarService?category=providers")])
     * 
     * 契约4：通知顺序保证
     * 
     * 时间线：
     * T1: Provider3上线 -> notify([p1, p2, p3])
     * T2: Provider2下线 -> notify([p1, p3])
     * 
     * 要求：Consumer必须按T1 -> T2顺序收到通知
     * 
     * 违反后果：如果T2先到达，Consumer会认为p2可用，导致调用失败
     * 
     * 契约5：数据类型必须全量
     * 
     * 场景：10个Provider，其中1个配置变更
     * 
     * 错误示例：
     * notify([provider1_new])  // ❌ 只通知变更的那个
     * 
     * 正确示例：
     * notify([p1_new, p2, p3, ..., p10])  // ✓ 全量通知所有Provider
     * 
     * 【中等】URLs参数说明：
     * 
     * 参数约束：
     * - 永远不为null（至少包含empty协议URL）
     * - 可能只包含一个category的数据
     * - URL列表是全量的，不是增量
     * 
     * URL类型：
     * - dubbo://...：普通Provider URL
     * - empty://...：表示该category数据为空
     * - route://...：路由规则URL
     * - override://...：配置覆盖URL
     * 
     * 数据格式示例：
     * <pre>
     * // providers类别通知
     * [
     *   dubbo://192.168.1.100:20880/com.foo.BarService?version=1.0,
     *   dubbo://192.168.1.101:20880/com.foo.BarService?version=1.0
     * ]
     * 
     * // routers类别通知
     * [
     *   route://0.0.0.0/com.foo.BarService?rule=...&category=routers
     * ]
     * 
     * // providers为空时的通知
     * [
     *   empty://0.0.0.0/com.foo.BarService?category=providers
     * ]
     * </pre>
     * 
     * 【中等】实现建议：
     * 
     * 1. 快速返回，异步处理：
     * <pre>
     * public void notify(List<URL> urls) {
     *     // 快速放入队列，不阻塞推送线程
     *     updateQueue.offer(urls);
     *     // 后台线程处理
     * }
     * </pre>
     * 
     * 2. 过滤empty协议：
     * <pre>
     * List<URL> validProviders = urls.stream()
     *     .filter(u -> !EMPTY_PROTOCOL.equals(u.getProtocol()))
     *     .collect(Collectors.toList());
     * </pre>
     * 
     * 3. 按category分类处理：
     * <pre>
     * Map<String, List<URL>> categoryMap = urls.stream()
     *     .collect(Collectors.groupingBy(u -> u.getCategory(DEFAULT_CATEGORY)));
     * </pre>
     * 
     * 【关键】并发安全要求：
     * - 线程安全：notify可能在注册中心推送线程被调用
     * - 可重入：同一Listener可能被多个线程同时调用（不同订阅）
     * - 顺序保证：实现应确保处理顺序与通知顺序一致
     * 
     * @param urls 注册信息列表，永远不为空，含义同{@link org.apache.dubbo.registry.RegistryService#lookup(URL)}返回值
     */
    void notify(List<URL> urls);

    /**
     * 【中等】添加服务实例变更监听器（用于应用级服务发现）
     * 
     * 设计背景：Dubbo 3.0引入应用级服务发现，需要监听应用级别的实例变更
     * 
     * @param instanceListener 实例变更监听器
     */
    default void addServiceListener(ServiceInstancesChangedListener instanceListener) {}

    /**
     * 【易懂】获取服务实例监听器
     * @return 实例监听器（默认null表示不使用应用级服务发现）
     */
    default ServiceInstancesChangedListener getServiceListener() {
        return null;
    }

    /**
     * 【易懂】获取Consumer URL（用于标识订阅者）
     * @return Consumer URL（默认null）
     */
    default URL getConsumerUrl() {
        return null;
    }
}
