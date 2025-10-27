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
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.FrameworkExecutorRepository;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.SystemPropertyConfigUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.FILE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REGISTRY_LOCAL_FILE_CACHE_ENABLED;
import static org.apache.dubbo.common.constants.CommonConstants.SystemProperty.USER_HOME;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_EMPTY_ADDRESS;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_FAILED_DELETE_LOCKFILE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_FAILED_DESTROY_UNREGISTER_URL;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_FAILED_NOTIFY_EVENT;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_FAILED_READ_WRITE_CACHE_FILE;
import static org.apache.dubbo.common.constants.RegistryConstants.ACCEPTS_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.registry.Constants.CACHE;
import static org.apache.dubbo.registry.Constants.DUBBO_REGISTRY;
import static org.apache.dubbo.registry.Constants.REGISTRY_FILESAVE_SYNC_KEY;

/**
 * ============================================================================
 * 文件概述：Dubbo注册中心的核心抽象基类，提供本地缓存容错机制
 * ============================================================================
 * 
 * 本文件是整个Registry体系中最关键的基础实现，为所有注册中心（Zookeeper、Nacos等）
 * 提供统一的本地缓存、文件持久化、通知管理等核心能力。当注册中心宕机时，Consumer和
 * Provider仍能通过本地缓存互相发现，保证服务高可用性。
 * 
 * 【关键】核心设计理念和架构价值：
 * 
 * 1. 容错设计：注册中心不是单点，宕机后依赖本地缓存继续提供服务
 *    - 设计哲学："Registry只是服务发现的辅助手段，不应成为服务调用的强依赖"
 *    - 实现方式：每次收到通知都写入本地文件，断线后从文件读取
 *    - 可靠性保证：即使注册中心完全不可用，已启动的服务仍能正常调用
 * 
 * 2. 三层数据结构（理解整个缓存机制的关键）：
 *    内存缓存（notified）：
 *      - 结构：ConcurrentMap<URL, Map<Category, List<URL>>>
 *      - 作用：最新的订阅数据，实时更新，供业务代码快速访问
 *      - 示例：notified.get(consumerUrl).get("providers") 获取Provider列表
 *    
 *    文件缓存（properties + file）：
 *      - 结构：Properties持久化到本地文件
 *      - 作用：进程重启后恢复数据，注册中心断线时的备份
 *      - 位置：~/.dubbo/dubbo-registry-{application}-{address}.cache
 *      - 格式：com.foo.BarService=dubbo://host1 dubbo://host2 ...
 *    
 *    注册状态（registered）：
 *      - 结构：ConcurrentHashSet<URL>
 *      - 作用：记录本地注册过的URL，用于重连后自动恢复注册
 *      - 场景：Provider重启后自动重新注册到注册中心
 * 
 * 3. 文件锁机制（解决多进程冲突）：
 *    - 问题：同一台机器多个Dubbo进程共享同一个缓存文件会冲突
 *    - 方案：使用FileLock保证同一时刻只有一个进程写入文件
 *    - 容错：获取锁失败不阻塞，记录日志后重试
 * 
 * 4. 异步批量写入（性能优化）：
 *    - 问题：每次通知都立即写文件会导致频繁IO
 *    - 方案：延迟500ms批量写入，将短时间内的多次变更合并
 *    - 版本控制：使用AtomicLong版本号避免覆盖最新数据
 * 
 * 【困难】数据流转全链路（建议画图理解）：
 * 
 * 启动恢复流程：
 * <pre>
 * AbstractRegistry构造
 *   ↓
 * loadProperties()           // 从文件加载历史缓存
 *   ↓
 * properties -> notified    // 填充内存缓存
 *   ↓
 * notify(backupUrls)        // 使用缓存数据进行首次通知
 *   ↓
 * [后续] 连接注册中心后实时数据覆盖缓存
 * </pre>
 * 
 * 订阅通知流程：
 * <pre>
 * 注册中心推送Provider变更
 *   ↓
 * notify(url, listener, urls)  // 子类调用
 *   ↓
 * 按category分类             // providers/routers/configurators
 *   ↓
 * 更新notified内存缓存        // ConcurrentMap
 *   ↓
 * listener.notify()          // 回调业务代码
 *   ↓
 * saveProperties()           // 触发文件保存
 *   ↓
 * 延迟500ms后doSaveProperties()  // 异步批量写入文件
 * </pre>
 * 
 * 【中等】使用约束和注意事项：
 * - 本类是抽象类，不能直接实例化，必须由子类（如ZookeeperRegistry）继承
 * - 子类负责实现与具体注册中心的交互，本类只提供缓存和通知管理
 * - 线程安全：所有public方法都是线程安全的，使用ConcurrentHashMap
 * - 文件路径：默认在用户主目录下，可通过file参数自定义
 * 
 * 【关键】标签使用策略：
 * - 【关键】：本地缓存机制、文件锁、通知分发等核心容错逻辑
 * - 【困难】：文件持久化、版本控制、多进程并发等复杂机制
 * - 【中等】：注册状态管理、订阅关系维护、数据查询等常规逻辑
 * - 【易懂】：简单的getter/setter、工具方法
 * 
 * (SPI, Prototype, ThreadSafe)
 */
public abstract class AbstractRegistry implements Registry {

    // 【易懂】URL分隔符：文件缓存中使用空格分隔多个Provider URL
    // 示例：dubbo://host1 dubbo://host2 dubbo://host3
    private static final char URL_SEPARATOR = ' ';
    
    // 【易懂】URL分割正则：解析文件缓存中的Provider列表（兼容多个空格）
    private static final String URL_SPLIT = "\\s+";
    
    // 【中等】文件保存最大重试次数
    // 设计考量：文件锁冲突时重试3次，避免无限重试占用资源
    private static final int MAX_RETRY_TIMES_SAVE_PROPERTIES = 3;
    
    // 【中等】文件保存延迟间隔（毫秒）
    // 设计考量：500ms的延迟可以合并短时间内的多次变更，减少磁盘IO
    private static final long DEFAULT_INTERVAL_SAVE_PROPERTIES = 500L;

    // 【易懂】日志输出器
    protected final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

    // 【关键】本地缓存的核心数据结构：Properties
    // 用途：持久化到文件的Key-Value存储
    // 格式：serviceKey -> "url1 url2 url3"（空格分隔的URL列表）
    // 示例：com.foo.BarService -> "dubbo://192.168.1.100:20880/... dubbo://192.168.1.101:20880/..."
    // 线程安全：Properties内部使用Hashtable，是线程安全的
    private final Properties properties = new Properties();
    
    // 【中等】定时任务执行器：用于异步延迟保存文件
    // 共享线程池：从FrameworkExecutorRepository获取，避免创建过多线程
    private final ScheduledExecutorService registryCacheExecutor;
    
    // 【困难】缓存版本号：用于异步写入时的并发控制
    // 工作原理：每次修改properties时版本号+1，写入文件前检查版本是否已过期
    // 场景：如果有更新的版本正在等待写入，则放弃写入旧版本数据
    // CAS保证：AtomicLong保证版本号递增的原子性
    private final AtomicLong lastCacheChanged = new AtomicLong();
    
    // 【中等】文件保存重试计数器：跟踪当前重试次数
    // 用途：达到MAX_RETRY_TIMES_SAVE_PROPERTIES后停止重试并记录错误日志
    private final AtomicInteger savePropertiesRetryTimes = new AtomicInteger();
    
    // 【关键】已注册URL集合：记录本地注册到注册中心的所有URL
    // 用途：用于注册中心重连后自动恢复注册（recover方法）
    // 场景：Provider启动注册 -> 注册中心重启 -> 检测到重连 -> 遍历此集合重新注册
    // 线程安全：使用ConcurrentHashSet保证并发安全
    private final Set<URL> registered = new ConcurrentHashSet<>();
    
    // 【关键】订阅关系映射：记录所有订阅关系和对应的监听器
    // 结构：URL（订阅条件） -> Set<NotifyListener>（监听器集合）
    // 场景：Consumer订阅服务 -> 保存到此Map -> Provider变更时遍历所有listener进行通知
    // 多监听器：同一个URL可以有多个listener（例如同一服务被多个模块引用）
    private final ConcurrentMap<URL, Set<NotifyListener>> subscribed = new ConcurrentHashMap<>();
    
    // 【关键】通知数据缓存：最新的订阅数据（三层嵌套结构）
    // 结构：URL（订阅条件） -> Category（分类） -> List<URL>（Provider列表）
    // 示例：consumerUrl -> {"providers": [providerUrl1, providerUrl2], "routers": [routerUrl]}
    // 作用：
    //   1. 内存缓存：快速查询最新的Provider列表
    //   2. 通知去重：避免相同数据重复通知
    //   3. 断线恢复：注册中心宕机时仍可查询历史数据
    // 并发控制：外层使用ConcurrentHashMap，内层Map需要在更新时加锁或使用ConcurrentHashMap
    private final ConcurrentMap<URL, Map<String, List<URL>>> notified = new ConcurrentHashMap<>();
    
    // 【中等】同步保存文件标志：是否同步写入文件（默认false异步）
    // 设计权衡：
    //   - false（默认）：异步写入，性能好但可能丢失最后500ms的数据
    //   - true：同步写入，可靠性高但性能较差
    // 配置：通过registry.filesave.sync参数控制
    private boolean syncSaveFile;
    
    // 【易懂】注册中心URL：当前Registry连接的注册中心地址
    // 示例：zookeeper://127.0.0.1:2181/org.apache.dubbo.registry.RegistryService
    private URL registryUrl;
    
    // 【关键】本地缓存文件：持久化存储订阅数据
    // 默认路径：~/.dubbo/dubbo-registry-{application}-{registryAddress}.cache
    // 格式：Java Properties格式（key=value）
    // 生命周期：应用启动时读取，运行期间实时更新，进程退出时保留
    private File file;
    
    // 【易懂】本地缓存开关：是否启用文件缓存（默认true）
    // 配置：通过registry.local.file.cache.enabled参数控制
    // 关闭场景：某些特殊环境（如只读文件系统）无法写入文件时关闭
    private final boolean localCacheEnabled;
    
    // 【易懂】注册中心管理器：全局Registry实例管理器
    protected RegistryManager registryManager;
    
    // 【易懂】应用模型：当前应用的元数据和扩展点容器
    protected ApplicationModel applicationModel;

    // 【易懂】多进程文件冲突的错误原因描述
    private static final String CAUSE_MULTI_DUBBO_USING_SAME_FILE = "multiple Dubbo instance are using the same file";

    /**
     * 【关键】AbstractRegistry构造函数：初始化本地缓存机制
     * 
     * ═══════════════════════════════════════════════════════════════════════
     * 核心职责：设置注册中心URL、初始化缓存文件、加载历史数据
     * ═══════════════════════════════════════════════════════════════════════
     * 
     * 【困难】初始化流程详解：
     * 
     * 1. 基础配置初始化：
     *    - 保存注册中心URL（如zookeeper://127.0.0.1:2181）
     *    - 获取RegistryManager和线程池（从ApplicationModel）
     *    - 读取本地缓存开关配置（默认启用）
     * 
     * 2. 缓存文件路径生成（如果启用本地缓存）：
     *    默认路径格式：~/.dubbo/dubbo-registry-{应用名}-{注册中心地址}.cache
     *    示例：~/.dubbo/dubbo-registry-demo-provider-127.0.0.1-2181.cache
     *    
     *    路径组成：
     *    - USER_HOME：用户主目录（~）
     *    - DUBBO_REGISTRY：固定前缀（/.dubbo/dubbo-registry-）
     *    - application：应用名称，用于隔离不同应用
     *    - address：注册中心地址，冒号替换为短横线（避免文件名非法字符）
     *    - CACHE：固定后缀（.cache）
     *    
     *    自定义路径：通过file参数指定（file=/custom/path/cache.properties）
     * 
     * 3. 目录创建和权限检查：
     *    - 检查缓存文件所在目录是否存在
     *    - 不存在则递归创建目录（mkdirs）
     *    - 创建失败则抛出IllegalArgumentException（可能是权限问题）
     *    设计考量：启动阶段检查文件权限，避免运行期间写入失败
     * 
     * 4. 加载历史缓存数据（容错机制的关键）：
     *    loadProperties()：从文件读取上次保存的Provider列表
     *    notify(backupUrls)：使用历史数据进行首次通知
     *    
     *    作用：注册中心暂时不可用时，Consumer仍能从缓存中找到Provider
     *    时序：构造完成后，历史数据已加载到内存，可以立即提供服务
     * 
     * 5. 异步写入配置：
     *    syncSaveFile参数：
     *    - false（默认）：异步延迟500ms写入，性能更好
     *    - true：同步立即写入，可靠性更高但影响性能
     * 
     * 【关键】多进程场景处理：
     * 
     * 问题：同一台机器启动多个Dubbo应用可能使用相同的缓存文件
     * 方案1：通过应用名区分文件路径（推荐）
     *   示例：app1使用~/.dubbo/dubbo-registry-app1-xxx.cache
     *        app2使用~/.dubbo/dubbo-registry-app2-xxx.cache
     * 
     * 方案2：自定义file参数为不同路径
     *   配置：dubbo.registry.file=/path/to/app1.cache
     * 
     * 冲突处理：如果多个进程共享文件，使用FileLock避免并发写入冲突
     * 
     * 【中等】典型使用场景：
     * 
     * 场景1：ZookeeperRegistry初始化
     * <pre>
     * public ZookeeperRegistry(URL url, ZookeeperTransporter zookeeperTransporter) {
     *     super(url);  // 调用AbstractRegistry构造，初始化缓存
     *     // 然后连接Zookeeper服务器
     * }
     * </pre>
     * 
     * 场景2：禁用本地缓存（某些只读文件系统）
     * <pre>
     * URL url = URL.valueOf("zookeeper://127.0.0.1:2181?registry.local.file.cache.enabled=false");
     * // 此时不会创建缓存文件，但会损失断线容错能力
     * </pre>
     * 
     * 场景3：自定义缓存文件路径
     * <pre>
     * URL url = URL.valueOf("zookeeper://127.0.0.1:2181?file=/data/registry/my-app.cache");
     * // 使用指定的缓存文件路径
     * </pre>
     * 
     * 【关键】前置条件：
     * - URL不为空且包含必要参数（address、application等）
     * - 应用有权限访问用户主目录或自定义文件路径
     * - ApplicationModel已正确初始化（用于获取Bean）
     * 
     * 【关键】后置条件（构造完成时）：
     * - registryUrl已设置
     * - 缓存文件已创建（如果启用本地缓存）
     * - 历史数据已加载到properties和notified
     * - registryCacheExecutor已初始化（可用于调度任务）
     * 
     * 【关键】可见副作用：
     * - 文件系统：可能创建缓存文件和目录
     * - 内存分配：加载历史数据到properties和notified
     * - 日志输出：记录缓存文件路径和加载状态
     * 
     * @param url 注册中心URL，包含连接信息和配置参数
     * @throws IllegalArgumentException 如果url为null或缓存目录创建失败
     */
    protected AbstractRegistry(URL url) {
        // 【易懂】保存注册中心URL
        setUrl(url);
        
        // 【易懂】获取全局RegistryManager实例（用于管理所有Registry）
        registryManager = url.getOrDefaultApplicationModel().getBeanFactory().getBean(RegistryManager.class);
        
        // 【易懂】读取本地缓存开关配置（默认true）
        localCacheEnabled = url.getParameter(REGISTRY_LOCAL_FILE_CACHE_ENABLED, true);
        
        // 【中等】获取共享的定时任务线程池（用于异步保存文件）
        // 共享设计：避免每个Registry创建独立线程池，节省资源
        registryCacheExecutor = url.getOrDefaultFrameworkModel()
                .getBeanFactory()
                .getBean(FrameworkExecutorRepository.class)
                .getSharedScheduledExecutor();
        
        if (localCacheEnabled) {
            // 【易懂】读取文件保存模式配置（同步/异步）
            syncSaveFile = url.getParameter(REGISTRY_FILESAVE_SYNC_KEY, false);

            // 【中等】生成默认缓存文件路径
            // 格式：~/.dubbo/dubbo-registry-{application}-{address}.cache
            // 冒号替换：避免Windows/Linux文件名非法字符问题
            String defaultFilename = SystemPropertyConfigUtils.getSystemProperty(USER_HOME) + DUBBO_REGISTRY
                    + url.getApplication() + "-" + url.getAddress().replaceAll(":", "-") + CACHE;

            // 【易懂】允许通过file参数自定义路径，否则使用默认路径
            String filename = url.getParameter(FILE_KEY, defaultFilename);
            File file = null;

            if (ConfigUtils.isNotEmpty(filename)) {
                file = new File(filename);
                
                // 【中等】检查并创建缓存文件所在目录
                // 条件：文件不存在 且 父目录不存在
                if (!file.exists()
                        && file.getParentFile() != null
                        && !file.getParentFile().exists()) {
                    
                    // 【中等】递归创建目录，失败则抛异常
                    if (!file.getParentFile().mkdirs()) {

                        IllegalArgumentException illegalArgumentException =
                                new IllegalArgumentException("Invalid registry cache file " + file
                                        + ", cause: Failed to create directory " + file.getParentFile() + "!");

                        if (logger != null) {
                            // 【易懂】记录错误日志（错误码1-9：缓存文件读写失败）
                            logger.error(
                                    REGISTRY_FAILED_READ_WRITE_CACHE_FILE,
                                    "cache directory inaccessible",
                                    "Try adjusting permission of the directory.",
                                    "failed to create directory",
                                    illegalArgumentException);
                        }

                        throw illegalArgumentException;
                    }
                }
            }

            this.file = file;

            // 【关键】容错机制的核心：加载历史缓存数据
            // 作用：注册中心暂时不可用时，Consumer仍能从缓存找到Provider
            // 顺序：必须先loadProperties()加载文件，再notify()使用数据
            loadProperties();
            
            // 【中等】使用备份URL进行首次通知（如果有的话）
            // backupUrls：注册中心URL可能配置多个备份地址
            notify(url.getBackupUrls());
        }
    }

    /**
     * 【中等】过滤空列表，返回empty协议URL
     * 
     * 设计背景：当某个服务的所有Provider都下线时，注册中心会返回空列表。
     * 但Dubbo需要明确区分"服务不存在"和"所有Provider已下线"两种情况。
     * 
     * 处理策略：
     * - 空列表 -> 返回包含empty协议的单元素列表
     * - 非空列表 -> 直接返回原列表
     * 
     * empty协议作用：
     * - 告诉Consumer该服务存在但暂无Provider
     * - Consumer可以保留引用，等待Provider上线
     * - 区别于服务不存在（返回null）
     * 
     * @param url 原始订阅URL（用于构造empty协议URL）
     * @param urls Provider列表（可能为空）
     * @return 处理后的URL列表（至少包含一个元素）
     */
    protected static List<URL> filterEmpty(URL url, List<URL> urls) {
        if (CollectionUtils.isEmpty(urls)) {
            List<URL> result = new ArrayList<>(1);
            result.add(url.setProtocol(EMPTY_PROTOCOL));
            return result;
        }
        return urls;
    }

    /**
     * 【易懂】获取注册中心URL
     * @return 注册中心URL（如zookeeper://127.0.0.1:2181/...）
     */
    @Override
    public URL getUrl() {
        return registryUrl;
    }

    /**
     * 【易懂】设置注册中心URL（带参数校验）
     * @param url 注册中心URL
     * @throws IllegalArgumentException 如果url为null
     */
    protected void setUrl(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("registry url == null");
        }
        this.registryUrl = url;
    }

    /**
     * 【中等】获取已注册URL集合（只读视图）
     * 
     * 用途：
     * - 查询当前应用注册了哪些服务
     * - 用于重连后恢复注册（recover方法）
     * - 监控和诊断工具
     * 
     * @return 不可修改的已注册URL集合
     */
    public Set<URL> getRegistered() {
        return Collections.unmodifiableSet(registered);
    }

    /**
     * 【中等】获取订阅关系映射（只读视图）
     * 
     * 结构：URL（订阅条件） -> Set<NotifyListener>（监听器集合）
     * 
     * 用途：
     * - 查询当前应用订阅了哪些服务
     * - 用于重连后恢复订阅（recover方法）
     * - 诊断订阅关系
     * 
     * @return 不可修改的订阅关系映射
     */
    public Map<URL, Set<NotifyListener>> getSubscribed() {
        return Collections.unmodifiableMap(subscribed);
    }

    /**
     * 【关键】获取通知数据缓存（只读视图）
     * 
     * 三层结构：URL（订阅条件） -> Category（分类） -> List<URL>（Provider列表）
     * 
     * 用途：
     * - 快速查询最新的Provider列表
     * - 注册中心宕机时使用缓存数据
     * - lookup()方法从此获取数据
     * 
     * @return 不可修改的通知数据缓存
     */
    public Map<URL, Map<String, List<URL>>> getNotified() {
        return Collections.unmodifiableMap(notified);
    }

    /**
     * 【易懂】获取本地缓存文件对象
     * @return 缓存文件（可能为null如果禁用了本地缓存）
     */
    public File getCacheFile() {
        return file;
    }

    /**
     * 【易懂】获取Properties缓存对象
     * @return 内存中的Properties缓存
     */
    public Properties getCacheProperties() {
        return properties;
    }

    /**
     * 【中等】获取缓存版本号（用于并发控制）
     * @return 当前缓存版本号
     */
    public AtomicLong getLastCacheChanged() {
        return lastCacheChanged;
    }

    /**
     * 【困难】执行文件保存操作（核心持久化逻辑）
     * 
     * ═══════════════════════════════════════════════════════════════════════
     * 核心职责：将内存中的Properties缓存写入本地文件，使用文件锁避免多进程冲突
     * ═══════════════════════════════════════════════════════════════════════
     * 
     * 【困难】版本控制机制（防止覆盖最新数据）：
     * 
     * 问题场景：
     * 1. 线程A触发保存，version=100，延迟500ms执行
     * 2. 线程B收到新通知，version=101，也延迟500ms执行
     * 3. 如果A后执行，会用旧数据覆盖B的新数据
     * 
     * 解决方案：
     * - 每次修改properties时版本号+1（lastCacheChanged.incrementAndGet()）
     * - 执行保存前检查version < lastCacheChanged.get()
     * - 如果版本过期，放弃本次保存（已有更新的版本在队列中）
     * 
     * 版本比较逻辑：
     * <pre>
     * if (version < lastCacheChanged.get()) {
     *     return;  // 当前版本已过期，不保存
     * }
     * </pre>
     * 
     * 【困难】文件锁机制（解决多进程并发写入）：
     * 
     * 问题：同一台机器多个Dubbo进程可能共享同一个缓存文件
     * 示例：两个应用都配置了相同的file参数
     * 
     * 文件锁策略：
     * 1. 创建.lock锁文件（缓存文件路径 + ".lock"）
     * 2. 使用FileLock.tryLock()尝试获取独占锁
     * 3. 获取成功：执行写入，释放锁，删除.lock文件
     * 4. 获取失败：抛出IOException，触发重试机制
     * 
     * 锁类型：
     * - 独占锁（排他锁）：同一时刻只有一个进程能写入
     * - 非阻塞：tryLock()立即返回，不等待
     * 
     * 锁文件生命周期：
     * <pre>
     * 创建.lock文件 
     *   ↓
     * 获取FileLock
     *   ↓
     * 执行文件写入
     *   ↓
     * 释放FileLock（自动，try-with-resources）
     *   ↓
     * 删除.lock文件（finally块）
     * </pre>
     * 
     * 【困难】并发写入优化（深拷贝vs直接引用）：
     * 
     * 同步模式（syncSaveFile=true）：
     * - 保存线程 = 修改线程（都在registryCacheExecutor）
     * - 直接使用properties引用，无需拷贝
     * - 性能最优，但可能阻塞其他操作
     * 
     * 异步模式（syncSaveFile=false，默认）：
     * - 保存线程 ≠ 修改线程（notify可能在任意线程）
     * - 深拷贝properties到tmpProperties避免并发修改
     * - 性能稍差，但不会阻塞业务线程
     * 
     * 为什么需要深拷贝：
     * <pre>
     * 线程A：properties.setProperty("key", "value")  // 正在修改
     * 线程B：properties.store(outputStream)          // 正在保存
     * 问题：Properties内部使用Hashtable，setProperty和store都需要锁
     *      如果并发调用会产生锁竞争，影响性能
     * 解决：拷贝一份独立的Properties，保存操作不影响主对象
     * </pre>
     * 
     * 【中等】重试机制：
     * 
     * 失败场景：
     * 1. 文件锁被其他进程占用（OverlappingFileLockException）
     * 2. 磁盘空间不足
     * 3. 权限不足
     * 
     * 重试策略：
     * - 最大重试次数：MAX_RETRY_TIMES_SAVE_PROPERTIES（3次）
     * - 重试间隔：DEFAULT_INTERVAL_SAVE_PROPERTIES（500ms）
     * - 达到上限后记录错误日志并停止重试
     * 
     * 【中等】性能分析：
     * 
     * 时间复杂度：O(N) - N为properties中的键值对数量（深拷贝）
     * 空间复杂度：O(N) - 异步模式需要深拷贝
     * IO开销：单次文件写入，通常<10ms（取决于数据量）
     * 锁竞争：tryLock非阻塞，冲突时立即返回
     * 
     * 瓶颈：
     * - 大量服务订阅导致properties数据量大
     * - 磁盘IO速度（机械硬盘 vs SSD）
     * - 多进程频繁写入导致锁竞争
     * 
     * 【关键】典型使用场景：
     * 
     * 场景1：异步延迟保存（默认行为）
     * <pre>
     * notify收到Provider变更
     *   ↓
     * saveProperties()更新properties并触发保存
     *   ↓
     * registryCacheExecutor.schedule(() -> doSaveProperties(version), 500ms)
     *   ↓
     * 500ms后执行doSaveProperties()
     * </pre>
     * 
     * 场景2：同步立即保存（syncSaveFile=true）
     * <pre>
     * notify收到Provider变更
     *   ↓
     * saveProperties()更新properties
     *   ↓
     * doSaveProperties(version)立即执行（阻塞当前线程）
     * </pre>
     * 
     * 场景3：多进程文件锁冲突
     * <pre>
     * 进程A正在写入文件（持有锁）
     *   ↓
     * 进程B尝试写入：tryLock()返回null
     *   ↓
     * 抛出IOException触发重试
     *   ↓
     * 500ms后重新尝试
     * </pre>
     * 
     * 【关键】前置条件：
     * - file不为null（本地缓存已启用）
     * - version >= lastCacheChanged.get()（版本未过期）
     * - 有文件写入权限
     * 
     * 【关键】后置条件：
     * - properties数据已持久化到文件
     * - .lock锁文件已删除
     * - 重试计数器已重置（成功时）
     * 
     * 【关键】可见副作用：
     * - 文件系统：写入缓存文件，创建并删除.lock文件
     * - 内存：异步模式会临时创建Properties副本
     * - 日志：失败时记录warn/error日志
     * - 重试任务：失败时调度延迟重试任务
     * 
     * @param version 要保存的数据版本号（用于乐观锁校验）
     */
    public void doSaveProperties(long version) {
        // 【困难】版本检查：如果当前版本已过期，放弃保存
        // 原因：已有更新的版本在队列中等待保存，无需保存旧数据
        if (version < lastCacheChanged.get()) {
            return;
        }
        
        // 【易懂】缓存未启用，直接返回
        if (file == null) {
            return;
        }
        
        // 【中等】锁文件对象，用于finally块中删除
        File lockfile = null;
        try {
            // 【困难】创建锁文件（缓存文件路径 + ".lock"）
            // 示例：~/.dubbo/dubbo-registry-app-127.0.0.1-2181.cache.lock
            lockfile = new File(file.getAbsolutePath() + ".lock");
            if (!lockfile.exists()) {
                lockfile.createNewFile();
            }

            // 【困难】使用try-with-resources自动释放文件锁和通道
            try (RandomAccessFile raf = new RandomAccessFile(lockfile, "rw");
                    FileChannel channel = raf.getChannel()) {
                
                // 【关键】尝试获取独占文件锁（非阻塞）
                // 返回null表示锁被其他进程持有
                FileLock lock = channel.tryLock();
                if (lock == null) {

                    IOException ioException = new IOException(
                            "Can not lock the registry cache file " + file.getAbsolutePath() + ", "
                                    + "ignore and retry later, maybe multi java process use the file, please config: dubbo.registry.file=xxx.properties");

                    // 【易懂】记录警告日志（错误码1-9：缓存文件读写失败）
                    logger.warn(
                            REGISTRY_FAILED_READ_WRITE_CACHE_FILE,
                            CAUSE_MULTI_DUBBO_USING_SAME_FILE,
                            "",
                            "Adjust dubbo.registry.file.",
                            ioException);

                    throw ioException;
                }

                // 【关键】获取锁成功，开始写入文件
                try {
                    // 【易懂】确保缓存文件存在
                    if (!file.exists()) {
                        file.createNewFile();
                    }

                    // 【困难】根据同步/异步模式选择不同的数据源
                    Properties tmpProperties;
                    if (syncSaveFile) {
                        // 【中等】同步模式：直接使用properties引用
                        // 原因：properties.setProperty和store在同一线程（registryCacheExecutor）
                        //      不存在并发修改问题，无需深拷贝
                        tmpProperties = properties;
                    } else {
                        // 【困难】异步模式：深拷贝properties避免并发修改
                        // 原因：properties可能在其他线程被修改（notify调用）
                        //      直接使用会导致ConcurrentModificationException或锁竞争
                        tmpProperties = new Properties();
                        Set<Map.Entry<Object, Object>> entries = properties.entrySet();
                        for (Map.Entry<Object, Object> entry : entries) {
                            tmpProperties.setProperty((String) entry.getKey(), (String) entry.getValue());
                        }
                    }

                    try (FileOutputStream outputFile = new FileOutputStream(file)) {
                        tmpProperties.store(outputFile, "Dubbo Registry Cache");
                    }
                } finally {
                    lock.release();
                }
            }
        } catch (Throwable e) {
            savePropertiesRetryTimes.incrementAndGet();

            if (savePropertiesRetryTimes.get() >= MAX_RETRY_TIMES_SAVE_PROPERTIES) {
                if (e instanceof OverlappingFileLockException) {
                    // fix #9341, ignore OverlappingFileLockException
                    logger.info("Failed to save registry cache file for file overlapping lock exception, file name "
                            + file.getName());
                } else {
                    // 1-9 failed to read / save registry cache file.
                    logger.warn(
                            REGISTRY_FAILED_READ_WRITE_CACHE_FILE,
                            CAUSE_MULTI_DUBBO_USING_SAME_FILE,
                            "",
                            "Failed to save registry cache file after retrying " + MAX_RETRY_TIMES_SAVE_PROPERTIES
                                    + " times, cause: " + e.getMessage(),
                            e);
                }

                savePropertiesRetryTimes.set(0);
                return;
            }

            if (version < lastCacheChanged.get()) {
                savePropertiesRetryTimes.set(0);
                return;
            } else {
                registryCacheExecutor.schedule(
                        () -> doSaveProperties(lastCacheChanged.incrementAndGet()),
                        DEFAULT_INTERVAL_SAVE_PROPERTIES,
                        TimeUnit.MILLISECONDS);
            }

            if (!(e instanceof OverlappingFileLockException)) {
                logger.warn(
                        REGISTRY_FAILED_READ_WRITE_CACHE_FILE,
                        CAUSE_MULTI_DUBBO_USING_SAME_FILE,
                        "However, the retrying count limit is not exceeded. Dubbo will still try.",
                        "Failed to save registry cache file, will retry, cause: " + e.getMessage(),
                        e);
            }
        } finally {
            if (lockfile != null) {
                if (!lockfile.delete()) {
                    // 1-10 Failed to delete lock file.
                    logger.warn(
                            REGISTRY_FAILED_DELETE_LOCKFILE,
                            "",
                            "",
                            String.format("Failed to delete lock file [%s]", lockfile.getName()));
                }
            }
        }
    }

    private void loadProperties() {
        if (file == null || !file.exists()) {
            return;
        }
        try (InputStream in = Files.newInputStream(file.toPath())) {
            properties.load(in);
            if (logger.isInfoEnabled()) {
                logger.info("Loaded registry cache file " + file);
            }
        } catch (IOException e) {
            // 1-9 failed to read / save registry cache file.
            logger.warn(
                    REGISTRY_FAILED_READ_WRITE_CACHE_FILE, CAUSE_MULTI_DUBBO_USING_SAME_FILE, "", e.getMessage(), e);

        } catch (Throwable e) {
            // 1-9 failed to read / save registry cache file.
            logger.warn(
                    REGISTRY_FAILED_READ_WRITE_CACHE_FILE,
                    CAUSE_MULTI_DUBBO_USING_SAME_FILE,
                    "",
                    "Failed to load registry cache file " + file,
                    e);
        }
    }

    public List<URL> getCacheUrls(URL url) {
        Map<String, List<URL>> categoryNotified = notified.get(url);
        if (CollectionUtils.isNotEmptyMap(categoryNotified)) {
            List<URL> urls = categoryNotified.values().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            return urls;
        }

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (StringUtils.isNotEmpty(key)
                    && key.equals(url.getServiceKey())
                    && (Character.isLetter(key.charAt(0)) || key.charAt(0) == '_')
                    && StringUtils.isNotEmpty(value)) {
                String[] arr = value.trim().split(URL_SPLIT);
                List<URL> urls = new ArrayList<>();
                for (String u : arr) {
                    urls.add(URL.valueOf(u));
                }
                return urls;
            }
        }
        return null;
    }

    @Override
    public List<URL> lookup(URL url) {
        List<URL> result = new ArrayList<>();
        Map<String, List<URL>> notifiedUrls = getNotified().get(url);
        if (CollectionUtils.isNotEmptyMap(notifiedUrls)) {
            for (List<URL> urls : notifiedUrls.values()) {
                for (URL u : urls) {
                    if (!EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        } else {
            final AtomicReference<List<URL>> reference = new AtomicReference<>();
            NotifyListener listener = reference::set;
            subscribe(url, listener); // Subscribe logic guarantees the first notify to return
            List<URL> urls = reference.get();
            if (CollectionUtils.isNotEmpty(urls)) {
                for (URL u : urls) {
                    if (!EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public void register(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("register url == null");
        }
        if (url.getPort() != 0) {
            if (logger.isInfoEnabled()) {
                logger.info("Register: " + url);
            }
        }
        registered.add(url);
    }

    @Override
    public void unregister(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("unregister url == null");
        }
        if (url.getPort() != 0) {
            if (logger.isInfoEnabled()) {
                logger.info("Unregister: " + url);
            }
        }
        registered.remove(url);
    }

    @Override
    public void subscribe(URL url, NotifyListener listener) {
        if (url == null) {
            throw new IllegalArgumentException("subscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("subscribe listener == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Subscribe: " + url);
        }
        Set<NotifyListener> listeners =
                ConcurrentHashMapUtils.computeIfAbsent(subscribed, url, n -> new ConcurrentHashSet<>());
        listeners.add(listener);
    }

    @Override
    public void unsubscribe(URL url, NotifyListener listener) {
        if (url == null) {
            throw new IllegalArgumentException("unsubscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("unsubscribe listener == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Unsubscribe: " + url);
        }
        Set<NotifyListener> listeners = subscribed.get(url);
        if (listeners != null) {
            listeners.remove(listener);
        }

        // do not forget remove notified
        notified.remove(url);
    }

    protected void recover() throws Exception {
        // register
        Set<URL> recoverRegistered = new HashSet<>(getRegistered());
        if (!recoverRegistered.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover register url " + recoverRegistered);
            }
            for (URL url : recoverRegistered) {
                register(url);
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
                    subscribe(url, listener);
                }
            }
        }
    }

    protected void notify(List<URL> urls) {
        if (CollectionUtils.isEmpty(urls)) {
            return;
        }

        for (Map.Entry<URL, Set<NotifyListener>> entry : getSubscribed().entrySet()) {
            URL url = entry.getKey();

            if (!UrlUtils.isMatch(url, urls.get(0))) {
                continue;
            }

            Set<NotifyListener> listeners = entry.getValue();
            if (listeners != null) {
                for (NotifyListener listener : listeners) {
                    try {
                        notify(url, listener, filterEmpty(url, urls));
                    } catch (Throwable t) {
                        // 1-7: Failed to notify registry event.
                        logger.error(
                                REGISTRY_FAILED_NOTIFY_EVENT,
                                "consumer is offline",
                                "",
                                "Failed to notify registry event, urls: " + urls + ", cause: " + t.getMessage(),
                                t);
                    }
                }
            }
        }
    }

    /**
     * Notify changes from the provider side.
     *
     * @param url      consumer side url
     * @param listener listener
     * @param urls     provider latest urls
     */
    protected void notify(URL url, NotifyListener listener, List<URL> urls) {
        if (url == null) {
            throw new IllegalArgumentException("notify url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        if ((CollectionUtils.isEmpty(urls)) && !ANY_VALUE.equals(url.getServiceInterface())) {
            // 1-4 Empty address.
            logger.warn(REGISTRY_EMPTY_ADDRESS, "", "", "Ignore empty notify urls for subscribe url " + url);
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("[INSTANCE_REGISTER] Notify urls for subscribe url " + url + ", url size: " + urls.size());
        }
        // keep every provider's category.
        Map<String, List<URL>> result = new HashMap<>();
        for (URL u : urls) {
            if (UrlUtils.isMatch(url, u)) {
                String category = u.getCategory(DEFAULT_CATEGORY);
                List<URL> categoryList = result.computeIfAbsent(category, k -> new ArrayList<>());
                categoryList.add(u);
            }
        }
        if (result.size() == 0) {
            return;
        }
        Map<String, List<URL>> categoryNotified =
                ConcurrentHashMapUtils.computeIfAbsent(notified, url, u -> new ConcurrentHashMap<>());
        for (Map.Entry<String, List<URL>> entry : result.entrySet()) {
            String category = entry.getKey();
            List<URL> categoryList = entry.getValue();
            categoryNotified.put(category, categoryList);
            listener.notify(categoryList);

            // We will update our cache file after each notification.
            // When our Registry has a subscribed failure due to network jitter, we can return at least the existing
            // cache URL.
            if (localCacheEnabled) {
                saveProperties(url);
            }
        }
    }

    private void saveProperties(URL url) {
        if (file == null) {
            return;
        }

        try {
            StringBuilder buf = new StringBuilder();
            Map<String, List<URL>> categoryNotified = notified.get(url);
            if (categoryNotified != null) {
                for (List<URL> us : categoryNotified.values()) {
                    for (URL u : us) {
                        if (buf.length() > 0) {
                            buf.append(URL_SEPARATOR);
                        }
                        buf.append(u.toFullString());
                    }
                }
            }
            properties.setProperty(url.getServiceKey(), buf.toString());
            long version = lastCacheChanged.incrementAndGet();
            if (syncSaveFile) {
                doSaveProperties(version);
            } else {
                registryCacheExecutor.schedule(
                        () -> doSaveProperties(version), DEFAULT_INTERVAL_SAVE_PROPERTIES, TimeUnit.MILLISECONDS);
            }
        } catch (Throwable t) {
            logger.warn(INTERNAL_ERROR, "unknown error in registry module", "", t.getMessage(), t);
        }
    }

    @Override
    public void destroy() {
        if (logger.isInfoEnabled()) {
            logger.info("Destroy registry:" + getUrl());
        }
        Set<URL> destroyRegistered = new HashSet<>(getRegistered());
        if (!destroyRegistered.isEmpty()) {
            for (URL url : new HashSet<>(destroyRegistered)) {
                if (url.getParameter(DYNAMIC_KEY, true)) {
                    try {
                        unregister(url);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unregister url " + url);
                        }
                    } catch (Throwable t) {
                        // 1-8: Failed to unregister / unsubscribe url on destroy.
                        logger.warn(
                                REGISTRY_FAILED_DESTROY_UNREGISTER_URL,
                                "",
                                "",
                                "Failed to unregister url " + url + " to registry " + getUrl() + " on destroy, cause: "
                                        + t.getMessage(),
                                t);
                    }
                }
            }
        }
        Map<URL, Set<NotifyListener>> destroySubscribed = new HashMap<>(getSubscribed());
        if (!destroySubscribed.isEmpty()) {
            for (Map.Entry<URL, Set<NotifyListener>> entry : destroySubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    try {
                        unsubscribe(url, listener);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unsubscribe url " + url);
                        }
                    } catch (Throwable t) {
                        // 1-8: Failed to unregister / unsubscribe url on destroy.
                        logger.warn(
                                REGISTRY_FAILED_DESTROY_UNREGISTER_URL,
                                "",
                                "",
                                "Failed to unsubscribe url " + url + " to registry " + getUrl() + " on destroy, cause: "
                                        + t.getMessage(),
                                t);
                    }
                }
            }
        }
        registryManager.removeDestroyedRegistry(this);
    }

    protected boolean acceptable(URL urlToRegistry) {
        String pattern = registryUrl.getParameter(ACCEPTS_KEY);
        if (StringUtils.isEmpty(pattern)) {
            return true;
        }

        String[] accepts = COMMA_SPLIT_PATTERN.split(pattern);

        Set<String> allow =
                Arrays.stream(accepts).filter(p -> !p.startsWith("-")).collect(Collectors.toSet());
        Set<String> disAllow = Arrays.stream(accepts)
                .filter(p -> p.startsWith("-"))
                .map(p -> p.substring(1))
                .collect(Collectors.toSet());

        if (CollectionUtils.isNotEmpty(allow)) {
            // allow first
            return allow.contains(urlToRegistry.getProtocol());
        } else if (CollectionUtils.isNotEmpty(disAllow)) {
            // contains disAllow, deny
            return !disAllow.contains(urlToRegistry.getProtocol());
        } else {
            // default allow
            return true;
        }
    }

    @Override
    public String toString() {
        return getUrl().toString();
    }
}
