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

import java.util.List;

/**
 * ============================================================================
 * 文件概述：Dubbo注册中心服务契约接口
 * ============================================================================
 * 
 * 本文件定义了注册中心的核心业务操作契约，包括服务注册、注销、订阅、取消订阅和查询。
 * 这是Dubbo服务注册与发现机制的基础接口，所有注册中心实现（Zookeeper、Nacos等）
 * 都必须遵循此契约，确保不同注册中心实现的行为一致性。
 * 
 * 【关键】设计原则和核心约束：
 * 1. 线程安全：所有方法必须是线程安全的，支持并发调用
 * 2. 幂等性：重复调用register/unregister应该是安全的
 * 3. 异步重试：网络故障时在后台自动重试（check=false时）
 * 4. 数据分类：支持providers/consumers/routers/configurators等分类存储
 * 5. 持久化：支持dynamic参数控制数据是否持久化
 * 
 * 【困难】数据流架构：
 * Provider注册流程：
 *   ServiceConfig.export() -> RegistryProtocol.export() -> Registry.register()
 *   -> 注册中心存储 -> 触发订阅通知 -> Consumer.NotifyListener.notify()
 * 
 * Consumer订阅流程：
 *   ReferenceConfig.get() -> RegistryProtocol.refer() -> Registry.subscribe()
 *   -> 首次同步返回服务列表 -> 后续异步通知变更
 * 
 * 【中等】注意事项：
 * - register/subscribe在首次调用时必须是阻塞的，确保数据已写入或已读取
 * - URL参数变化会被视为不同的注册项，不会相互覆盖
 * - 注册中心宕机重启后，客户端需要自动恢复注册和订阅状态
 * 
 * @see org.apache.dubbo.registry.Registry Registry接口（继承本接口）
 * @see org.apache.dubbo.registry.RegistryFactory#getRegistry(URL) 获取Registry实例
 */
public interface RegistryService {

    /**
     * 【关键】注册服务元数据到注册中心
     * 
     * ═══════════════════════════════════════════════════════════════════════
     * 核心职责：将Provider服务、Consumer地址、路由规则、配置覆盖等元数据注册到注册中心
     * ═══════════════════════════════════════════════════════════════════════
     * 
     * 【困难】注册契约和行为约束（必须严格遵守）：
     * 
     * 1. 启动检查机制（check参数）：
     *    - check=true（默认）：注册失败立即抛出异常，阻止服务启动（fail-fast策略）
     *    - check=false：注册失败不抛异常，在后台启动重试任务（适用于注册中心不稳定场景）
     *    设计考量：生产环境建议check=true确保服务可用性，开发环境可用check=false提升容错性
     * 
     * 2. 数据持久化策略（dynamic参数）：
     *    - dynamic=true（默认）：临时节点，进程退出后自动删除（适合服务实例）
     *    - dynamic=false：持久节点，进程退出后数据保留（适合配置、路由规则等元数据）
     *    实现原理：Zookeeper中分别对应临时节点和持久节点
     *    误用风险：如果服务实例使用dynamic=false，异常退出后注册中心会残留脏数据导致调用失败
     * 
     * 3. 分类存储机制（category参数）：
     *    - 默认category=providers：服务提供者列表
     *    - category=consumers：服务消费者列表（用于服务治理）
     *    - category=routers：路由规则（用于流量控制）
     *    - category=configurators：动态配置（用于参数覆盖）
     *    数据结构：/dubbo/{serviceName}/{category}/{url}
     *    通知隔离：订阅providers的Consumer不会收到routers变更的通知（避免无效刷新）
     * 
     * 4. 容错恢复保证：
     *    - 注册中心重启：客户端检测到连接断开后，自动重新注册所有URL
     *    - 网络抖动：短暂断开不删除临时节点（会话超时时间内），恢复后继续使用原节点
     *    - 数据完整性：必须确保注册的URL不会因网络问题丢失
     * 
     * 5. 并发注册处理：
     *    - 相同URL重复注册：幂等操作，不会创建多个节点
     *    - 不同参数的相同服务：视为不同注册项，可以共存（例如不同版本）
     *    - 并发安全：必须支持多线程同时调用register()
     * 
     * 【中等】典型使用场景：
     * 
     * 场景1：Provider启动时注册服务
     * <pre>
     * URL url = URL.valueOf("dubbo://192.168.1.100:20880/com.foo.BarService?version=1.0.0&application=demo-provider");
     * registry.register(url);  // 注册到/dubbo/com.foo.BarService/providers/节点下
     * </pre>
     * 
     * 场景2：注册路由规则（需要持久化）
     * <pre>
     * URL rule = URL.valueOf("route://0.0.0.0/com.foo.BarService?category=routers&dynamic=false&rule=...");
     * registry.register(rule);  // 持久化路由规则，即使进程退出规则仍然生效
     * </pre>
     * 
     * 场景3：Consumer注册消费者信息（用于服务治理监控）
     * <pre>
     * URL consumer = URL.valueOf("consumer://192.168.1.101/com.foo.BarService?category=consumers&application=demo-consumer");
     * registry.register(consumer);  // 让Provider知道有哪些Consumer在调用
     * </pre>
     * 
     * 【易懂】误用示例和常见陷阱：
     * 
     * // 误用示例1：服务实例使用dynamic=false导致脏数据
     * URL url = URL.valueOf("dubbo://192.168.1.100:20880/com.foo.BarService?dynamic=false");
     * registry.register(url);  // ❌ 错误！进程异常退出后节点不会删除，Consumer会调用到已下线的服务
     * 
     * // 误用示例2：忘记设置category导致分类错误
     * URL router = URL.valueOf("route://0.0.0.0/com.foo.BarService?rule=...");
     * registry.register(router);  // ❌ 错误！没有category参数，会被当成providers注册
     * 
     * // 误用示例3：注册空URL
     * registry.register(null);  // ❌ 抛出IllegalArgumentException
     * 
     * // 正确用法：Provider服务注册（使用默认参数）
     * URL url = URL.valueOf("dubbo://192.168.1.100:20880/com.foo.BarService?version=1.0.0&application=demo-provider");
     * registry.register(url);  // ✓ 正确：dynamic=true（临时节点），category=providers
     * 
     * 【困难】性能和可靠性考量：
     * 
     * 时间复杂度：O(1) - 本地操作 + O(log N) - 注册中心网络操作（N为同类节点数）
     * 网络开销：单次RPC调用到注册中心，通常<10ms（局域网）
     * 并发性能：受注册中心限制，Zookeeper通常支持1000+ TPS
     * 重试策略：失败后以固定间隔（默认5秒）无限重试，直到成功
     * 
     * 瓶颈识别：
     * - 大规模服务启动：同时注册大量服务可能导致注册中心压力过大
     * - 频繁注册注销：不应该用于实时数据更新，会影响注册中心性能
     * 
     * 优化建议：
     * - 批量启动服务时，可以添加随机延迟避免同时注册
     * - 非必要的参数不要放在URL中，减少注册中心数据量
     * 
     * 【关键】前置条件：
     * - 注册中心连接已建立（RegistryFactory.getRegistry()已调用）
     * - URL对象不为空且格式正确
     * - 必要的参数已设置（如application、interface等）
     * 
     * 【关键】后置条件：
     * - 注册中心中存在对应的节点（如果check=true）
     * - 后台重试任务已启动（如果check=false且注册失败）
     * - 相关订阅者会收到新增服务的通知
     * 
     * 【关键】可见副作用：
     * - 修改注册中心状态：创建或更新节点
     * - 触发订阅通知：所有订阅了该服务的Consumer会收到notify回调
     * - 启动重试任务：失败时在后台线程池中调度重试任务
     * 
     * 【关键】并发安全性：
     * - 线程安全：可以多线程并发调用
     * - 幂等性：重复注册相同URL是安全的
     * - 无锁设计：实现应避免使用重量级锁，推荐CAS和ConcurrentHashMap
     * 
     * @param url 注册信息URL，不允许为空
     *            示例：dubbo://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     * @throws IllegalArgumentException 如果url为null
     * @throws IllegalStateException 如果check=true且注册失败
     */
    void register(URL url);

    /**
     * 【关键】从注册中心注销服务元数据
     * 
     * ═══════════════════════════════════════════════════════════════════════
     * 核心职责：删除之前注册的服务元数据，通知订阅者服务已下线
     * ═══════════════════════════════════════════════════════════════════════
     * 
     * 【中等】注销契约和行为约束：
     * 
     * 1. 精确匹配原则：
     *    - 必须使用完整URL进行匹配（包括所有参数）
     *    - URL参数顺序不影响匹配（dubbo://host/service?a=1&b=2 等同于 ?b=2&a=1）
     *    - 只有完全匹配才能注销，部分匹配会被忽略
     * 
     * 2. 持久化数据检查（dynamic=false）：
     *    - 如果注销持久化数据但节点不存在，抛出IllegalStateException
     *    - 原因：持久化数据不应该无故消失，缺失表示可能存在数据不一致
     *    - 临时数据（dynamic=true）找不到则直接忽略（可能已经被会话超时自动删除）
     * 
     * 3. 注销时机：
     *    - 正常停机：应用关闭时调用unregister主动注销（优雅下线）
     *    - 异常停机：临时节点依赖会话超时机制自动删除，持久节点需要人工清理
     *    - 服务降级：临时摘除某个服务实例时调用unregister
     * 
     * 【易懂】典型使用场景：
     * 
     * 场景1：Provider优雅停机
     * <pre>
     * // 应用关闭时，ServiceConfig.unexport()会调用
     * registry.unregister(providerUrl);  // 通知Consumer该实例已下线
     * </pre>
     * 
     * 场景2：删除持久化路由规则
     * <pre>
     * URL rule = URL.valueOf("route://0.0.0.0/com.foo.BarService?category=routers&dynamic=false");
     * registry.unregister(rule);  // 删除之前注册的路由规则
     * </pre>
     * 
     * 【易懂】误用示例：
     * 
     * // 误用示例1：URL参数不完整导致匹配失败
     * registry.register(URL.valueOf("dubbo://host/service?version=1.0&group=demo"));
     * registry.unregister(URL.valueOf("dubbo://host/service?version=1.0"));  // ❌ 参数不完整，无法匹配
     * 
     * // 误用示例2：注销不存在的持久化数据
     * URL persistent = URL.valueOf("route://0.0.0.0/service?category=routers&dynamic=false");
     * registry.unregister(persistent);  // ❌ 如果该URL从未注册过，会抛出IllegalStateException
     * 
     * // 正确用法：使用相同的URL对象或完全相同的URL字符串
     * URL url = URL.valueOf("dubbo://host/service?version=1.0&group=demo");
     * registry.register(url);
     * registry.unregister(url);  // ✓ 正确
     * 
     * 【中等】副作用和影响：
     * - 注册中心状态变更：删除对应节点
     * - 触发通知：订阅者会收到服务下线的empty协议通知
     * - 停止重试：如果该URL有失败重试任务，会被取消
     * 
     * @param url 要注销的注册信息，不允许为空
     *            示例：dubbo://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     * @throws IllegalArgumentException 如果url为null
     * @throws IllegalStateException 如果dynamic=false的持久化数据找不到
     */
    void unregister(URL url);

    /**
     * 【关键】订阅注册中心数据变更并自动接收推送通知
     * 
     * ═══════════════════════════════════════════════════════════════════════
     * 核心职责：Consumer订阅Provider列表，当Provider上下线时自动收到通知并更新本地缓存
     * ═══════════════════════════════════════════════════════════════════════
     * 
     * 【困难】订阅契约和核心机制（Dubbo服务发现的关键）：
     * 
     * 1. 首次同步阻塞返回（最关键的设计约束）：
     *    - subscribe()方法必须在首次通知完成后才能返回（阻塞语义）
     *    - 保证调用subscribe()后立即就能使用服务列表，无需等待异步回调
     *    - 实现方式：内部调用listener.notify()完成首次通知后才返回
     *    - 时序保证：subscribe() -> 从注册中心拉取数据 -> notify(初始列表) -> 返回
     *    - 设计考量：首次必须同步是为了避免Consumer启动后立即调用服务但还没收到Provider列表
     * 
     * 2. 后续异步推送通知：
     *    - 首次返回后，后续的Provider变更通过异步回调listener.notify()
     *    - 推送时机：Provider注册/注销、配置变更、路由规则更新
     *    - 推送保证：注册中心保证最终一致性，可能有短暂延迟（通常<100ms）
     *    - 去重机制：实现应避免推送重复的数据（例如短时间内同一Provider多次上线）
     * 
     * 3. 分类订阅机制（category参数）：
     *    - 默认订阅providers分类（服务提供者列表）
     *    - category=routers：只订阅路由规则变更
     *    - category=configurators：只订阅配置覆盖规则
     *    - category=providers,routers：订阅多个分类，用逗号分隔
     *    - category=*：订阅所有分类（慎用，会收到大量通知）
     *    - 通知隔离：不同category的变更会分别通知，不会相互干扰
     * 
     * 4. 条件查询和通配符匹配：
     *    - 精确匹配：interface=com.foo.BarService&version=1.0.0&group=demo
     *    - 通配符匹配：interface=*（订阅所有接口）、version=*（所有版本）
     *    - 全量订阅：interface=*&group=*&version=*&category=*（监控中心使用）
     *    - 匹配规则：只有满足订阅条件的Provider才会被推送
     *    - 性能警告：使用*通配符会订阅大量数据，仅用于监控中心等特殊场景
     * 
     * 5. 启动检查和容错（check参数）：
     *    - check=true（默认）：首次订阅如果找不到Provider则抛异常（fail-fast）
     *    - check=false：找不到Provider不抛异常，在后台重试（适用于循环依赖场景）
     *    - 后台重试：check=false时如果订阅失败，会启动定时任务重试（默认5秒间隔）
     * 
     * 6. 自动恢复机制（关键可靠性保证）：
     *    - 注册中心重启：客户端检测到连接断开，重连后自动重新订阅
     *    - 网络抖动：短暂断开不影响订阅状态，恢复后继续接收通知
     *    - 断线缓存：连接中断期间依赖本地缓存文件提供服务（AbstractRegistry）
     *    - 数据一致性：重连后会全量拉取数据确保与注册中心状态一致
     * 
     * 7. 多监听器共存：
     *    - 同一个URL可以注册多个NotifyListener（不会相互覆盖）
     *    - 使用场景：同一个服务被多个模块引用，每个模块独立监听
     *    - 通知隔离：每个Listener独立接收通知，互不影响
     * 
     * 【困难】数据流和通知机制：
     * 
     * 订阅流程（完整链路）：
     * <pre>
     * Consumer启动
     *   ↓
     * ReferenceConfig.get()
     *   ↓
     * RegistryProtocol.refer() 
     *   ↓
     * Registry.subscribe(consumerUrl, notifyListener)  // 阻塞调用
     *   ↓
     * [注册中心] 查询匹配的Provider列表
     *   ↓
     * listener.notify(providerUrls)  // 首次同步通知（方法内部）
     *   ↓
     * InvokerRefreshListener.refreshInvoker()  // 刷新Invoker列表
     *   ↓
     * subscribe()方法返回  // 此时Consumer已经拿到Provider列表
     *   ↓
     * [后续] Provider变更 -> 注册中心推送 -> listener.notify() -> 异步更新
     * </pre>
     * 
     * 通知数据格式：
     * <pre>
     * List<URL> urls包含：
     * - Provider URL列表：dubbo://host:port/service?参数
     * - 空列表协议：empty://host/service（表示该服务所有Provider已下线）
     * - 路由规则URL：route://...
     * - 配置覆盖URL：override://...
     * 
     * 分类通知：按category分组，每个category独立触发一次notify
     * </pre>
     * 
     * 【中等】典型使用场景：
     * 
     * 场景1：Consumer订阅Provider服务（最常见）
     * <pre>
     * URL consumerUrl = URL.valueOf("consumer://192.168.1.101/com.foo.BarService?version=1.0.0&application=demo-consumer");
     * registry.subscribe(consumerUrl, urls -> {
     *     // 收到Provider列表变更通知
     *     System.out.println("Provider变更，新列表：" + urls);
     *     // 刷新RPC调用的Invoker列表
     * });
     * // 方法返回时已经收到首次通知，可以立即使用服务
     * </pre>
     * 
     * 场景2：监控中心全量订阅（使用通配符）
     * <pre>
     * URL monitorUrl = URL.valueOf("consumer://0.0.0.0/监控?interface=*&group=*&version=*&category=*");
     * registry.subscribe(monitorUrl, urls -> {
     *     // 收到所有服务的所有变更
     *     // 用于构建服务依赖关系图、流量监控等
     * });
     * </pre>
     * 
     * 场景3：订阅路由规则
     * <pre>
     * URL routerUrl = URL.valueOf("consumer://host/com.foo.BarService?category=routers");
     * registry.subscribe(routerUrl, urls -> {
     *     // 只接收路由规则变更，不接收Provider变更
     *     // 用于动态流量控制
     * });
     * </pre>
     * 
     * 【易懂】误用示例和陷阱：
     * 
     * // 误用示例1：异步使用首次数据（错误的理解）
     * AtomicReference<List<URL>> providers = new AtomicReference<>();
     * registry.subscribe(url, providers::set);
     * // ❌ 错误：这里providers.get()一定不是null，因为subscribe()是阻塞的
     * List<URL> list = providers.get();  // 这里list一定有值（可能是empty协议）
     * 
     * // 误用示例2：过度使用通配符
     * URL url = URL.valueOf("consumer://host/service?interface=*&category=*");
     * registry.subscribe(url, listener);  // ❌ 性能问题：会收到整个注册中心的所有变更
     * 
     * // 误用示例3：忘记处理empty协议
     * registry.subscribe(url, urls -> {
     *     for (URL u : urls) {
     *         // ❌ 错误：没有判断empty协议，可能把empty URL当成正常Provider
     *         if (!"empty".equals(u.getProtocol())) {
     *             // 正确的处理方式
     *         }
     *     }
     * });
     * 
     * // 正确用法：标准的Consumer订阅
     * URL url = URL.valueOf("consumer://host/com.foo.BarService?version=1.0&group=demo");
     * registry.subscribe(url, urls -> {
     *     // 过滤empty协议
     *     List<URL> validProviders = urls.stream()
     *         .filter(u -> !"empty".equals(u.getProtocol()))
     *         .collect(Collectors.toList());
     *     // 更新Invoker列表
     * });
     * 
     * 【困难】性能和可靠性分析：
     * 
     * 时间复杂度：
     * - 首次订阅：O(N) - N为匹配的Provider数量，需要从注册中心拉取并通知
     * - 后续通知：O(M) - M为单次变更的Provider数量，通常M << N
     * 
     * 网络开销：
     * - 首次订阅：一次RPC调用 + 数据传输（取决于Provider数量）
     * - 推送通知：增量推送，只传输变更的Provider
     * 
     * 并发性能：
     * - listener.notify()可能在不同线程被调用，实现必须是线程安全的
     * - 大量Consumer同时订阅可能导致注册中心压力过大
     * 
     * 可靠性保证：
     * - 至少一次通知：Provider变更一定会通知到Consumer（可能重复）
     * - 最终一致性：短暂不一致可接受，但最终会同步到最新状态
     * - 本地缓存：即使注册中心宕机，也能使用缓存数据继续提供服务
     * 
     * 瓶颈和优化：
     * - 瓶颈：全量订阅（interface=*）会导致大量数据传输和频繁通知
     * - 优化：精确订阅（指定interface、version、group）减少数据量
     * - 优化：通知合并（短时间内多次变更合并为一次通知）
     * 
     * 【关键】前置条件：
     * - 注册中心连接已建立
     * - URL和listener都不为空
     * - URL包含必要的订阅参数（至少有interface）
     * 
     * 【关键】后置条件（方法返回时）：
     * - listener已经收到首次通知（至少一次notify调用）
     * - 订阅关系已保存到注册中心（持久化订阅信息）
     * - 本地缓存已更新（AbstractRegistry.notified）
     * - 后台监听线程已启动（用于接收推送）
     * 
     * 【关键】可见副作用：
     * - 注册中心状态：创建订阅节点（临时节点）
     * - 本地状态：更新subscribed和notified缓存
     * - 线程创建：可能创建监听线程（取决于注册中心实现）
     * - 文件IO：更新本地缓存文件（如果启用了本地缓存）
     * 
     * 【关键】并发安全性：
     * - 线程安全：可以多线程并发订阅不同的URL
     * - 监听器隔离：同一URL的多个listener独立通知，互不影响
     * - 通知顺序：不保证多次变更的通知顺序（网络延迟可能导致乱序）
     * 
     * @param url 订阅条件URL，不允许为空
     *            示例：consumer://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     * @param listener 变更通知监听器，不允许为空，会在首次订阅和后续变更时被回调
     * @throws IllegalArgumentException 如果url或listener为null
     * @throws IllegalStateException 如果check=true且首次订阅失败或找不到Provider
     */
    void subscribe(URL url, NotifyListener listener);

    /**
     * 【中等】取消订阅，停止接收数据变更通知
     * 
     * 核心职责：移除之前注册的订阅关系和监听器，不再接收推送通知
     * 
     * 【易懂】取消订阅契约：
     * 
     * 1. 精确匹配：必须使用与subscribe时相同的URL和listener对象
     *    - URL必须完全匹配（包括所有参数）
     *    - listener必须是同一个对象实例（使用==比较，不是equals）
     * 
     * 2. 容错处理：如果从未订阅过，直接忽略（不抛异常）
     * 
     * 3. 多监听器：如果同一URL有多个listener，只移除匹配的那一个
     * 
     * 【易懂】使用场景：
     * - Consumer关闭时取消订阅
     * - 动态取消某个服务引用
     * - 更换监听器（先unsubscribe再subscribe新的）
     * 
     * 【易懂】副作用：
     * - 删除注册中心的订阅节点（如果该URL没有其他listener）
     * - 清理本地缓存中的notified数据
     * - 停止接收后续的变更通知
     * 
     * @param url 订阅条件，必须与subscribe时相同
     *            示例：consumer://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     * @param listener 监听器，必须与subscribe时是同一个对象
     * @throws IllegalArgumentException 如果url或listener为null
     */
    void unsubscribe(URL url, NotifyListener listener);

    /**
     * 【中等】查询注册中心的服务列表（Pull模式，对应subscribe的Push模式）
     * 
     * 核心职责：主动拉取匹配条件的Provider列表，只返回一次结果，不建立订阅关系
     * 
     * 【中等】与subscribe的区别：
     * 
     * subscribe（推模式）：
     * - 建立持久订阅关系
     * - 首次返回结果 + 后续自动推送变更
     * - 适用于需要实时感知变更的场景（Consumer引用服务）
     * 
     * lookup（拉模式）：
     * - 不建立订阅关系
     * - 只返回一次查询结果
     * - 适用于一次性查询场景（管理工具、健康检查）
     * 
     * 【易懂】使用场景：
     * 
     * 场景1：管理工具查询服务列表
     * <pre>
     * URL query = URL.valueOf("consumer://0.0.0.0/com.foo.BarService?version=1.0");
     * List<URL> providers = registry.lookup(query);
     * // 展示当前可用的Provider列表，不需要持续监听
     * </pre>
     * 
     * 场景2：服务健康检查
     * <pre>
     * List<URL> providers = registry.lookup(consumerUrl);
     * if (providers.isEmpty()) {
     *     // 服务不可用告警
     * }
     * </pre>
     * 
     * 场景3：临时查询路由规则
     * <pre>
     * URL routerQuery = URL.valueOf("consumer://host/service?category=routers");
     * List<URL> routers = registry.lookup(routerQuery);
     * // 获取当前的路由规则配置
     * </pre>
     * 
     * 【易懂】返回值说明：
     * - 空列表：没有匹配的Provider（不是null）
     * - empty协议：可能包含empty://协议的URL，表示服务存在但所有Provider已下线
     * - 混合结果：可能包含providers、routers、configurators等多种category的URL
     * 
     * 【易懂】性能考量：
     * - 每次调用都是实时查询注册中心，有网络开销
     * - 不建议高频调用，适合低频的管理操作
     * - 如果需要持续获取数据，应该使用subscribe而不是循环调用lookup
     * 
     * @param url 查询条件，不允许为空
     *            示例：consumer://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     * @return 注册信息列表，可能为空列表，含义与{@link NotifyListener#notify(List)}的参数相同
     * @see NotifyListener#notify(List) 通知的数据格式
     */
    List<URL> lookup(URL url);
}
