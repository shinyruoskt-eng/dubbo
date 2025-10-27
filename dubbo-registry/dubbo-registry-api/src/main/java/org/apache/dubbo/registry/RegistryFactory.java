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
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.SPI;

import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.common.extension.ExtensionScope.APPLICATION;

/**
 * ============================================================================
 * 文件概述：注册中心工厂SPI扩展接口
 * ============================================================================
 * 
 * 本接口是Dubbo注册中心模块的核心扩展点，定义了创建Registry实例的工厂方法。
 * Dubbo通过SPI机制加载不同的注册中心实现（Zookeeper、Nacos、Redis等），
 * 实现注册中心的可插拔和多注册中心并存。
 * 
 * 【关键】SPI扩展点设计：
 * 
 * 1. 自适应扩展（@Adaptive）：
 *    - 根据URL的protocol参数动态选择实现
 *    - 示例：zookeeper://127.0.0.1:2181 -> ZookeeperRegistryFactory
 *    - 示例：nacos://127.0.0.1:8848 -> NacosRegistryFactory
 *    - 机制：Dubbo自动生成代理类，运行时根据URL选择实现
 * 
 * 2. 应用级单例（scope = APPLICATION）：
 *    - 每个应用只有一个RegistryFactory实例
 *    - 多个Registry可以共享同一个Factory
 *    - 目的：减少资源占用，统一管理Registry实例
 * 
 * 3. 多注册中心支持：
 *    - 同一应用可以连接多个注册中心
 *    - 不同的URL创建不同的Registry实例
 *    - 通过AbstractRegistryFactory的缓存避免重复创建
 * 
 * 【中等】扩展点配置：
 * 
 * SPI配置文件：META-INF/dubbo/org.apache.dubbo.registry.RegistryFactory
 * <pre>
 * zookeeper=org.apache.dubbo.registry.zookeeper.ZookeeperRegistryFactory
 * nacos=org.apache.dubbo.registry.nacos.NacosRegistryFactory
 * redis=org.apache.dubbo.registry.redis.RedisRegistryFactory
 * multicast=org.apache.dubbo.registry.multicast.MulticastRegistryFactory
 * </pre>
 * 
 * 使用示例：
 * <pre>
 * // XML配置
 * <dubbo:registry protocol="zookeeper" address="127.0.0.1:2181"/>
 * 
 * // 代码配置
 * URL url = URL.valueOf("zookeeper://127.0.0.1:2181");
 * RegistryFactory factory = ExtensionLoader.getExtensionLoader(RegistryFactory.class)
 *     .getAdaptiveExtension();
 * Registry registry = factory.getRegistry(url);  // 自动选择ZookeeperRegistryFactory
 * </pre>
 * 
 * 【中等】典型实现类：
 * 
 * AbstractRegistryFactory（抽象基类）：
 * - 提供Registry实例缓存
 * - 避免重复创建相同URL的Registry
 * - 管理Registry生命周期
 * 
 * ZookeeperRegistryFactory：
 * - 创建ZookeeperRegistry实例
 * - 管理Zookeeper连接池
 * 
 * NacosRegistryFactory：
 * - 创建NacosRegistry实例
 * - 管理Nacos NamingService
 * 
 * (SPI, Singleton, ThreadSafe)
 * 
 * @see org.apache.dubbo.registry.support.AbstractRegistryFactory 抽象基类实现
 */
@SPI(scope = APPLICATION)
public interface RegistryFactory {

    /**
     * 【关键】连接注册中心并创建Registry实例
     * 
     * ═══════════════════════════════════════════════════════════════════════
     * 核心职责：根据URL连接到指定的注册中心，返回Registry操作接口
     * ═══════════════════════════════════════════════════════════════════════
     * 
     * 【关键】连接契约和支持的功能：
     * 
     * 1. 启动检查（check参数）：
     *    - check=false：不检查连接，即使注册中心不可用也返回Registry
     *    - check=true（默认）：连接失败抛出异常
     *    示例：zookeeper://127.0.0.1:2181?check=false
     * 
     * 2. 认证授权（username:password）：
     *    - 格式：protocol://username:password@host:port
     *    - 示例：zookeeper://admin:123456@127.0.0.1:2181
     *    - 用途：访问需要认证的注册中心
     * 
     * 3. 集群备份地址（backup参数）：
     *    - 格式：address=主地址&backup=备份地址1,备份地址2
     *    - 示例：zookeeper://127.0.0.1:2181?backup=127.0.0.2:2181,127.0.0.3:2181
     *    - 作用：主注册中心不可用时自动切换到备份地址
     * 
     * 4. 本地缓存文件（file参数）：
     *    - 格式：file=/path/to/cache.properties
     *    - 默认：~/.dubbo/dubbo-registry-{app}-{addr}.cache
     *    - 作用：断线容错，注册中心不可用时从文件读取
     * 
     * 5. 请求超时（timeout参数）：
     *    - 格式：timeout=5000（毫秒）
     *    - 默认：通常为5000ms
     *    - 作用：控制注册/订阅操作的超时时间
     * 
     * 6. 会话超时（session参数）：
     *    - 格式：session=60000（毫秒）
     *    - 默认：Zookeeper通常为60000ms
     *    - 作用：控制临时节点的存活时间
     * 
     * 【困难】URL格式和参数详解：
     * 
     * 完整URL格式：
     * <pre>
     * protocol://[username:password@]host:port[/path]?param1=value1&param2=value2
     * </pre>
     * 
     * 示例1：基础连接
     * <pre>
     * zookeeper://127.0.0.1:2181
     * </pre>
     * 
     * 示例2：集群+备份
     * <pre>
     * zookeeper://127.0.0.1:2181,127.0.0.2:2181,127.0.0.3:2181?backup=127.0.0.4:2181
     * </pre>
     * 
     * 示例3：完整配置
     * <pre>
     * zookeeper://admin:pass@127.0.0.1:2181?
     *   backup=127.0.0.2:2181&
     *   timeout=5000&
     *   session=60000&
     *   file=/data/registry.cache&
     *   check=false
     * </pre>
     * 
     * 【中等】自适应扩展机制：
     * 
     * @Adaptive注解作用：
     * - Dubbo自动生成RegistryFactory$Adaptive代理类
     * - 运行时根据URL的protocol参数选择实现
     * 
     * 选择逻辑：
     * <pre>
     * String protocol = url.getProtocol();  // 获取协议名
     * RegistryFactory factory = ExtensionLoader
     *     .getExtensionLoader(RegistryFactory.class)
     *     .getExtension(protocol);  // 根据协议名加载实现
     * return factory.getRegistry(url);
     * </pre>
     * 
     * 【中等】缓存机制（AbstractRegistryFactory实现）：
     * 
     * 缓存策略：
     * - 相同URL的多次调用返回同一个Registry实例
     * - 缓存Key：URL（去掉某些动态参数）
     * - 目的：避免重复连接，共享注册中心资源
     * 
     * 生命周期：
     * - 创建：首次调用getRegistry时创建并缓存
     * - 销毁：应用关闭时统一销毁所有Registry
     * 
     * 【中等】典型使用场景：
     * 
     * 场景1：Provider启动时获取Registry
     * <pre>
     * URL registryUrl = URL.valueOf("zookeeper://127.0.0.1:2181");
     * RegistryFactory factory = ExtensionLoader
     *     .getExtensionLoader(RegistryFactory.class)
     *     .getAdaptiveExtension();
     * Registry registry = factory.getRegistry(registryUrl);
     * registry.register(providerUrl);  // 注册服务
     * </pre>
     * 
     * 场景2：多注册中心
     * <pre>
     * Registry zkRegistry = factory.getRegistry(
     *     URL.valueOf("zookeeper://127.0.0.1:2181"));
     * Registry nacosRegistry = factory.getRegistry(
     *     URL.valueOf("nacos://127.0.0.1:8848"));
     * 
     * // 同时注册到两个注册中心
     * zkRegistry.register(url);
     * nacosRegistry.register(url);
     * </pre>
     * 
     * 场景3：测试环境禁用检查
     * <pre>
     * // 注册中心可能未启动，但允许应用启动
     * URL url = URL.valueOf("zookeeper://127.0.0.1:2181?check=false");
     * Registry registry = factory.getRegistry(url);  // 不抛异常
     * </pre>
     * 
     * 【关键】前置条件：
     * - URL不为null且格式正确
     * - protocol参数对应的扩展实现已加载
     * - 如果check=true，注册中心必须可访问
     * 
     * 【关键】后置条件：
     * - 返回非null的Registry实例
     * - Registry实例已缓存（相同URL再次调用返回同一实例）
     * - 如果check=true，确保注册中心连接成功
     * 
     * 【关键】可见副作用：
     * - 网络连接：建立到注册中心的TCP连接
     * - 内存分配：创建Registry实例及相关资源
     * - 线程创建：可能创建心跳、重连等后台线程
     * - 缓存更新：将Registry实例加入AbstractRegistryFactory的缓存
     * 
     * 【关键】异常处理：
     * - IllegalArgumentException：URL为null或格式错误
     * - IllegalStateException：check=true时连接失败
     * - 其他异常：网络故障、认证失败等
     * 
     * @param url 注册中心地址URL，不允许为空
     *            格式：protocol://host:port?param=value
     * @return Registry实例，永远不返回null
     * @throws IllegalArgumentException 如果url为null
     * @throws IllegalStateException 如果check=true且连接失败
     */
    @Adaptive({PROTOCOL_KEY})
    Registry getRegistry(URL url);
}
