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

import org.apache.dubbo.common.Node;
import org.apache.dubbo.common.URL;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_DELAY_NOTIFICATION_TIME;
import static org.apache.dubbo.common.constants.CommonConstants.REGISTRY_DELAY_NOTIFICATION_KEY;

/**
 * ============================================================================
 * 文件概述：Dubbo注册中心核心接口定义
 * ============================================================================
 * 
 * 本文件定义了Dubbo注册中心的核心接口Registry，是服务注册与发现机制的顶层抽象。
 * Registry整合了Node（生命周期管理）和RegistryService（注册服务）两个接口，
 * 为Zookeeper、Nacos等不同注册中心实现提供统一的访问契约。
 * 
 * 【关键】核心设计理念：
 * 1. 接口分层：Registry = Node（生命周期） + RegistryService（业务操作）
 * 2. 扩展点：通过RegistryFactory SPI动态加载不同注册中心实现
 * 3. 通知延迟：支持配置延迟通知避免频繁变更带来的抖动
 * 4. 服务发现模式：支持传统接口级和应用级两种服务发现模式
 * 
 * 【中等】使用场景：
 * - Provider端：在服务启动时调用register()注册服务元数据
 * - Consumer端：调用subscribe()订阅服务变更并通过NotifyListener接收通知
 * - 配置中心：通过register()动态注册路由规则、负载均衡策略等元数据
 * 
 * 【易懂】本接口不直接使用，而是通过RegistryFactory.getRegistry(url)获取实例
 * 
 * @see org.apache.dubbo.registry.RegistryFactory#getRegistry(URL) 注册中心实例工厂方法
 * @see org.apache.dubbo.registry.support.AbstractRegistry 核心抽象实现（提供本地缓存）
 */
public interface Registry extends Node, RegistryService {
    
    /**
     * 【易懂】获取通知延迟时间（毫秒）
     * 
     * 设计背景：注册中心可能会在短时间内收到大量服务变更事件（如批量上下线），
     * 如果立即通知会导致Consumer频繁刷新服务列表，影响性能。通过延迟通知可以
     * 将短时间内的多次变更合并为一次通知，减少无效刷新。
     * 
     * 默认延迟时间：DEFAULT_DELAY_NOTIFICATION_TIME（通常为0，即不延迟）
     * 
     * 参数来源：从注册中心URL的registry.delay.notification参数读取
     * 
     * @return 延迟通知的毫秒数
     */
    default int getDelay() {
        return getUrl().getParameter(REGISTRY_DELAY_NOTIFICATION_KEY, DEFAULT_DELAY_NOTIFICATION_TIME);
    }

    /**
     * 【中等】判断是否为应用级服务发现模式
     * 
     * 设计背景：Dubbo 3.0引入了应用级服务发现来替代传统的接口级发现。
     * 接口级发现：每个服务接口在注册中心有独立的节点（粒度细，数据量大）
     * 应用级发现：以应用为粒度注册（粒度粗，性能更好，适合大规模集群）
     * 
     * 区分意义：
     * - 接口级：适合小规模服务，能精确控制每个接口的配置
     * - 应用级：适合大规模微服务，减少注册中心压力，提升性能
     * 
     * 默认返回false表示传统接口级发现，子类可重写此方法返回true启用应用级发现
     * 
     * @return true=应用级服务发现模式，false=接口级服务发现模式（默认）
     */
    default boolean isServiceDiscovery() {
        return false;
    }

    /**
     * 【易懂】服务重新导出时的注册操作
     * 
     * 使用场景：当服务配置发生变化（如权重调整、路由规则变更）需要重新导出服务时，
     * 通过此方法重新注册更新后的服务元数据到注册中心。
     * 
     * 默认行为：直接委托给register()方法，子类可以重写实现特殊的重注册逻辑
     * （例如先清理旧数据再注册新数据，或者使用原子更新操作）
     * 
     * @param url 要重新注册的服务URL（包含更新后的参数）
     */
    default void reExportRegister(URL url) {
        register(url);
    }

    /**
     * 【易懂】服务重新导出时的注销操作
     * 
     * 使用场景：在reExportRegister之前调用，用于清理旧的服务注册信息。
     * 主要用于服务配置变更场景，确保注册中心数据的一致性。
     * 
     * 默认行为：直接委托给unregister()方法
     * 
     * @param url 要注销的服务URL
     */
    default void reExportUnregister(URL url) {
        unregister(url);
    }
}
