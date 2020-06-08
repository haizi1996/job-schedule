package com.hailin.job.schedule.core.reg.base;


import org.apache.curator.framework.state.ConnectionStateListener;

import java.util.List;

/**
 * 用户协调的分布式试注册中心
 * @author zhanghailin
 */
public interface CoordinatorRegistryCenter extends RegistCenter {
    /**
     * 直接从注册中心而非本地缓存获取数据.
     *
     * @param key 键
     * @return 值
     */
    String getDirectly(String key);

    /**
     * 获取子节点名称集合.
     *
     * @param key 键
     * @return 子节点名称集合
     */
    List<String> getChildrenKeys(String key);

    /**
     * 持久化临时注册数据.
     *
     * @param key 键
     * @param value 值
     */
    void persistEphemeral(String key, String value);

    /**
     * 持久化临时顺序注册数据.
     *
     * @param key 键
     */
    void persistEphemeralSequential(String key);

    /**
     * 获取会话超时时间
     * @return 会话超时时间
     */
    long getSessionTimeout();

    /**
     * 获取namespace
     * @return
     */
    String getNamespace();

    /**
     * 设置executorName
     * @param executorName
     */
    void setExecutorName(String executorName);

    /**
     * 获取executorName
     * @return
     */
    String getExecutorName();

    /**
     * 添加连接状态变更事件监听
     * @param listener
     */
    void addConnectionStateListener(ConnectionStateListener listener);

    /**
     * 删除连接状态变更事件监听
     * @param listener
     */
    void removeConnectionStateListener(ConnectionStateListener listener);

    /**
     * 是否己连上
     * @return
     */
    boolean isConnected();

    /**
     * 使用基于namespace的客户端，如果namespace为null，则视为不使用namespace
     * @param namespace
     * @return
     */
    CoordinatorRegistryCenter usingNamespace(String namespace);
    /**
     * 添加本地缓存.
     *
     * @param cachePath 需加入缓存的路径
     */
    void addCacheData(String cachePath);
    /**
     * 释放本地缓存.
     *
     * @param cachePath 需释放缓存的路径
     */
    void evictCacheData(String cachePath);

    /**
     * 获取注册中心数据缓存对象.
     *
     * @param cachePath 缓存的节点路径
     * @return 注册中心数据缓存对象
     */
    Object getRawCache(String cachePath);

}
