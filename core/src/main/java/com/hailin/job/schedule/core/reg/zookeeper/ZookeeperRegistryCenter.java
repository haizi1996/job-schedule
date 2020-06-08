package com.hailin.job.schedule.core.reg.zookeeper;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.job.schedule.core.reg.exception.RegExceptionHandler;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 基于Zookeeper的注册中心.
 * @author zhanghailin
 */
public class ZookeeperRegistryCenter implements CoordinatorRegistryCenter {

    @Override
    public void addCacheData(String cachePath) {

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperRegistryCenter.class);

    private static final String SLASH_CONSTNAT = "/";

    private ZookeeperConfiguration zkConfig;

    private CuratorFramework client;

    /**
     * 最小连接超时时间
     */
    private static final int MIN_CONNECTION_TIMEOUT = 20 * 1000;

    /**
     * 最小会话超时时间
     */
    private static final int MIN_SESSION_TIMEOUT = 20 * 1000;

    /**
     * 会话超时时间
     */
    private int sessionTimeout;

    private String executorName;

    public ZookeeperRegistryCenter(final ZookeeperConfiguration zkConfig) {
        this.zkConfig = zkConfig;
    }

    public ZookeeperConfiguration getZkConfig() {
        return zkConfig;
    }

    @Override
    public String getDirectly(String key) {
        return null;
    }

    @Override
    public List<String> getChildrenKeys(String key) {
        return null;
    }

    @Override
    public void persistEphemeral(String key, String value) {

    }

    @Override
    public void persistEphemeralSequential(String key) {

    }

    @Override
    public long getSessionTimeout() {
        return 0;
    }

    @Override
    public String getNamespace() {
        return null;
    }

    @Override
    public void setExecutorName(String executorName) {

    }

    @Override
    public String getExecutorName() {
        return null;
    }

    @Override
    public void addConnectionStateListener(ConnectionStateListener listener) {

    }

    @Override
    public void removeConnectionStateListener(ConnectionStateListener listener) {

    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public CoordinatorRegistryCenter usingNamespace(String namespace) {
        return null;
    }

    @Override
    public void init() {
        client = buildZkClient();
        client.start();

        try {
            client.getZookeeperClient().blockUntilConnectedOrTimedOut();
            if (!client.getZookeeperClient().isConnected()) {
                throw new RuntimeException("the zk client is not connected while reach connection timeout");
            }

            // check namespace node by using client, for UnknownHostException of connection string.
            client.checkExists().forPath(SLASH_CONSTNAT + zkConfig.getNamespace());
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            throw new RuntimeException("zk connect fail, zkList is " + zkConfig.getServerLists(), ex);
        }
        LOGGER.info("zkClient is created successfully.");
    }

    private CuratorFramework buildZkClient() {
        if(zkConfig.isUseNestedZookeeper()){

        }
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().connectString(zkConfig.getServerLists())
                .retryPolicy(new ExponentialBackoffRetry(zkConfig.getBaseSleepTimeMilliseconds(), zkConfig.getMaxRetries(), zkConfig.getMaxSleepTimeMilliseconds()))
                .namespace(zkConfig.getNamespace());
        if(!Strings.isNullOrEmpty(zkConfig.getDigest())){
            builder.authorization("digest" , zkConfig.getDigest().getBytes(Charsets.UTF_8))
                    .aclProvider(new ACLProvider() {
                        @Override
                        public List<ACL> getDefaultAcl() {
                            return ZooDefs.Ids.CREATOR_ALL_ACL;
                        }

                        @Override
                        public List<ACL> getAclForPath(String s) {
                            return ZooDefs.Ids.CREATOR_ALL_ACL;
                        }
                    });
        }
        return builder.build();

    }

    @Override
    public void close() {

    }

    @Override
    public String get(String key) {
        return null;
    }

    @Override
    public boolean isExisted(String key) {
        return false;
    }

    @Override
    public void persist(String key, String value) {

    }

    @Override
    public void update(String key, String value) {

    }

    @Override
    public void remove(String key) {

    }

    @Override
    public long getRegistryCenterTime(String key) {
        return 0;
    }

    @Override
    public Object getRawClient() {
        return null;
    }

    /**
     * 获取节点路径的状态
     * @param fullPath 节点全路径
     */
    public Stat getStat(String fullPath) {
        try {

            return client.checkExists().forPath(fullPath);
        }catch (final Exception e){
            RegExceptionHandler.handleException(e);
            return null;
        }
    }

    @Override
    public void evictCacheData(String cachePath) {

    }

    @Override
    public Object getRawCache(String cachePath) {
        return null;
    }

    public ZookeeperRegistryCenter(ZookeeperConfiguration zkConfig, CuratorFramework client, int sessionTimeout, String executorName) {
        this.zkConfig = zkConfig;
        this.client = client;
        this.sessionTimeout = sessionTimeout;
        this.executorName = executorName;
    }
}
