package com.hailin.shrine.job.sharding.service;

import com.hailin.shrine.job.integrate.service.ReportAlarmService;
import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Setter
public class NamespaceShardingService {
    private static final Logger LOGGER = LoggerFactory.getLogger(NamespaceShardingService.class);

    public static final boolean CONTAINER_ALIGN_WITH_PHYSICAL;

    private static final String NAME_IS_CONTAINER_ALIGN_WITH_PHYSICAL = "SHRINE_CONTAINER_ALIGN_WITH_PHYSICAL";

    private String namespace;

    private String hostValue;

    private CuratorFramework curatorFramework;

    private AtomicInteger shardingCount;

    private AtomicBoolean needAllSharding;

    private ExecutorService executorService;

    private NamespaceShardingContentService namespaceShardingContentService;

    private ReportAlarmService reportAlarmService;

    private ReentrantLock lock;

    static {
        String isContainerAlignWithPhysicalStr = System.getProperty(NAME_IS_CONTAINER_ALIGN_WITH_PHYSICAL,
                System.getenv(NAME_IS_CONTAINER_ALIGN_WITH_PHYSICAL));

        CONTAINER_ALIGN_WITH_PHYSICAL = StringUtils.isBlank(isContainerAlignWithPhysicalStr)
                || Boolean.parseBoolean(isContainerAlignWithPhysicalStr);
    }
    public NamespaceShardingService(CuratorFramework curatorFramework, String hostValue ,ReportAlarmService reportAlarmService) {
        this.curatorFramework = curatorFramework;
        this.hostValue = hostValue;
        this.shardingCount = new AtomicInteger(0);
        this.needAllSharding = new AtomicBoolean(false);
        this.executorService = newSingleThreadExecutor();
        this.namespace = curatorFramework.getNamespace();
        this.namespaceShardingContentService = new NamespaceShardingContentService(curatorFramework);
        this.reportAlarmService = reportAlarmService;
        this.lock = new ReentrantLock();
    }

    private ExecutorService newSingleThreadExecutor() {
        return Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, namespace + "-" + r.getClass().getSimpleName());
            }
        });
    }

    private boolean hasLeadership() throws Exception {
        return curatorFramework.checkExists().forPath(ShrineExecutorsNode.LEADER_HOSTNODE_PATH) != null;
    }
    public boolean isLeadershipOnly() throws Exception {
        if (hasLeadership()) {
            return new String(curatorFramework.getData().forPath(ShrineExecutorsNode.LEADER_HOSTNODE_PATH), StandardCharsets.UTF_8)
                    .equals(hostValue);
        } else {
            return false;
        }
    }

    public boolean isNeedAllSharding() {
        return needAllSharding.get();
    }

    public void setNeedAllSharding(boolean needAllSharding) {
        this.needAllSharding.set(needAllSharding);
    }
    public int shardingCountDecrementAndGet() {
        return shardingCount.decrementAndGet();
    }

    public int shardingCountIncrementAndGet() {
        return shardingCount.incrementAndGet();
    }

    /**
     * 关掉线程池，
     *
     */
    public void shutdownInner(boolean shutdownNow) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            if (executorService != null) {
                if (shutdownNow) {
                    executorService.shutdownNow();
                } else {
                    executorService.shutdown();
                }
            }
            try {
                if (curatorFramework.getZookeeperClient().isConnected()) {
                    releaseMyLeadership();
                }
            } catch (Exception e) {
                LOGGER.error(namespace + "-" + hostValue + " delete leadership error", e);
            }
        } finally {
            lock.unlock();
        }
    }
    private void releaseMyLeadership() throws Exception {
        if (isLeadershipOnly()) {
            curatorFramework.delete().forPath(ShrineExecutorsNode.LEADER_HOSTNODE_PATH);
        }
    }
}
