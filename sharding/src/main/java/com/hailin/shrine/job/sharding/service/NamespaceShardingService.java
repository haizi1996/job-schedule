package com.hailin.shrine.job.sharding.service;

import com.google.common.collect.Lists;
import com.hailin.shrine.job.integrate.service.ReportAlarmService;
import com.hailin.shrine.job.sharding.entity.Executor;
import com.hailin.shrine.job.sharding.entity.Shard;
import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import com.hailin.shrine.job.sharding.task.*;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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

    public void shutdown() throws InterruptedException {
        shutdownInner(true);
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

    /**
     * 选举
     */
    public void leaderElection() throws Exception {
        lock.lockInterruptibly();
        try {
            if (hasLeadership()) {
                return;
            }
            LOGGER.info("{}-{} leadership election start", namespace, hostValue);
            try (LeaderLatch leaderLatch = new LeaderLatch(curatorFramework,
                    ShrineExecutorsNode.LEADER_LATCHNODE_PATH)) {
                leaderLatch.start();
                int timeoutSeconds = 60;
                if (leaderLatch.await(timeoutSeconds, TimeUnit.SECONDS)) {
                    if (!hasLeadership()) {
                        becomeLeader();
                    } else {
                        LOGGER.info("{}-{} becomes a follower", namespace, hostValue);
                    }
                } else {
                    LOGGER.error("{}-{} leadership election is timeout({}s)", namespace, hostValue, timeoutSeconds);
                }
            } catch (InterruptedException e) {
                LOGGER.info("{}-{} leadership election is interrupted", namespace, hostValue);
                throw e;
            } catch (Exception e) {
                LOGGER.error(namespace + "-" + hostValue + " leadership election error", e);
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    private void becomeLeader() throws Exception {
        //清理，重置变量
        executorService.shutdownNow();

        while (!executorService.isTerminated()){ // 等待任务全部退出
            Thread.sleep(100L);
            executorService.shutdownNow();
        }
        needAllSharding.set(false);
        shardingCount.set(0);
        executorService = newSingleThreadExecutor();

        //持久化$Jobs节点
        if (curatorFramework.checkExists().forPath(ShrineExecutorsNode.JOBSNODE_PATH) == null){
            curatorFramework.create().creatingParentsIfNeeded()
                    .forPath(ShrineExecutorsNode.JOBSNODE_PATH);
        }
        //持久化LeaderValue
        curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                .forPath(ShrineExecutorsNode.LEADER_HOSTNODE_PATH, hostValue.getBytes(StandardCharsets.UTF_8));

        //提交全量分片线程
        needAllSharding.set(true);

        shardingCount.incrementAndGet();
        executorService.submit(new ExecuteAllShardingTask(this));
        LOGGER.info("{}-{} become leader", namespace, hostValue);

    }


    /**
     * 结点上线处理
     */
    public void asyncShardingWhenExecutorOnline(String executorName, String ip) throws Exception {
        if (isLeadership()) {
            shardingCount.incrementAndGet();
            executorService.submit(new ExecuteOnlineShardingTask(this, executorName, ip));
        }
    }
    /**
     * 结点掉线处理
     */
    public void asyncShardingWhenExecutorOffline(String executorName) throws Exception {
        if (isLeadership()) {
            shardingCount.incrementAndGet();
            executorService.submit(new ExecuteOfflineShardingTask(this, executorName));
        }
    }

    /**
     * 摘取流量
     */
    public void asyncShardingWhenExtractExecutorTraffic(String executorName) throws Exception {
        if (isLeadership()) {
            shardingCount.incrementAndGet();
            executorService.submit(new ExecuteExtractTrafficShardingTask(this, executorName));
        }
    }

    /**
     * 恢复流量
     */
    public void asyncShardingWhenRecoverExecutorTraffic(String executorName) throws Exception {
        if (isLeadership()) {
            shardingCount.incrementAndGet();
            executorService.submit(new ExecuteRecoverTrafficShardingTask(this, executorName));
        }
    }

    private boolean isLeadership() throws Exception {
        while (!hasLeadership()) {
            leaderElection();
        }
        return new String(curatorFramework.getData().forPath(ShrineExecutorsNode.LEADER_HOSTNODE_PATH), "UTF-8")
                .equals(hostValue);
    }

    /**
     * 作业启用事件
     */
    public void asyncShardingWhenJobEnable(String jobName) throws Exception {
        if (isLeadership()) {
            shardingCount.incrementAndGet();
            executorService.submit(new ExecuteJobEnableShardingTask(this, jobName));
        }
    }
    /**
     * 进行全量分片
     */
    public void asyncShardingWhenExecutorAll() throws Exception {
        if (isLeadership()) {
            needAllSharding.set(true);
            shardingCount.incrementAndGet();
            executorService.submit(new ExecuteAllShardingTask(this));
            String shardAllAtOnce = ShrineExecutorsNode.getExecutorShardingNodePath("shardAllAtOnce");
            if (curatorFramework.checkExists().forPath(shardAllAtOnce) != null) {
                curatorFramework.delete().deletingChildrenIfNeeded().forPath(shardAllAtOnce);
            }
        }
    }
    /**
     * 处理作业禁用事件
     */
    public void asyncShardingWhenJobDisable(String jobName) throws Exception {
        if (isLeadership()) {
            shardingCount.incrementAndGet();
            executorService.submit(new ExecuteJobDisableShardingTask(this, jobName));
        }
    }

    /**
     * 处理作业全排
     */
    public void asyncShardingWhenJobForceShard(String jobName) throws Exception {
        if (isLeadership()) {
            shardingCount.incrementAndGet();
            executorService.submit(new ExecuteJobForceShardShardingTask(this, jobName));
        }
    }

    /**
     * 处理作业executor上线
     */
    public void asyncShardingWhenJobServerOnline(String jobName, String executorName) throws Exception {
        if (isLeadership()) {
            shardingCount.incrementAndGet();
            executorService.submit(new ExecuteJobServerOnlineShardingTask(this, jobName, executorName));
        }
    }

    /**
     * 处理作业executor下线
     */
    public void asyncShardingWhenJobServerOffline(String jobName, String executorName) throws Exception {
        if (isLeadership()) {
            shardingCount.incrementAndGet();
            executorService.submit(new ExecuteJobServerOfflineShardingTask(this, jobName, executorName));
        }
    }

    public List<Shard> removeAllShardsOnExecutors(List<Executor> lastOnlineTrafficExecutorList, String jobName) {
        List<Shard> removedShards = Lists.newArrayList();

        for (int i = 0; i < lastOnlineTrafficExecutorList.size(); i++) {
            Executor executor = lastOnlineTrafficExecutorList.get(i);
            Iterator<Shard> iterator = executor.getShardList().iterator();
            while (iterator.hasNext()) {
                Shard shard = iterator.next();
                if (jobName.equals(shard.getJobName())) {
                    executor.setTotalLoadLevel(executor.getTotalLoadLevel() - shard.getLoadLevel());
                    iterator.remove();
                    removedShards.add(shard);
                }
            }
        }

        return removedShards;
    }
}
