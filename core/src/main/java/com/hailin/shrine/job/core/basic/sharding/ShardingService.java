package com.hailin.shrine.job.core.basic.sharding;

import com.hailin.shrine.job.common.exception.JobShuttingDownException;
import com.hailin.shrine.job.common.util.BlockUtils;
import com.hailin.shrine.job.common.util.ItemUtils;
import com.hailin.shrine.job.core.basic.AbstractShrineService;
import com.hailin.shrine.job.core.basic.election.LeaderElectionService;
import com.hailin.shrine.job.core.basic.execution.ExecutionService;
import com.hailin.shrine.job.core.basic.server.ServerService;
import com.hailin.shrine.job.core.basic.storage.JobNodePath;
import com.hailin.shrine.job.core.strategy.JobScheduler;
import com.hailin.shrine.job.sharding.service.NamespaceShardingContentService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * 作业分片服务
 * @author zhanghailin
 */
public class ShardingService extends AbstractShrineService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShardingService.class);

    public  static final String SHARDING_UN_NECESSARY = "0";

    private LeaderElectionService leaderElectionService;

    private ServerService serverService ;

    private ExecutionService executionService;

    private NamespaceShardingContentService namespaceShardingContentService;

    private volatile boolean isShutdown;
    private CuratorWatcher necessaryWatcher;


    public ShardingService(JobScheduler jobScheduler) {
        super(jobScheduler);
    }

    @Override
    public synchronized void start() {
        leaderElectionService = jobScheduler.getLeaderElectionService();
        serverService = jobScheduler.getServerService();
        executionService = jobScheduler.getExecutionService();
        namespaceShardingContentService = new NamespaceShardingContentService(
                (CuratorFramework) coordinatorRegistryCenter.getRawClient());
    }

    /**
     * 判断是否需要重分片.
     * necessar节点的内容来自于AbstractAsyncShardingTask::run()
     * @return 是否需要重分片
     */
    public boolean isNeedSharding() {
        return getJobNodeStorage().isJobNodeExisted(ShardingNode.NECESSARY) && !SHARDING_UN_NECESSARY
                .equals(getJobNodeStorage().getJobNodeDataDirectly(ShardingNode.NECESSARY));
    }

    public void registerNecessaryWatcher(CuratorWatcher necessaryWatcher) {
        this.necessaryWatcher = necessaryWatcher;
        registerNecessaryWatcher();
    }

    public void registerNecessaryWatcher() {
        try {
            if (necessaryWatcher != null) {
                getJobNodeStorage().getClient().checkExists().usingWatcher(necessaryWatcher)
                        .forPath(JobNodePath.getNodeFullPath(jobName, ShardingNode.NECESSARY));
            }
        } catch (Exception e) {
            LOGGER.error( jobName, e.getMessage(), e);
        }
    }

    private GetDataStat getNecessaryDataStat() {
        String data = null;
        int version = -1;
        try {
            Stat stat = new Stat();
            byte[] bs = null;
            if (necessaryWatcher != null) {
                bs = getJobNodeStorage().getClient().getData().storingStatIn(stat).usingWatcher(necessaryWatcher)
                        .forPath(JobNodePath.getNodeFullPath(jobName, ShardingNode.NECESSARY));
            } else {
                bs = getJobNodeStorage().getClient().getData().storingStatIn(stat)
                        .forPath(JobNodePath.getNodeFullPath(jobName, ShardingNode.NECESSARY));
            }
            if (bs != null) {
                data = new String(bs, "UTF-8");
            }
            version = stat.getVersion();
        } catch (Exception e) {
            LOGGER.error( jobName, e.getMessage(), e);
        }
        return new GetDataStat(data, version);
    }

    /**
     * 如果需要分片且当前节点为主节点, 则作业分片.
     */
    public synchronized void shardingIfNecessary() throws JobShuttingDownException {
        if (isShutdown) {
            return;
        }
        GetDataStat getDataStat = null;
        if (getJobNodeStorage().isJobNodeExisted(ShardingNode.NECESSARY)) {
            getDataStat = getNecessaryDataStat();
        }
        // sharding necessary内容为空，或者内容是"0"则返回，否则，需要进行sharding处理
        if (getDataStat == null || SHARDING_UN_NECESSARY.equals(getDataStat.getData())) {
            return;
        }
        // 如果不是leader，则等待leader处理完成（这也是一个死循环，知道满足跳出循环的条件：1. 被shutdown 2. 无须sharding而且不处于processing状态）
        if (blockUntilShardingComplatedIfNotLeader()) {
            return;
        }
        // 如果有作业分片处于running状态则等待（无限期）
        waitingOtherJobCompleted();
        // 建立一个临时节点，标记sharding处理中
        getJobNodeStorage().fillEphemeralJobNode(ShardingNode.PROCESSING, "");
        try {
            // 删除作业下面的所有JobServer的sharding节点
            clearShardingInfo();

            int maxRetryTime = 3;
            int retryCount = 0;
            while (!isShutdown) {
                int version = getDataStat.getVersion();
                // 首先尝试从job/leader/sharding/necessary节点获取，如果失败，会从$SaturnExecutors/sharding/content下面获取
                // key is executor, value is sharding items
                Map<String, List<Integer>> shardingItems = namespaceShardingContentService
                        .getShardingContent(jobName, getDataStat.getData());
                try {
                    // 所有jobserver的（检查+创建），加上设置sharding necessary内容为0，都是一个事务
                    CuratorTransactionFinal curatorTransactionFinal = getJobNodeStorage().getClient().inTransaction()
                            .check().forPath("/").and();
                    for (Map.Entry<String, List<Integer>> entry : shardingItems.entrySet()) {
                        curatorTransactionFinal.create().forPath(
                                JobNodePath.getNodeFullPath(jobName, ShardingNode.getShardingNode(entry.getKey())),
                                ItemUtils.toItemsString(entry.getValue()).getBytes(StandardCharsets.UTF_8)).and();
                    }
                    curatorTransactionFinal.setData().withVersion(version)
                            .forPath(JobNodePath.getNodeFullPath(jobName, ShardingNode.NECESSARY),
                                    SHARDING_UN_NECESSARY.getBytes(StandardCharsets.UTF_8)).and();
                    curatorTransactionFinal.commit();
                    break;
                } catch (KeeperException.BadVersionException e) {
                    LOGGER.warn( jobName, "zookeeper bad version exception happens", e);
                    if (++retryCount <= maxRetryTime) {
                        LOGGER.info( jobName,
                                "bad version because of concurrency, will retry to get shards from sharding/necessary later");
                        Thread.sleep(200L); // NOSONAR
                        getDataStat = getNecessaryDataStat();
                    }
                } catch (Exception e) {
                    LOGGER.warn( jobName, "commit shards failed", e);
                    /**
                     * 已知场景：
                     *   异常为NoNodeException，域下作业数量大，业务容器上下线。
                     *   原因是，大量的sharding task导致计算结果有滞后，同时某些server被删除，导致commit失败，报NoNode异常。
                     *
                     * 是否需要重试：
                     *   如果作业一直处于启用状态，necessary最终会被更新正确，这时不需要主动重试。 如果重试，可能导致提前拿到数据，后面再重新拿一次数据，不过也没多大问题。
                     *   如果作业在中途禁用了，那么necessary将不会被更新，这时从necessary拿到的数据是过时的，仍然会commit失败，这时需要从content获取数据来重试。
                     *   如果是其他未知场景导致的commit失败，也是可以尝试从content获取数据来重试。
                     *   所以，为了保险起见，均从content获取数据来重试。
                     */
                    if (++retryCount <= maxRetryTime) {
                        LOGGER.info( jobName,
                                "unexpected error, will retry to get shards from sharding/content later");
                        // 睡一下，没必要马上重试。减少对zk的压力。
                        Thread.sleep(200L); // NOSONAR
                        /**
                         * 注意：
                         *   data为x，是为了使得反序列化失败，然后从sharding/content下获取数据。
                         *   version使用necessary的version。
                         */
                        getDataStat = new GetDataStat("x", version);
                    }
                }
                if (retryCount > maxRetryTime) {
                    LOGGER.warn( jobName, "retry time exceed {}, will give up to get shards", maxRetryTime);
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.error( jobName, e.getMessage(), e);
        } finally {
            getJobNodeStorage().removeJobNodeIfExisted(ShardingNode.PROCESSING);
        }
    }

    /**
     * <p>如果不是leader，等待leader分片完成，返回true；如果期间变为leader，返回false。
     * <p>
     *     TODO：如果业务运行时间很快，以至于在followers waitingShortTime的100ms期间，其他分片已经运行完，并且leader crash并当前follower变为leader了。
     *     		这种情况，如果返回false，则当前线程作为leader获取分片完，会再次跑业务。 这可能不被期望。
     *     		所以，最好是能有全局的作业级别轮次锁定。
     * @return true or false
     * @throws JobShuttingDownException
     */
    private boolean blockUntilShardingComplatedIfNotLeader() throws JobShuttingDownException {
        for (; ; ) {
            if (isShutdown) {
                throw new JobShuttingDownException();
            }
            if (leaderElectionService.isLeader()) {
                return false;
            }
            if (!(isNeedSharding() || getJobNodeStorage().isJobNodeExisted(ShardingNode.PROCESSING))) {
                return true;
            }
            LOGGER.debug( jobName, "Sleep short time until sharding completed");
            BlockUtils.waitingShortTime();
        }
    }

    private void waitingOtherJobCompleted() {
        while (!isShutdown && executionService.hasRunningItems()) {
            LOGGER.info( jobName, "Sleep short time until other job completed.");
            BlockUtils.waitingShortTime();
        }
    }

    private void clearShardingInfo() {
        for (String each : serverService.getAllServers()) {
            getJobNodeStorage().removeJobNodeIfExisted(ShardingNode.getShardingNode(each));
        }
    }


    /**
     * 获取运行在本作业服务器的分片序列号.
     *
     * @return 运行在本作业服务器的分片序列号
     */
    public List<Integer> getLocalHostShardingItems() {
        String value = getJobNodeStorage().getJobNodeDataDirectly(ShardingNode.getShardingNode(executorName));
        return ItemUtils.toItemList(value);
    }
    @Override
    public void shutdown() {
        isShutdown = true;
        necessaryWatcher = null; // cannot registerNecessaryWatcher
    }

    public Object getLocalShardingItems() {
    }
}
