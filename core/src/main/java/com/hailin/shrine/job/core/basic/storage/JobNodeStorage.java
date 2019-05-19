package com.hailin.shrine.job.core.basic.storage;

import com.hailin.shrine.job.common.exception.JobException;
import com.hailin.shrine.job.common.util.BlockUtils;
import com.hailin.shrine.job.core.basic.server.ServerNode;
import com.hailin.shrine.job.core.job.config.JobConfiguration;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.shrine.job.core.reg.exception.RegExceptionHandler;
import com.hailin.shrine.job.core.reg.zookeeper.ZookeeperConfiguration;
import com.hailin.shrine.job.core.reg.zookeeper.ZookeeperRegistryCenter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 作业节点数据访问类
 * @author zhanghailin
 */
public class JobNodeStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobNodeStorage.class);

    //最大删除重试次数
    private static final int MAX_DELETE_RETRY_TIMES = 10;

    private final CoordinatorRegistryCenter registryCenter ;

    private final JobConfiguration jobConfiguration;

    private String executorName;

    private final String jobName;


    public JobNodeStorage(CoordinatorRegistryCenter registryCenter, JobConfiguration jobConfiguration) {

        this.registryCenter = registryCenter;
        this.jobConfiguration = jobConfiguration;
        this.jobName = jobConfiguration.getJobName();
        if (registryCenter != null){
            executorName = registryCenter.getExecutorName();
        }
    }

    /**
     * 判断作业节点是否存在
     * @param node 作业节点名称
     * @return 作业节点是否存在
     */
    public boolean isJobNodeExisted(final String node){
        return registryCenter.isExisted(JobNodePath.getNodeFullPath(jobConfiguration.getJobName() , node));
    }

    /**
     * 判断作业是否存在
     * @param jobName 作业节点名称
     * @return 作业是否存在
     */
    public boolean isJobExisted(final String jobName){
        return registryCenter.isExisted(JobNodePath.getJobNameFullPath(jobName));
    }

    /**
     * 获取作业节点数据
     * @param node 作业节点名称
     * @return 作业节点数据值
     */
    public String getJobNodeData(final String node){
        return registryCenter.get(JobNodePath.getNodeFullPath(jobConfiguration.getJobName() , node));
    }
    /**
     * 直接从注册中心而非本地缓存获取作业节点数据.
     *
     * @param node 作业节点名称
     * @return 作业节点数据值
     */
    public String getJobNodeDataDirectly(final String node) {
        return registryCenter.getDirectly(JobNodePath.getNodeFullPath(jobConfiguration.getJobName(), node));
    }

    /**
     * 直接从注册中心而非本地缓存获取作业节点数据.可用于相同namespace下的其他作业。
     *
     * @param jobName 作业名
     * @param node 作业节点名称
     * @return 作业节点数据值
     */
    public String getJobNodeDataDirectly(String jobName, final String node) {
        return registryCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, node));
    }

    /**
     * 获取作业节点子节点名称列表.
     *
     * @param node 作业节点名称
     * @return 作业节点子节点名称列表
     */
    public List<String> getJobNodeChildrenKeys(final String node) {
        return registryCenter
                .getChildrenKeys(JobNodePath.getNodeFullPath(jobConfiguration.getJobName(), node));
    }

    /**
     * 如果不存在则创建作业节点.
     *
     * @param node 作业节点名称
     */
    public void createJobNodeIfNeeded(final String node) {
        registryCenter.persist(JobNodePath.getNodeFullPath(jobConfiguration.getJobName(), node), "");
    }

    public JobConfiguration getJobConfiguration() {
        return jobConfiguration;
    }

    public void createOrUpdateJobNodeWithValue(final String node, final String value) {
        registryCenter.persist(JobNodePath.getNodeFullPath(jobConfiguration.getJobName(), node), value);
    }

    /**
     * 如果节点存在，则删除作业节点
     *
     * @param node 作业节点名称
     */
    public void removeJobNodeIfExisted(final String node) {
        if (isJobNodeExisted(node)) {
            registryCenter.remove(JobNodePath.getNodeFullPath(jobConfiguration.getJobName(), node));
        }
    }

    /**
     * 删除作业节点
     *
     * @param node 作业节点名称
     */
    public void removeJobNode(final String node) {
        registryCenter.remove(JobNodePath.getNodeFullPath(jobConfiguration.getJobName(), node));
    }


    /**
     * 如果节点不存在或允许覆盖则填充节点数据.
     *
     * @param node 作业节点名称
     * @param value 作业节点数据值
     */
    public void fillJobNodeIfNullOrOverwrite(final String node, final Object value) {
        if (null == value) {
            LOGGER.info(jobName, "job node value is null, node:{}", node);
            return;
        }
        if (!isJobNodeExisted(node) || (!value.toString().equals(getJobNodeDataDirectly(node)))) {
            registryCenter
                    .persist(JobNodePath.getNodeFullPath(jobConfiguration.getJobName(), node), value.toString());
        }
    }

    /**
     * 填充临时节点数据.
     *
     * @param node 作业节点名称
     * @param value 作业节点数据值
     */
    public void fillEphemeralJobNode(final String node, final Object value) {
        registryCenter
                .persistEphemeral(JobNodePath.getNodeFullPath(jobConfiguration.getJobName(), node), value.toString());
    }

    /**
     * 更新节点数据.
     *
     * @param node 作业节点名称
     * @param value 作业节点数据值
     */
    public void updateJobNode(final String node, final Object value) {
        registryCenter
                .update(JobNodePath.getNodeFullPath(jobConfiguration.getJobName(), node), value.toString());
    }

    /**
     * 跟新作业节点数据。可用于同一个namespace下的其他作业。
     *
     * @param jobName 作业名
     * @param node 作业节点名称
     * @param value 待替换的数据
     */
    public void updateJobNode(final String jobName, final String node, final Object value) {
        registryCenter.update(JobNodePath.getNodeFullPath(jobName, node), value.toString());
    }

    /**
     * 替换作业节点数据.
     *
     * @param node 作业节点名称
     * @param value 待替换的数据
     */
    public void replaceJobNode(final String node, final Object value) {
        registryCenter
                .persist(JobNodePath.getNodeFullPath(jobConfiguration.getJobName(), node), value.toString());
    }

    /**
     * 替换作业节点数据.
     *
     * @param jobName 作业名
     * @param node 作业节点名称
     * @param value 待替换的数据
     */
    public void replaceJobNode(final String jobName, final String node, final Object value) {
        registryCenter.persist(JobNodePath.getNodeFullPath(jobName, node), value.toString());
    }

    /**
     * 在事务中执行操作.
     *
     * @param callback 执行操作的回调
     */
    public void executeInTransaction(final TransactionExecutionCallback callback) {
        try {
            CuratorTransactionFinal curatorTransactionFinal = getClient().inTransaction().check().forPath("/").and();
            callback.execute(curatorTransactionFinal);
            curatorTransactionFinal.commit();
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }

    /**
     * 在主节点执行操作.
     *
     * @param latchNode 分布式锁使用的作业节点名称
     * @param callback 执行操作的回调
     */
    public void executeInLeader(final String latchNode, final LeaderExecutionCallback callback) {
        try (LeaderLatch latch = new LeaderLatch(getClient(),
                JobNodePath.getNodeFullPath(jobConfiguration.getJobName(), latchNode))) {
            latch.start();
            latch.await();
            callback.execute();
            // CHECKSTYLE:OFF
        } catch (final Exception e) {
            LOGGER.error( jobName, e.getMessage(), e);
            // CHECKSTYLE:ON
            if (e instanceof InterruptedException) {// NOSONAR
                Thread.currentThread().interrupt();
            } else {
                throw new JobException(e);
            }
        }
    }

    public void executeInLeader(final String latchNode, final LeaderExecutionCallback callback, final long timeout,
                                final TimeUnit unit, final LeaderExecutionCallback timeoutCallback) {
        try (LeaderLatch latch = new LeaderLatch(getClient(),
                JobNodePath.getNodeFullPath(jobConfiguration.getJobName(), latchNode))) {
            latch.start();
            if (latch.await(timeout, unit)) {
                callback.execute();
            } else {
                if (timeoutCallback != null) {
                    timeoutCallback.execute();
                }
            }
            // CHECKSTYLE:OFF
        } catch (final Exception e) {
            LOGGER.error( jobName, e.getMessage(), e);
            // CHECKSTYLE:ON
            if (e instanceof InterruptedException) {// NOSONAR
                Thread.currentThread().interrupt();
            } else {
                throw new JobException(e);
            }
        }
    }

    public CuratorFramework getClient() {
        return (CuratorFramework) registryCenter.getRawClient();
    }

    /**
     * 获取当前运行execution分片列表
     * @return 当前运行execution分片列表
     */
    public List<String> getRunningItems() {
        return registryCenter
                .getChildrenKeys(JobNodePath.getNodeFullPath(jobConfiguration.getJobName(), "execution"));
    }

    /**
     * 删除ZK结点
     */
    public void deleteJobNode() {
        ZookeeperConfiguration zkConfig = ((ZookeeperRegistryCenter) registryCenter).getZkConfig();
        ZookeeperRegistryCenter newZk = new ZookeeperRegistryCenter(zkConfig);
        try {
            newZk.init(); // maybe throw RuntimeException
            newZk.remove(ServerNode.getServerNode(jobName, executorName));

            String fullPath = JobNodePath.getJobNameFullPath(jobConfiguration.getJobName());
            Stat stat = newZk.getStat(fullPath);
            if (stat == null) {
                return;
            }
            long ctime = stat.getCtime();
            for (int i = 0; i < MAX_DELETE_RETRY_TIMES; i++) {
                stat = newZk.getStat(fullPath);
                if (stat == null) {
                    return;
                }
                if (stat.getCtime() != ctime) {
                    LOGGER.info( jobName, "the job node ctime has changed, give up to delete");
                    return;
                }
                List<String> servers = newZk.getChildrenKeys(ServerNode.getServerRoot(jobName));
                if (servers == null || servers.isEmpty()) {
                    if (tryToRemoveNode(newZk, fullPath)) {
                        return;
                    }
                }
                BlockUtils.waitingShortTime();
            }
        } catch (Throwable t) {
            LOGGER.error( jobName, "delete job node error", t);
        } finally {
            newZk.close();
        }
    }

    private boolean tryToRemoveNode(ZookeeperRegistryCenter newZk, String fullPath) {
        try {
            newZk.remove(fullPath);
            return true;
        } catch (Exception e) {
            LOGGER.error( jobName, e.getMessage(), e);
        }
        return false;
    }

    public boolean isConnected() {
        return registryCenter.isConnected();
    }
}
