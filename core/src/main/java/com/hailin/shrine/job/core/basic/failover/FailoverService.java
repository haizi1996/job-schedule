package com.hailin.shrine.job.core.basic.failover;

import com.google.common.collect.Lists;
import com.hailin.shrine.job.core.basic.AbstractShrineService;
import com.hailin.shrine.job.core.basic.execution.ExecutionNode;
import com.hailin.shrine.job.core.basic.storage.JobNodePath;
import com.hailin.shrine.job.core.basic.storage.LeaderExecutionCallback;
import com.hailin.shrine.job.core.job.constant.ConfigurationNode;
import com.hailin.shrine.job.core.strategy.JobScheduler;
import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 作业失效转移服务
 * @author zhanghailin
 */
public class FailoverService extends AbstractShrineService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailoverService.class);

    public FailoverService(JobScheduler jobScheduler) {
        super(jobScheduler);
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void shutdown()  {
        super.shutdown();
    }

    /**
     * 设置失效的分片项标记
     * @param item 崩溃的作业项
     */
    public void createCrashedFailoverFlag(final int item){
        if (!isFailoverAssigned(item)){
            try {
                getJobNodeStorage().getClient().create()
                        .creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                        .forPath(JobNodePath.getNodeFullPath(jobName , FailoverNode.getItemsNode(item)));
                LOGGER.info(jobName, "{} - {} create failover flag of item {}", executorName, jobName, item);
            } catch (KeeperException.NodeExistsException e) { // NOSONAR
                LOGGER.debug( jobName,
                        "{} - {} create failover flag of item {} failed, because it is already existing", executorName,
                        jobName, item);
            } catch (Exception e) {
                LOGGER.error(jobName, e.getMessage(), e);
            }
        }
    }

    /**
     * 判断失效的作业片节点是否存在
     * @param item 崩溃的作业分片项
     */
    private boolean isFailoverAssigned(int item) {
        return getJobNodeStorage().isJobNodeExisted(FailoverNode.getExecutionFailoverNode(item));
    }

    /**
     * 是否需要进行失效转移
     * 作业存在崩溃的分片项，作业配置了允许进行失效转移
     */
    private boolean needFailover(){
        return getJobNodeStorage().isJobNodeExisted(FailoverNode.ITEMS_ROOT)
                && !getJobNodeStorage().getJobNodeChildrenKeys(FailoverNode.ITEMS_ROOT).isEmpty()
                && getJobNodeStorage().isJobNodeExisted(ConfigurationNode.ENABLED)
                &&Boolean.parseBoolean(getJobNodeStorage().getJobNodeData(ConfigurationNode.ENABLED));
    }

    /**
     * 更新执行完毕失效转移的分片项状态
     * @param item 执行完毕失效转移的分片项列表
     */
    public void updateFailoverComplete(final Integer item){
        getJobNodeStorage().removeJobNodeIfExisted(FailoverNode.getExecutionFailoverNode(item));
    }


    /**
     * 作业进行失效作业分片项转移时，执行回调接口
     * @author zhanghailin
     */
    class FailoverLeaderExecutionCallback implements LeaderExecutionCallback{
        @Override
        public void execute() {
            if (!needFailover() || jobScheduler == null
                    || coordinatorRegistryCenter.isExisted(ShrineExecutorsNode.getExecutorNoTrafficNodePath(executorName))){
                return;
            }
            if (!jobScheduler.getConfigService().getPreferList().contains(executorName)
            && !jobScheduler.getConfigService().isUseDispreferList()){
                return;
            }
            List<String> items = getJobNodeStorage().getJobNodeChildrenKeys(FailoverNode.ITEMS_ROOT);

            if (CollectionUtils.isEmpty(items)){
                return;
            }

            //获取第1个分片项
            int crashedItem = Integer.parseInt(getJobNodeStorage().
                    getJobNodeChildrenKeys(FailoverNode.ITEMS_ROOT).get(0));
            LOGGER.debug( jobName, "Elastic job: failover job begin, crashed item:{}.", crashedItem);
            //将此分片划分到自己的分片目录下
            getJobNodeStorage()
                    .fillEphemeralJobNode(FailoverNode.getExecutionFailoverNode(crashedItem), executorName);
            //从崩溃分片目录下移除此分片项
            getJobNodeStorage().removeJobNodeIfExisted(FailoverNode.getItemsNode(crashedItem));
             //立即出发此任务
            jobScheduler.triggerJob(null);
        }
    }

    /**
     * 获取运行在本作业服务器的失效转移序列号.
     *
     * @return 运行在本作业服务器的失效转移序列号
     */
    public List<Integer> getLocalHostFailoverItems() {
        List<String> items = getJobNodeStorage().getJobNodeChildrenKeys(ExecutionNode.ROOT);
        List<Integer> result = new ArrayList<>(items.size());
        for (String each : items) {
            int item = Integer.parseInt(each);
            String node = FailoverNode.getExecutionFailoverNode(item);
            if (getJobNodeStorage().isJobNodeExisted(node) && executorName
                    .equals(getJobNodeStorage().getJobNodeDataDirectly(node))) {
                result.add(item);
            }
        }
        Collections.sort(result);
        return result;
    }
    /**
     * 获取运行在本作业服务器的被失效转移的序列号.
     *
     * @return 运行在本作业服务器的被失效转移的序列号
     */
    public List<Integer> getLocalHostTakeOffItems(){
        List<Integer> shardingItems = jobScheduler.getShardingService().getLocalHostShardingItems();
        List<Integer> result = Lists.newArrayListWithCapacity(shardingItems.size());
        for (int each : shardingItems) {
            if (getJobNodeStorage().isJobNodeExisted(FailoverNode.getExecutionFailoverNode(each))) {
                result.add(each);
            }
        }
        return result;
    }

    /**
     * 删除作业失效转移信息
     */
    public void removeFailoverInfo(){
        getJobNodeStorage().removeJobNodeIfExisted(FailoverNode.ITEMS_ROOT);

        for (String each : getJobNodeStorage().getJobNodeChildrenKeys(ExecutionNode.ROOT)){
            getJobNodeStorage().removeJobNodeIfExisted(FailoverNode.getExecutionFailoverNode(Integer.parseInt(each)));
        }

    }


    class FailoverTimeoutLeaderExecutionCallback implements LeaderExecutionCallback{
        @Override
        public void execute() {
            LOGGER.warn( jobName, "Failover leader election timeout with a minute");

        }
    }


}
