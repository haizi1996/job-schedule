package com.hailin.shrine.job.sharding.task;

import com.hailin.shrine.job.sharding.entity.Executor;
import com.hailin.shrine.job.sharding.entity.Shard;
import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import com.hailin.shrine.job.sharding.service.NamespaceShardingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ExecuteAllShardingTask extends AbstractAsyncShardingTask {

    private static final Logger log = LoggerFactory.getLogger(ExecuteAllShardingTask.class);

    public ExecuteAllShardingTask(NamespaceShardingService namespaceShardingService) {
        super(namespaceShardingService);
    }

    @Override
    protected void logStartInfo() {
        log.info("Execute the {} ", this.getClass().getSimpleName());
    }

    @Override
    protected boolean pick(List<String> allJobs, List<String> allEnableJobs, List<Shard> shardList, List<Executor> lastOnlineExecutorList, List<Executor> lastOnlineTrafficExecutorList) throws Exception {

        //修正所有executor对所有的jobNameList
        for (String job : allJobs){
            fixJobNameList(lastOnlineExecutorList , job);
        }
        //获取该域下所有enabled作业的所有分片
        for (String jobName : allEnableJobs){
            shardList.addAll(createShards(jobName , lastOnlineTrafficExecutorList));
        }
        return true;
    }

    @Override
    protected List<Executor> customLastOnlineExecutorList() throws Exception {
        if (!isNodeExisted(ShrineExecutorsNode.getExecutorsNodePath())) {
            return new ArrayList<>();
        }
        // 从$SaturnExecutors节点下，获取所有正在运行的Executor
        List<String> zkExecutors = curatorFramework.getChildren().forPath(ShrineExecutorsNode.getExecutorsNodePath());
        if (zkExecutors == null) {
            return new ArrayList<>();
        }

        List<Executor> lastOnlineExecutorList = new ArrayList<>();
        for (int i = 0; i < zkExecutors.size(); i++) {
            String zkExecutor = zkExecutors.get(i);
            if (isNodeExisted(ShrineExecutorsNode.getExecutorIpNodePath(zkExecutor))) {
                byte[] ipData = curatorFramework.getData()
                        .forPath(ShrineExecutorsNode.getExecutorIpNodePath(zkExecutor));
                if (ipData != null) {
                    Executor executor = new Executor();
                    executor.setExecutorName(zkExecutor);
                    executor.setIp(new String(ipData, StandardCharsets.UTF_8));
                    executor.setNoTraffic(getExecutorNoTraffic(zkExecutor));
                    executor.setShardList(new ArrayList<Shard>());
                    executor.setJobNameList(new ArrayList<String>());
                    lastOnlineExecutorList.add(executor);
                }
            }
        }
        return lastOnlineExecutorList;
    }

    private boolean isNodeExisted(String executorsNodePath) throws Exception {
        return curatorFramework.checkExists().forPath(executorsNodePath) != null;
    }
}
