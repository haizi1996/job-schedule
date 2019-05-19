package com.hailin.shrine.job.sharding.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hailin.shrine.job.sharding.entity.Executor;
import com.hailin.shrine.job.sharding.entity.Shard;
import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorMultiTransaction;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NamespaceShardingContentService {

    private static final Logger LOGGER = LoggerFactory.getLogger(NamespaceShardingContentService.class);

    private static final int SHARDING_CONTENT_SLICE_LEN = 1024 * 1023;

    private CuratorFramework curatorFramework;

    private Gson gson = new Gson();

    public NamespaceShardingContentService(CuratorFramework curatorFramework) {
        this.curatorFramework = curatorFramework;
    }

    public String toShardingContent(List<Executor> executors){
        return gson.toJson(executors);
    }

    public void persistDirectly(List<Executor> executorList) throws Exception {
        // sharding/content如果不存在，则新建
        if (curatorFramework.checkExists().forPath(ShrineExecutorsNode.SHARDING_CONTENTNODE_PATH) == null) {
            curatorFramework.create().creatingParentsIfNeeded().forPath(ShrineExecutorsNode.SHARDING_CONTENTNODE_PATH);
        }
        // 删除sharding/content节点下的内容
        List<String> shardingContent = curatorFramework.getChildren()
                .forPath(ShrineExecutorsNode.SHARDING_CONTENTNODE_PATH);
        if (shardingContent != null && !shardingContent.isEmpty()) {
            for (String shardingConentElement : shardingContent) {
                curatorFramework.delete()
                        .forPath(ShrineExecutorsNode.getShardingContentElementNodePath(shardingConentElement));
            }
        }

        // 持久化新的内容
        String shardingContentStr = toShardingContent(executorList);
        LOGGER.info("Persisit sharding content: {}", shardingContentStr);
        // 如果内容过大，分开节点存储。不能使用事务提交，因为即使使用事务、写多个节点，但是提交事务时，仍然会报长度过长的错误。
        byte[] shardingContentBytes = shardingContentStr.getBytes(StandardCharsets.UTF_8);
        int length = shardingContentBytes.length;
        int sliceCount = length / SHARDING_CONTENT_SLICE_LEN + 1;
        for (int i = 0; i < sliceCount; i++) {
            int start = SHARDING_CONTENT_SLICE_LEN * i;
            int end = start + SHARDING_CONTENT_SLICE_LEN;
            if (end > length) {
                end = length;
            }
            byte[] subBytes = Arrays.copyOfRange(shardingContentBytes, start, end);
            curatorFramework.create()
                    .forPath(ShrineExecutorsNode.getShardingContentElementNodePath(String.valueOf(i)), subBytes);
        }
    }

    public Map<String, List<Integer>> getShardingItems(List<Executor> executorList, String jobName) {
        if (executorList == null || executorList.isEmpty()) {
            return Maps.newHashMap();
        }

        Map<String, List<Integer>> shardingItems = new HashMap<>();
        for (Executor tmp : executorList) {
            if (tmp.getJobNameList() != null && tmp.getJobNameList().contains(jobName)) {
                List<Integer> items = new ArrayList<>();
                for (Shard shard : tmp.getShardList()) {
                    if (shard.getJobName().equals(jobName)) {
                        items.add(shard.getItem());
                    }
                }
                shardingItems.put(tmp.getExecutorName(), items);
            }
        }
        return shardingItems;
    }

    /**
     * @param jobName 作业名
     * @return 返回Map数据，key值为executorName, value为分片项集合
     */
    public Map<String, List<Integer>> getShardingItems(String jobName) throws Exception {
        List<Executor> executorList = getExecutorList();
        return getShardingItems(executorList, jobName);
    }

    public List<Executor> getExecutorList()throws  Exception{
        List<Executor> executorList = Lists.newArrayList();
        //sharding/content 内容多时，分多个节点存数据
        if (curatorFramework.checkExists().forPath(ShrineExecutorsNode.SHARDING_CONTENTNODE_PATH) != null){
            List<String> elementNodes = curatorFramework.getChildren().forPath(ShrineExecutorsNode.SHARDING_CONTENTNODE_PATH);
            Collections.sort(elementNodes , Comparator.comparing(Integer::valueOf));
            List<Byte> dataByteList = new ArrayList<>();
            for (String elementNode : elementNodes) {
                byte[] elementData = curatorFramework.getData()
                        .forPath(ShrineExecutorsNode.getShardingContentElementNodePath(elementNode));
                for (int i = 0; i < elementData.length; i++) {
                    dataByteList.add(elementData[i]);
                }
            }
            byte[] dataArray = new byte[dataByteList.size()];
            for (int i = 0; i < dataByteList.size(); i++) {
                dataArray[i] = dataByteList.get(i);
            }
            List<Executor> tmp = gson.fromJson(new String(dataArray, StandardCharsets.UTF_8), new TypeToken<List<Executor>>() {
            }.getType());
            if (tmp != null) {
                executorList.addAll(tmp);
            }
        }
        return executorList;
    }

    public void persistJobsNecessaryInTransaction(Map<String /*jobName*/, Map<String /*executorName*/ , List<Integer> /*items*/>> jobShardContent) throws Exception {
        if (! jobShardContent.isEmpty()){
            LOGGER.info("Notify jobs sharding necessary, jobs is {}", jobShardContent.keySet());
            CuratorMultiTransaction multiTransaction = curatorFramework.transaction();
            CuratorOp curatorOp = curatorFramework.transactionOp().check().forPath("/");
            for (Map.Entry<String, Map<String, List<Integer>>> entry : jobShardContent.entrySet()) {
                String jobName = entry.getKey();
                Map<String , List<Integer>> shardContent = entry.getValue();
                String shardContentJson = gson.toJson(shardContent);
                byte[] necessaryContent = shardContentJson.getBytes(StandardCharsets.UTF_8);
                // 更新$Jobs/xx/leader/sharding/neccessary 节点的内容为新分配的sharding 内容
                String jobLeaderShardingNodePath = ShrineExecutorsNode.getJobLeaderShardingNodePath(jobName);
                String jobLeaderShardingNecessaryNodePath = ShrineExecutorsNode
                        .getJobLeaderShardingNecessaryNodePath(jobName);
                if (curatorFramework.checkExists().forPath(jobLeaderShardingNodePath) == null) {
                    curatorFramework.create().creatingParentsIfNeeded().forPath(jobLeaderShardingNodePath);
                }
                CuratorOp curatorOp1;
                if (curatorFramework.checkExists().forPath(jobLeaderShardingNecessaryNodePath) == null) {
                     curatorOp1 = curatorFramework.transactionOp().create().forPath(jobLeaderShardingNecessaryNodePath, necessaryContent);
                } else {
                    curatorOp1 = curatorFramework.transactionOp().setData().forPath(jobLeaderShardingNecessaryNodePath, necessaryContent);
                }
                multiTransaction.forOperations(curatorOp , curatorOp1);
            }
        }
    }

    public Map<String , List<Integer>> getShardingContent(String jobName , String jobNecessaryContent) throws Exception{

        Map<String , List<Integer>> shardContent = Maps.newHashMap();
        try {
            Map<String, List<Integer>> obj = gson
                    .fromJson(jobNecessaryContent, new TypeToken<Map<String, List<Integer>>>() {
                    }.getType());
            shardContent.putAll(obj);

        }catch (Exception e){
            LOGGER.warn("deserialize " + jobName
                    + "'s shards from necessary failed, will try to get shards from sharding/content", e);
            shardContent.putAll(getShardingItems(jobName));
        }
        return shardContent;
    }


}
