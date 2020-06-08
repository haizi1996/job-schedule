package com.hailin.job.schedule.core.reg.zookeeper;

import com.google.common.collect.Maps;
import com.hailin.job.schedule.core.basic.threads.ScheduleThreadFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * zk缓存的管理器
 * @author zhangailin
 */
public class ZkCacheManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkCacheManager.class);

    private Map<String , TreeCache> treeCacheMap = Maps.newHashMap();
    private Map<String , NodeCache> nodeCacheMap = Maps.newHashMap();

    private String jobName;

    private CuratorFramework client;

    private String executorName;

    private ExecutorService executorService;

    public ZkCacheManager(CuratorFramework client , String jobName , String executorName) {
        this.jobName = jobName;
        this.client = client;
        this.executorName = executorName;
        executorService = Executors
                .newSingleThreadExecutor(new ScheduleThreadFactory(executorName + "-" + jobName + "-watcher", false));
        LOGGER.info( jobName, "ZkCacheManager for executor:{} - job:{} created.", executorName, jobName);
    }

    public NodeCache buildAndStartNodeCache(String path){
        try{
            NodeCache nodeCache = nodeCacheMap.get(path);
            if (nodeCache == null){
                nodeCache = new NodeCache(client , path);
                nodeCacheMap.put(path , nodeCache);
                nodeCache.start();
                LOGGER.info( jobName, "{} - {} builds nodeCache for path = {}", executorName, jobName, path);
            }
            return nodeCache;
        }catch (Exception e){
            LOGGER.error(jobName,
                    "{} - {}  fails in building nodeCache for path = {}, saturn will not work correctly.", executorName,
                    jobName, path);
            LOGGER.error( jobName, e.getMessage(), e);
        }
        return null;
    }

    private TreeCache buildAndStartTreeCache(String path, int depth) {
        try {
            String key = buildMapKey(path, depth);
            TreeCache tc = treeCacheMap.get(key);
            if (tc == null) {
                tc = TreeCache.newBuilder(client, path).setMaxDepth(depth).setExecutor(
                        executorService).build();
                treeCacheMap.put(key, tc);
                tc.start();
                LOGGER.info(jobName, "{} - {}  builds treeCache for path = {}, depth = {}", executorName,
                        jobName, path, depth);
            }
            return tc;
        } catch (Exception e) {
            LOGGER.error( jobName,
                    "{} - {} fails in building treeCache for path = {}, depth = {}, saturn will not work correctly.",
                    executorName, jobName, path, depth);
            LOGGER.error( jobName, e.getMessage(), e);
        }
        return null;
    }

    /**
     * 构建M缓存Map的key
     * @param path 路径
     * @param depth 高度
     */
    private static String buildMapKey(String path, int depth) {
        return path + "-" + depth;
    }

    /**
     * 增加一个treeCache的监听器
     * @param listener 监听器
     * @param path 路径
     * @param depth tree的高度
     */
    public void addTreeCacheListener(final TreeCacheListener listener , final String path , final int depth){
        TreeCache treeCache = buildAndStartTreeCache(path, depth);
        if (treeCache != null){
            treeCache.getListenable().addListener(listener);
        }
    }
    public void addNodeCacheListener(final NodeCacheListener listener, final String path) {
        NodeCache nc = buildAndStartNodeCache(path);
        if (nc != null) {
            nc.getListenable().addListener(listener);
        }
    }

    /**
     * 关闭TreeCache
     * @param path 路径
     * @param depth tree高度
     */
    public void closeTreeCache(String path , int depth){
        String key = buildMapKey(path,depth);
        TreeCache treeCache = treeCacheMap.get(key);
        if (treeCache != null){
            try {
                treeCache.close();
                treeCacheMap.remove(key);
                LOGGER.info( jobName, "{} - {} closed treeCache, path and depth is {}", executorName, jobName,
                        key);
            } catch (Exception e) {
                LOGGER.error(jobName, e.getMessage(), e);
            }
        }
    }

    public void closeAllTreeCache(){
        Iterator<Map.Entry<String, TreeCache>> iterator = treeCacheMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, TreeCache> next = iterator.next();
            TreeCache tc = next.getValue();
            String path = next.getKey();
            try {
                tc.close();
                iterator.remove();
                LOGGER.info(jobName, "{} - {} closed treeCache, path and depth is {}", executorName, jobName,
                        path);
            } catch (Exception e) {
                LOGGER.error( jobName, e.getMessage(), e);
            }
        }
    }

    public void closeNodeCache(String path) {
        NodeCache nc = nodeCacheMap.get(path);
        if (nc != null) {
            try {
                nc.close();
                nodeCacheMap.remove(path);
                LOGGER.info(jobName, "{} - {} closed nodeCache, path is {}", executorName, jobName, path);
            } catch (Exception e) {
                LOGGER.error(jobName, e.getMessage(), e);
            }
        }
    }

    public void closeAllNodeCache() {
        Iterator<Map.Entry<String, NodeCache>> iterator = nodeCacheMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, NodeCache> next = iterator.next();
            NodeCache nc = next.getValue();
            String path = next.getKey();
            try {
                nc.close();
                iterator.remove();
                LOGGER.info(jobName, "{} - {} closed nodeCache, path is {}", executorName, jobName, path);
            } catch (Exception e) {
                LOGGER.error( jobName, e.getMessage(), e);
            }
        }
    }

    public void shutdown() {
        closeAllTreeCache();
        closeAllNodeCache();
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }

    public Map<String, TreeCache> getTreeCacheMap() {
        return treeCacheMap;
    }

    public void setTreeCacheMap(Map<String, TreeCache> treeCacheMap) {
        this.treeCacheMap = treeCacheMap;
    }

    public Map<String, NodeCache> getNodeCacheMap() {
        return nodeCacheMap;
    }

    public void setNodeCacheMap(Map<String, NodeCache> nodeCacheMap) {
        this.nodeCacheMap = nodeCacheMap;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void setClient(CuratorFramework client) {
        this.client = client;
    }

    public String getExecutorName() {
        return executorName;
    }

    public void setExecutorName(String executorName) {
        this.executorName = executorName;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }
}
