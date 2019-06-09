package com.hailin.shrine.job.sharding.entity;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 分片的Tree缓存
 */
public class ShardingTreeCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShardingTreeCache.class);

    private Map<String , TreeCache> treeCacheMap = Maps.newHashMap();

    private Map<TreeCache , List<TreeCacheListener>> treeCacheListenerMap = Maps.newHashMap();

    private String getKey(String path , int depth){
        return  path + depth;
    }

    public boolean containsTreeCache(String path, int depth) {
        synchronized (this) {
            return treeCacheMap.containsKey(getKey(path, depth));
        }
    }

    public List<String> getTreeCachePaths() {
        return Lists.newArrayList(treeCacheMap.keySet());
    }

    public void putTreeCache(String path, int depth, TreeCache treeCache) {
        synchronized (this) {
            treeCacheMap.put(getKey(path, depth), treeCache);
            treeCacheListenerMap.put(treeCache, new ArrayList<TreeCacheListener>());
        }
    }

    public TreeCache putTreeCacheIfAbsent(String path, int depth, TreeCache treeCache) {
        synchronized (this) {
            String key = getKey(path, depth);
            if (!treeCacheMap.containsKey(key)) {
                treeCacheMap.put(key, treeCache);
                treeCacheListenerMap.put(treeCache, new ArrayList<TreeCacheListener>());
                return null;
            } else {
                return treeCacheMap.get(key);
            }
        }
    }
    public void removeTreeCache(String path, int depth) {
        String key = getKey(path, depth);
        removeTreeCacheByKey(key);
    }

    public void removeTreeCacheByKey(String key) {
        synchronized (this) {
            TreeCache treeCache = treeCacheMap.get(key);
            if (treeCache != null) {
                treeCacheListenerMap.remove(treeCache);
                treeCacheMap.remove(key);
                treeCache.close();
                LOGGER.info("remove TreeCache success, path+depth is {}", key);
            }
        }
    }

    public TreeCacheListener addTreeCacheListenerIfAbsent(String path , int depth , TreeCacheListener treeCacheListener){
        synchronized (this){
            TreeCacheListener treeCacheListenerOld = null;
            String key = getKey(path , depth);
            TreeCache treeCache = treeCacheMap.get(key);
            if (treeCache == null){
                LOGGER.error("The TreeCache is not exists, cannot add TreeCacheListener, path is {}, depth is {}", path,
                        depth);
            }else {
                List<TreeCacheListener> treeCacheListeners = treeCacheListenerMap.get(key);
                boolean included = false;
                for (TreeCacheListener tmp : treeCacheListeners) {
                    Class<? extends TreeCacheListener> tmpClass = tmp.getClass();
                    Class<? extends TreeCacheListener> treeCacheListenerClass = treeCacheListener.getClass();
                    if (tmpClass.equals(treeCacheListenerClass)) {
                        treeCacheListenerOld = tmp;
                        included = true;
                        break;
                    }
                }
                if (included) {
                    LOGGER.info(
                            "The TreeCache has already included the instance of listener, will not be added, path is {}, depth is {}, listener is {}",
                            path, depth, treeCacheListener.getClass());
                } else {
                    treeCacheListeners.add(treeCacheListener);
                    treeCache.getListenable().addListener(treeCacheListener);
                }
            }
            return treeCacheListenerOld;
        }
    }

    public void shutdown(){
        synchronized (this){
            for (Map.Entry<String, TreeCache> entry:  treeCacheMap.entrySet() ){
                try {
                    entry.getValue().close();
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }
        treeCacheMap.clear();
    }
}
