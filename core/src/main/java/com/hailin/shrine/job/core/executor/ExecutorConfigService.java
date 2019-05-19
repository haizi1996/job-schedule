package com.hailin.shrine.job.core.executor;

import com.hailin.shrine.job.common.exception.ShrineExecutorException;
import com.hailin.shrine.job.common.util.JsonUtils;
import com.hailin.shrine.job.common.util.LogEvents;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 执行器配置服务类
 * @author zhanghailin
 */
public class ExecutorConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorConfigService.class);

    private static final String EXECUTOR_CONFIG_PATH = "/$ShrineSelf/Shrine-executor/config";

    private String executorName ;

    private CuratorFramework curatorFramework;

    private Class executorConfigClass;

    private NodeCache nodeCache;

    private volatile Object executorConfig;

    public ExecutorConfigService(String executorName, CuratorFramework curatorFramework, Class executorConfigClass) {
        this.executorName = executorName;
        this.curatorFramework = curatorFramework;
        this.executorConfigClass = executorConfigClass;
    }

    public void start() throws Exception{
        validateAndInitExecutorConfig();
        nodeCache = new NodeCache(curatorFramework.usingNamespace(null) ,
                EXECUTOR_CONFIG_PATH);
        nodeCache.getListenable().addListener(()->{
            try {
                final ChildData currentData = nodeCache.getCurrentData();
                if (currentData == null){
                    return;
                }

                String configStr = null;
                byte[] data = currentData.getData();
                if (data != null && data.length > 0){
                    configStr = new String(data , StandardCharsets.UTF_8);
                }
                LOGGER.info(LogEvents.ExecutorEvent.INIT , "The path {} created or updated event is received by {}, the data is {}",
                        EXECUTOR_CONFIG_PATH, executorName, configStr);
                if (StringUtils.isBlank(configStr)){
                    executorConfig = executorConfigClass.newInstance();
                }else {
                    executorConfig = JsonUtils.getGson().fromJson(configStr , executorConfigClass);
                }


            }catch (Throwable t){
                LOGGER.error(LogEvents.ExecutorEvent.INIT , t.toString() , t );
            }
        });
        nodeCache.start(false);
    }

    public void stop(){
        try {
            if (nodeCache != null){
                nodeCache.close();
            }
        }catch (Exception e){
            LOGGER.error(LogEvents.ExecutorEvent.INIT_OR_SHUTDOWN , e.toString() , e);
        }
    }

    private void validateAndInitExecutorConfig() throws Exception{
        if (curatorFramework == null) {
            throw new ShrineExecutorException("curatorFramework cannot be null");
        }

        if (executorConfigClass == null) {
            throw new ShrineExecutorException("executorConfigClass cannot be null");
        }
        Object temp = executorConfigClass.newInstance();
        if (!(temp instanceof ExecutorConfig)) {
            throw new ShrineExecutorException(String.format("executorConfigClass should be %s or its child",
                    ShrineExecutorException.class.getCanonicalName()));
        }
        executorConfig = temp;
    }

    public ExecutorConfig getExecutorConfig(){
        return (ExecutorConfig) executorConfig;
    }
}
