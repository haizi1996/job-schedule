package com.hailin.job.schedule.core.executor;

import com.hailin.shrine.job.common.exception.ShrineJobException;
import com.hailin.shrine.job.common.util.LogEvents;
import com.hailin.shrine.job.common.util.SystemEnvProperties;
import com.hailin.job.schedule.core.basic.threads.ScheduleThreadFactory;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 重启执行器服务
 * @author zhanghailin
 */
public class RestartAndDumpService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestartAndDumpService.class);

    private String executorName;

    private CoordinatorRegistryCenter coordinatorRegistryCenter;
    private CuratorFramework curatorFramework;

    private File prgDir;
    private NodeCache restartNC;
    private ExecutorService restartES;
    private NodeCache dumpNC;
    private ExecutorService dumpES;

    public RestartAndDumpService(String executorName, CoordinatorRegistryCenter coordinatorRegistryCenter) {
        this.executorName = executorName;
        this.coordinatorRegistryCenter = coordinatorRegistryCenter;
        this.curatorFramework = (CuratorFramework) coordinatorRegistryCenter.getRawClient();
    }

    public void start() throws Exception{
        if (!SystemEnvProperties.SHRINE_ENABLE_EXEC_SCRIPT){
            LOGGER.info( LogEvents.ExecutorEvent.INIT, "The RestartAndDumpService is disabled");
            return;
        }
        validateFile(SystemEnvProperties.NAME_SHRINE_PRG , SystemEnvProperties.SHRINE_PRG);
        validateConfigured(SystemEnvProperties.NAME_SHRINE_LOG_OUTFILE , SystemEnvProperties.SHRINE_LOG_OUTFILE);
        prgDir = new File(SystemEnvProperties.NAME_SHRINE_PRG).getParentFile();
        initRestart();
        initDump();
    }

    private void initDump() throws Exception {
        dumpES = Executors.newSingleThreadExecutor(new ScheduleThreadFactory(executorName + "-dump-watcher-thread", false));
        final String nodePath = ShrineExecutorsNode.EXECUTORS_ROOT + "/" + executorName + "/dump";
        coordinatorRegistryCenter.remove(nodePath);
        dumpNC = new NodeCache(curatorFramework, nodePath);
        dumpNC.getListenable().addListener(new NodeCacheListener() {

            @Override
            public void nodeChanged() throws Exception {
                // Watch create, update event
                if (dumpNC.getCurrentData() != null) {
                    LOGGER.info( LogEvents.ExecutorEvent.DUMP, "The executor {} dump event is received",
                            executorName);
                    dumpES.execute(new Runnable() {
                        @Override
                        public void run() {
                            executeRestartOrDumpCmd("dump", LogEvents.ExecutorEvent.DUMP);
                            coordinatorRegistryCenter.remove(nodePath);
                        }
                    });
                }
            }

        });
        dumpNC.start(false);
    }

    private void initRestart() throws Exception{
        restartES = Executors.newSingleThreadExecutor(new ScheduleThreadFactory(executorName +
                "-restart-watcher-thread" , false));
        String nodePath = ShrineExecutorsNode.EXECUTORS_ROOT + "/" + executorName + "restart";
        coordinatorRegistryCenter.remove(nodePath);
        restartNC = new NodeCache(curatorFramework , nodePath);
        restartNC.getListenable().addListener(()->{
            if (restartNC.getCurrentData() != null){
                LOGGER.info(LogEvents.ExecutorEvent.RESTART , "The executor {} restart event is received",
                        executorName);
                restartES.execute(()->{
                    executeRestartOrDumpCmd("restart" , LogEvents.ExecutorEvent.RESTART);
                });
            }
        });
        restartNC.start(false);
    }

    private void executeRestartOrDumpCmd(String cmd, String eventName) {
        try {
            LOGGER.info(eventName, "Begin to execute {} script", cmd);
            String command = "chmod +x " + SystemEnvProperties.SHRINE_PRG + ";"
                    + SystemEnvProperties.SHRINE_PRG
                    + " " + cmd;
            Process process = new ProcessBuilder()
                    .command("/bin/bash", "-c", command)
                    .directory(prgDir)
                    .redirectOutput(
                            ProcessBuilder.Redirect.appendTo(new File(SystemEnvProperties.SHRINE_LOG_OUTFILE)))
                    .redirectError(
                            ProcessBuilder.Redirect.appendTo(new File(SystemEnvProperties.SHRINE_LOG_OUTFILE)))
                    .start();
            int exit = process.waitFor();
            LOGGER.info(eventName, "Executed {} script, the exit value {} is returned", cmd, exit);
        } catch (InterruptedException e) {
            LOGGER.warn( eventName, "{} thread is interrupted", cmd);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOGGER.error( eventName, "Execute {} script error", cmd, e);
        }
    }

    private void validateFile(String name , String value) throws Exception{
        validateConfigured(name , value);
        File file = new File(value);
        if (!file.exists()){
            throw new ShrineJobException(value + " is not existing");
        }
        if (!file.isFile()){
            throw new ShrineJobException(value + " is not file");
        }
    }
    public void stop() {
        closeNodeCacheQuietly(restartNC);
        if (restartES != null) {
            restartES.shutdownNow();
        }
        closeNodeCacheQuietly(dumpNC);
        if (dumpES != null) {
            dumpES.shutdownNow();
        }
    }


    private void closeNodeCacheQuietly(NodeCache nodeCache) {
        try {
            if (nodeCache != null) {
                nodeCache.close();
            }
        } catch (Exception e) {
            LOGGER.error( LogEvents.ExecutorEvent.INIT_OR_SHUTDOWN, e.toString(), e);
        }
    }

    private void validateConfigured(String name, String value) throws ShrineJobException {
        if (StringUtils.isBlank(value)){
            throw new ShrineJobException(name + " is not configured");
        }
        LOGGER.info(LogEvents.ExecutorEvent.INIT , "The {} is configured as {}");
    }


}
