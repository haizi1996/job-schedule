package com.hailin.shrine.job.core.basic.listener;

import com.hailin.shrine.job.core.basic.Shutdownable;
import com.hailin.shrine.job.core.basic.analyse.AnalyseResetListenerManager;
import com.hailin.shrine.job.core.basic.config.ConfigurationListenerManager;
import com.hailin.shrine.job.core.basic.control.ControlListenerManager;
import com.hailin.shrine.job.core.basic.election.ElectionListenerManager;
import com.hailin.shrine.job.core.basic.failover.FailoverListenerManager;
import com.hailin.shrine.job.core.basic.server.JobOperationListenerManager;
import com.hailin.shrine.job.core.basic.sharding.ShardingListenerManager;
import com.hailin.shrine.job.core.basic.storage.JobNodeStorage;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.shrine.job.core.strategy.JobScheduler;

/**
 * 作业注册的中心的监听器管理者
 * @author zhanghailin
 */
public class ListenerManager implements Shutdownable {

    //主节点选举的监听管理
    private ElectionListenerManager electionListenerManager;

    //失败转移的监听管理
    private FailoverListenerManager failoverListenerManager;

    private JobOperationListenerManager jobOperationListenerManager;

    private ConfigurationListenerManager configurationListenerManager;

    private ShardingListenerManager shardingListenerManager;

    private AnalyseResetListenerManager analyseResetListenerManager;

    private ControlListenerManager controlListenerManager;

    private final JobNodeStorage jobNodeStorage;

    private  RegistryCenterConnectionStateListener regCenterConnectionStateListener;


    public ListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName,){
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);

    }

    public void start() {
        //创建监听器实例
        electionListenerManager = new ElectionListenerManager(jobScheduler);
        failoverListenerManager = new FailoverListenerManager(jobScheduler);
        jobOperationListenerManager = new JobOperationListenerManager(jobScheduler);
        configurationListenerManager = new ConfigurationListenerManager(jobScheduler);
        shardingListenerManager = new ShardingListenerManager(jobScheduler);
        analyseResetListenerManager = new AnalyseResetListenerManager(jobScheduler);
        controlListenerManager = new ControlListenerManager(jobScheduler);
        regCenterConnectionStateListener = new RegistryCenterConnectionStateListener(coordinatorRegistryCenter , jobName);

        //开启监听
        electionListenerManager.start();
        failoverListenerManager.start();
        jobOperationListenerManager.start();
        configurationListenerManager.start();
        shardingListenerManager.start();
        analyseResetListenerManager.start();
        controlListenerManager.start();


        jobNodeStorage.addConnectionStateListener(regCenterConnectionStateListener);
    }

    @Override
    public void shutdown() {
        electionListenerManager.shutdown();
        failoverListenerManager.shutdown();
        jobOperationListenerManager.shutdown();
        configurationListenerManager.shutdown();
        shardingListenerManager.shutdown();
        analyseResetListenerManager.shutdown();
        controlListenerManager.shutdown();
    }

}
