package com.hailin.job.schedule.core.listener;

import com.hailin.job.schedule.core.basic.Shutdownable;
import com.hailin.job.schedule.core.basic.analyse.AnalyseResetListenerManager;
import com.hailin.job.schedule.core.basic.config.ConfigurationListenerManager;
import com.hailin.job.schedule.core.basic.control.ControlListenerManager;
import com.hailin.job.schedule.core.basic.election.ElectionListenerManager;
import com.hailin.job.schedule.core.basic.failover.FailoverListenerManager;
import com.hailin.job.schedule.core.basic.guarantee.GuaranteeListenerManager;
import com.hailin.job.schedule.core.basic.instance.ShutdownListenerManager;
import com.hailin.job.schedule.core.basic.instance.TriggerListenerManager;
import com.hailin.job.schedule.core.basic.monitor.MonitorExecutionListenerManager;
import com.hailin.job.schedule.core.basic.server.JobOperationListenerManager;
import com.hailin.job.schedule.core.basic.sharding.ShardingListenerManager;
import com.hailin.job.schedule.core.basic.storage.JobNodeStorage;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.job.schedule.core.schedule.RescheduleListenerManager;

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

    private ShutdownListenerManager shutdownListenerManager;

    private RescheduleListenerManager rescheduleListenerManager;

    private TriggerListenerManager triggerListenerManager;

    private final GuaranteeListenerManager guaranteeListenerManager;

    private final MonitorExecutionListenerManager monitorExecutionListenerManager;


    public ListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName){
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        //创建监听器实例
        electionListenerManager = new ElectionListenerManager( jobName , regCenter);
        failoverListenerManager = new FailoverListenerManager(jobName , regCenter);
        shardingListenerManager = new ShardingListenerManager(jobName , regCenter);
        shutdownListenerManager = new ShutdownListenerManager(jobName , regCenter);
        monitorExecutionListenerManager = new MonitorExecutionListenerManager(jobName , regCenter);
        triggerListenerManager = new TriggerListenerManager(jobName , regCenter);
        rescheduleListenerManager = new RescheduleListenerManager(jobName , regCenter);
//        jobOperationListenerManager = new JobOperationListenerManager(jobScheduler);
//        configurationListenerManager = new ConfigurationListenerManager(jobScheduler);
//        analyseResetListenerManager = new AnalyseResetListenerManager(jobScheduler);
//        controlListenerManager = new ControlListenerManager(jobScheduler);
        guaranteeListenerManager = new GuaranteeListenerManager(jobName ,regCenter);
        regCenterConnectionStateListener = new RegistryCenterConnectionStateListener(regCenter , jobName);


    }

    public void start() {

        //开启监听
        electionListenerManager.start();
        shardingListenerManager.start();
        failoverListenerManager.start();
        monitorExecutionListenerManager.start();
        shutdownListenerManager.start();
        triggerListenerManager.start();
        rescheduleListenerManager.start();
        guaranteeListenerManager.start();
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
