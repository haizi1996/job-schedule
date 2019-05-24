package com.hailin.shrine.job.core.basic.listener;

import com.hailin.shrine.job.core.basic.JobRegistry;
import com.hailin.shrine.job.core.basic.execution.ExecutionService;
import com.hailin.shrine.job.core.basic.instance.InstanceService;
import com.hailin.shrine.job.core.basic.server.ServerService;
import com.hailin.shrine.job.core.basic.sharding.ShardingService;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.shrine.job.core.schedule.JobScheduleController;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

/**
 * 注册中心连接状态监听器.
 *
 * @author zhanghailin
 */
public final class RegistryCenterConnectionStateListener implements ConnectionStateListener {
    
    private final String jobName;
    
    private final ServerService serverService;
    
    private final InstanceService instanceService;
    
    private final ShardingService shardingService;
    
    private final ExecutionService executionService;
    
    public RegistryCenterConnectionStateListener(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        serverService = new ServerService( jobName , regCenter);
        instanceService = new InstanceService( jobName , regCenter);
        shardingService = new ShardingService( jobName , regCenter);
        executionService = new ExecutionService( jobName , regCenter);
    }
    
    @Override
    public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
        if (JobRegistry.getInstance().isShutdown(jobName)) {
            return;
        }
        JobScheduleController jobScheduleController = JobRegistry.getInstance().getJobScheduleController(jobName);
        if (ConnectionState.SUSPENDED == newState || ConnectionState.LOST == newState) {
            jobScheduleController.pauseJob();
        } else if (ConnectionState.RECONNECTED == newState) {
            // 持久化作业服务器上线信息
            serverService.persistOnline(serverService.isEnableServer(JobRegistry.getInstance().getJobInstance(jobName).getIp()));
            // 持久化作业运行实例上线相关信息
            instanceService.persistOnline();
            // 清除本地分配的作业分片项运行中的标记
            executionService.clearRunningInfo(shardingService.getLocalShardingItems());
            // 恢复作业调度
            jobScheduleController.resumeJob();
        }
    }
}
