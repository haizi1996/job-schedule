package com.hailin.shrine.job.core.basic.listener;

import com.hailin.shrine.job.core.basic.Shutdownable;
import com.hailin.shrine.job.core.job.config.JobConfiguration;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.shrine.job.core.reg.zookeeper.ZkCacheManager;
import com.hailin.shrine.job.core.strategy.JobScheduler;
import org.apache.curator.framework.state.ConnectionStateListener;

/**
 * 作业注册中心的监听器管理者的抽象类
 * @author zhanghailin
 */
public abstract class AbstractListenerManager implements Shutdownable {

    //注册中心
    protected CoordinatorRegistryCenter coordinatorRegistryCenter;

    //作业名称
    protected String jobName;

    //执行器的名称
    protected String executorName;

    //任务调度器
    protected JobScheduler jobScheduler;

    //作业配置类
    protected JobConfiguration jobConfiguration;

    //作业缓存管理
    protected ZkCacheManager zkCacheManager;

    public AbstractListenerManager(JobScheduler jobScheduler) {
        this.jobScheduler = jobScheduler;
        this.jobName = jobScheduler.getJobName();
        this.executorName = jobScheduler.getExecutorName();
        this.jobConfiguration = jobScheduler.getCurrentConf();
        coordinatorRegistryCenter = jobScheduler.getRegCenter();
        zkCacheManager = jobScheduler.getZkCacheManager();
    }

    public abstract void start();

    /**
     * 增加一个连接状态的监听器
     * @param listener  连接状态的监听器
     */
    protected void addConnectionStateListener(final ConnectionStateListener listener){
        coordinatorRegistryCenter.addConnectionStateListener(listener);
    }

    /**
     * 移除一个连接状态的监听器
     * @param listener 连接状态的监听器
     */
    protected void removeConnectionStateListener(final ConnectionStateListener listener){
        coordinatorRegistryCenter.removeConnectionStateListener(listener);
    }

    @Override
    public void shutdown() {

    }
}
