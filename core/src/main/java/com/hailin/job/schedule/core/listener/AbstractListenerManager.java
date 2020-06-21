package com.hailin.job.schedule.core.listener;

import com.hailin.job.schedule.core.basic.Shutdownable;
import com.hailin.job.schedule.core.basic.storage.JobNodeStorage;
import com.hailin.job.schedule.core.config.JobConfiguration;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.job.schedule.core.reg.zookeeper.ZkCacheManager;
import com.hailin.job.schedule.core.strategy.JobScheduler;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
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


    /**
     * 开启监听器.
     */
    public abstract void start();

    @Override
    public void shutdown() {

    }
}
