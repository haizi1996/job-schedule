package com.hailin.job.schedule.core.basic.instance;

import com.hailin.job.schedule.core.strategy.JobInstance;
import com.hailin.job.schedule.core.basic.AbstractShrineService;
import com.hailin.job.schedule.core.basic.server.ServerService;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;

import java.util.LinkedList;
import java.util.List;

/**
 * 作业运行实例服务
 */
public final class InstanceService  extends AbstractShrineService {


    private InstanceNode instanceNode;

    private ServerService serverService;

    public InstanceService(final String jobName, final CoordinatorRegistryCenter registryCenter) {
        super(jobName , registryCenter);
        instanceNode = new InstanceNode(jobName);
        serverService = new ServerService(jobName , registryCenter);
    }
    /**
     * 持久化作业运行实例上线相关信息.
     */
    public void persistOnline() {
        jobNodeStorage.fillEphemeralJobNode(instanceNode.getLocalInstanceNode(), "");
    }

    /**
     * 删除作业运行状态.
     */
    public void removeInstance() {
        jobNodeStorage.removeJobNodeIfExisted(instanceNode.getLocalInstanceNode());
    }

    /**
     * 清理作业触发标记.
     */
    public void clearTriggerFlag() {
        jobNodeStorage.updateJobNode(instanceNode.getLocalInstanceNode(), "");
    }

    /**
     * 获取可分片的作业运行实例.
     *
     * @return 可分片的作业运行实例
     */
    public List<JobInstance> getAvailableJobInstances() {
        List<JobInstance> result = new LinkedList<>();
        for (String each : jobNodeStorage.getJobNodeChildrenKeys(InstanceNode.ROOT)) {
            JobInstance jobInstance = new JobInstance(each);
            if (serverService.isEnableServer(jobInstance.getIp())) {
                result.add(new JobInstance(each));
            }
        }
        return result;
    }

    /**
     * 判断当前作业运行实例的节点是否仍然存在.
     *
     * @return 当前作业运行实例的节点是否仍然存在
     */
    public boolean isLocalJobInstanceExisted() {
        return jobNodeStorage.isJobNodeExisted(instanceNode.getLocalInstanceNode());
    }
}
