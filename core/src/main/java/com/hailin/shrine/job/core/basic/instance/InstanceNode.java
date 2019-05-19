
package com.hailin.shrine.job.core.basic.instance;


import com.hailin.shrine.job.core.basic.JobRegistry;
import com.hailin.shrine.job.core.basic.storage.JobNodePath;

/**
 * 运行实例节点路径.
 * 
 * @author zhangliang
 */
public final class InstanceNode {
    
    /**
     * 运行实例信息根节点.
     */
    public static final String ROOT = "instances";
    
    private static final String INSTANCES = ROOT + "/%s";
    
    private final String jobName;
    

    public InstanceNode(final String jobName) {
        this.jobName = jobName;
    }
    
    /**
     * 获取作业运行实例全路径.
     *
     * @return 作业运行实例全路径
     */
    public String getInstanceFullPath() {
        return JobNodePath.getNodeFullPath(jobName , InstanceNode.ROOT);
    }
    
    /**
     * 判断给定路径是否为作业运行实例路径.
     *
     * @param path 待判断的路径
     * @return 是否为作业运行实例路径
     */
    public boolean isInstancePath(final String path) {
        return path.startsWith(JobNodePath.getNodeFullPath(jobName , InstanceNode.ROOT));
    }
    
    boolean isLocalInstancePath(final String path) {
        return path.equals(JobNodePath.getNodeFullPath(String.format(jobName, JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId())));
    }
    
    String getLocalInstanceNode() {
        return String.format(INSTANCES, JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());
    }
}
