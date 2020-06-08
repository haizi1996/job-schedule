
package com.hailin.job.schedule.core.basic.election;


import com.hailin.job.schedule.core.basic.storage.JobNodePath;

/**
 * 主节点路径.
 * 
 */
public final class LeaderNode {
    
    /**
     * 主节点根路径.
     */
    public static final String ROOT = "leader";
    
    static final String ELECTION_ROOT = ROOT + "/election";
    
    static final String INSTANCE = ELECTION_ROOT + "/instance";
    
    static final String LATCH = ELECTION_ROOT + "/latch";
    
    private final JobNodePath jobNodePath;
    
    LeaderNode(final String jobName) {
        jobNodePath = new JobNodePath(jobName);
    }
    
    boolean isLeaderInstancePath(final String path) {
        return jobNodePath.getFullPath(INSTANCE).equals(path);
    }
}
