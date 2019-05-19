package com.hailin.shrine.job.core.basic.election;

import com.hailin.shrine.job.core.basic.storage.JobNodePath;

/**
 * 选主服务根节点的常量类
 * @author zhanghailin
 */
public final class ElectionNode {
    /**
     * 主服务器根节点.
     */
    public static final String ROOT = "leader";

    static final String ELECTION_ROOT = ROOT + "/election";

    public static final String LEADER_HOST = ELECTION_ROOT + "/host";

    static final String LATCH = ELECTION_ROOT + "/latch";

    private final String jobName;

    ElectionNode(final String jobName) {
        this.jobName = jobName;
    }

    boolean isLeaderHostPath(final String path) {
        return JobNodePath.getNodeFullPath(jobName, LEADER_HOST).equals(path);
    }

}
