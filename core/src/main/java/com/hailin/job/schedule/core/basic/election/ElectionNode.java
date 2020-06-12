package com.hailin.job.schedule.core.basic.election;

import com.hailin.job.schedule.core.basic.storage.JobNodePath;
import lombok.RequiredArgsConstructor;

/**
 * 主服务器根节点名称的常量类
 */
@RequiredArgsConstructor
public final class ElectionNode {

    public static final String ROOT = "leader" ;

    public static final String ELECTION_ROOT = ROOT + "/election" ;

    public static final String LEADER_HOST = ELECTION_ROOT + "/host" ;

    public static final String LATCH = ELECTION_ROOT + "/latch" ;

    private final String jobName;

    boolean isLeaderHostPath(final String path) {
        return JobNodePath.getNodeFullPath(jobName, LEADER_HOST).equals(path);
    }

}
