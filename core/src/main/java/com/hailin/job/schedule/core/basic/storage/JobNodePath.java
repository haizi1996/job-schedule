package com.hailin.job.schedule.core.basic.storage;

import lombok.RequiredArgsConstructor;

/**
 * 作业节点路径
 * 作业节点是在普通节点前加上作业名称的前缀
 * @author zhanghailin
 */
@RequiredArgsConstructor
public final class JobNodePath {

    public static final String NODE_NAME = "$JOBS" ;

    public static final String ROOT = "/" + NODE_NAME;

    private static final String LEADER_HOST_NODE = "leader/election/instance";

    private static final String CONFIG_NODE = "config";

    private static final String SERVERS_NODE = "servers";

    private static final String INSTANCES_NODE = "instances";

    private static final String SHARDING_NODE = "sharding";

    private final String jobName;
    /**
     *获取作业节点全路径
     * @param jn 作业节点全路径
     */
    public static String getJobNameFullPath(String jn){
        return String.format("/%s/%s" , NODE_NAME , jn);
    }

    /**
     * 获取节点全路径
     * @param jobName 作业名
     * @param node 节点名
     */
    public static String getNodeFullPath(final String jobName , final String node){
        return String.format("/%s/%s/%s" , NODE_NAME ,jobName , node);
    }
    /**
     * 获取配置节点根路径.
     *
     * @return 配置节点根路径
     */
    public String getConfigNodePath() {
        return String.format("/%s/%s", jobName, CONFIG_NODE);
    }

    /**
     *  获取配置节点的全路径
     * @param jobName 作业名
     * @param nodeName 节点名
     */
    public static String getConfigNodePath(String jobName , final String nodeName){
        return String.format("/%s/%s/config/%s" , NODE_NAME ,jobName , nodeName);
    }
    /**
     *  获取配置服务器节点的全路径
     * @param jobName 作业名
     * @param nodeName 节点名
     */
    public static String getServerNodePath(String jobName , final String nodeName){
        return String.format("/%s/%s/config/%s" , NODE_NAME ,jobName , nodeName);
    }
    /**
     * 获取节点全路径.
     *
     * @param node 节点名称
     * @return 节点全路径
     */
    public String getFullPath(final String node) {
        return String.format("/%s/%s", jobName, node);
    }
}
