package com.hailin.shrine.job.core.basic.storage;

/**
 * 作业节点路径
 * 作业节点是在普通节点前加上作业名称的前缀
 * @author zhanghailin
 */
public final class JobNodePath {

    public static final String NODE_NAME = "$JOBS" ;

    public static final String ROOT = "/" + NODE_NAME;

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
}
