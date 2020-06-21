package com.hailin.schedule.job.console;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduleEnvProperties {

    /**
     * 指定Console在进行sharding计算和dashbaord统计等计算的服务器集群标识
     */
    public static String NAME_SCHEDULE_CONSOLE_CLUSTER = "SCHEDULE_CONSOLE_CLUSTER";
    public static String SCHEDULE_CONSOLE_CLUSTER_ID = StringUtils
            .trim(System.getProperty(NAME_SCHEDULE_CONSOLE_CLUSTER, System.getenv(NAME_SCHEDULE_CONSOLE_CLUSTER)));
    /**
     * zk注册中心
     */
    public static String NAME_SCHEDULE_ZK_CONNECTION = "SCHEDULE_ZK_CONNECTION";
    public static String CONTAINER_TYPE = System.getProperty("SCHEDULE_CONTAINER_TYPE",
            System.getenv("SCHEDULE_CONTAINER_TYPE"));
    public static String SCHEDULE_DCOS_REST_URI = System.getProperty("SCHEDULE_DCOS_REST_URI",
            System.getenv("SCHEDULE_DCOS_REST_URI"));
    public static String SCHEDULE_DCOS_REGISTRY_URI = System.getProperty("SCHEDULE_DCOS_REGISTRY_URI",
            System.getenv("SCHEDULE_DCOS_REGISTRY_URI"));
    public static String NAME_SCHEDULE_EXECUTOR_CLEAN = "SCHEDULE_EXECUTOR_CLEAN";
    public static String NAME_SCHEDULE_DCOS_TASK = "SCHEDULE_DCOS_TASK";
    protected static Logger log = LoggerFactory.getLogger(ScheduleEnvProperties.class);

    static {
        if (CONTAINER_TYPE == null) {
            CONTAINER_TYPE = "MARATHON";
        }
    }

    private ScheduleEnvProperties() {
    }

}
