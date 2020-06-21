package com.hailin.schedule.job.console.service.impl;

import com.google.common.collect.Sets;
import com.hailin.schedule.job.console.ScheduleEnvProperties;
import com.hailin.schedule.job.console.service.RegistryCenterService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Set;

@Service
public class RegistryCenterServiceImpl implements RegistryCenterService {

    private static final Logger log = LoggerFactory.getLogger(RegistryCenterServiceImpl.class);

    protected static final String DEFAULT_CONSOLE_CLUSTER_ID = "default";

    protected static final String NAMESPACE_CREATOR_NAME = "REST_API";

    protected static final String ERR_MSG_TEMPLATE_FAIL_TO_CREATE = "Fail to create new namespace {%s} for reason {%s}";

    protected static final String ERR_MSG_NS_NOT_FOUND = "The namespace does not exists.";

    protected static final String ERR_MSG_NS_ALREADY_EXIST = "Invalid request. Namespace: {%s} already existed";

    // 集群ID
    private String consoleClusterId;

    private Set<String> restrictComputeZkClusterKeys = Sets.newHashSet();

    @Override
    @PostConstruct
    public void init() throws Exception {
        getConsoleClusterId();
        localRefresh();
    }

    private synchronized void localRefresh() {

        try {
            log.info("Start refresh RegCenter");
            long startTime = System.currentTimeMillis();
            refreshRestrictComputeZkClusters();
            if (restrictComputeZkClusterKeys.isEmpty()) {
                log.warn("根据Console的集群ID:" + consoleClusterId + ",找不到配置可以参与Sharding和Dashboard计算的zk集群");
                return;
            }
            refreshRegistryCenter();
            refreshDashboardLeaderTreeCache();
            refreshNamespaceShardingListenerManagerMap();
            log.info("End refresh RegCenter, cost {}ms", System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            log.error("refresh RegCenter error", e);
        }
    }

    /**
     * 解析Console集群的映射关系 数据库中配置的例子 如下 CONSOLE-1:/saturn,/forVdos;CONSOLE-2:/zk3; 如果不存在此配置项，则可以计算所有zk集群；
     */
    private void refreshRestrictComputeZkClusters() {

        // 清空 当前可计算的ZK集群列表
        restrictComputeZkClusterKeys.clear();


    }

    /**
     * 获取集群ID
     */
    private void getConsoleClusterId() {

        if (StringUtils.isBlank(ScheduleEnvProperties.SCHEDULE_CONSOLE_CLUSTER_ID)) {
            log.info(
                    "No environment variable or system property of [VIP_SATURN_CONSOLE_CLUSTER] is set. Use the default Id");
            consoleClusterId = DEFAULT_CONSOLE_CLUSTER_ID;
        } else {
            consoleClusterId = ScheduleEnvProperties.SCHEDULE_CONSOLE_CLUSTER_ID;
        }
    }
}
