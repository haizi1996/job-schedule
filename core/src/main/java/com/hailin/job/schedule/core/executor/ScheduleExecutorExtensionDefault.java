package com.hailin.job.schedule.core.executor;

import com.hailin.shrine.job.common.util.LocalHostService;
import org.slf4j.Logger;

import java.util.Map;

public class ScheduleExecutorExtensionDefault extends ScheduleExecutorExtension {

    private static Logger log;

    private static final String NAME_JOB_SCHEDULE_LOG_DIR = "JOB_SCHEDULE_LOG_DIR";

    public ScheduleExecutorExtensionDefault(String executorName, String namespace, ClassLoader jobClassLoader, ClassLoader executorClassLoader) {
        super(executorName, namespace, jobClassLoader, executorClassLoader);
    }

    @Override
    public void initBefore() {

    }

    @Override
    public void initLogDirEnv() {
        String saturnLogDir = System
                .getProperty(NAME_JOB_SCHEDULE_LOG_DIR, getEnv(NAME_JOB_SCHEDULE_LOG_DIR, getDefaultLogDir(executorName)));
        System.setProperty("saturn.log.dir", saturnLogDir); // for logback.xml
    }

    private String getEnv(String key, String defaultValue) {
        String v = System.getenv(key);
        if (v == null || v.isEmpty()) {
            return defaultValue;
        }
        return v;
    }

    private static String getDefaultLogDir(String executorName) {
        return "/apps/logs/saturn/" + System.getProperty("namespace") + "/" + executorName + "-"
                + LocalHostService.cachedIpAddress;
    }

    @Override
    public void initLog() {

    }

    @Override
    public void initAfter() {

    }

    @Override
    public void registerJobType() {

    }

    @Override
    public void validateNamespaceExisting(String connectString) throws Exception {

    }

    @Override
    public void init() {

    }

    @Override
    public Class getExecutorConfigClass() {
        return null;
    }

    @Override
    public void postDiscover(Map<String, String> discoveryInfo) {

    }

    @Override
    public void handleExecutorStartError(Throwable t) {

    }
}
