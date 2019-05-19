package com.hailin.shrine.job.core.executor;

import java.util.Map;

/**
 *
 * @author zhanghailin
 */
public abstract class ShrineExecutorExtension {

    protected String executorName;

    protected String namespace;

    protected ClassLoader jobClassLoader;

    protected ClassLoader executorClassLoader;

    public ShrineExecutorExtension(String executorName, String namespace, ClassLoader jobClassLoader, ClassLoader executorClassLoader) {
        this.executorName = executorName;
        this.namespace = namespace;
        this.jobClassLoader = jobClassLoader;
        this.executorClassLoader = executorClassLoader;
    }

    public abstract void initBefore();

    public abstract void initLogDirEnv();

    public abstract void initLog();

    public abstract void initAfter();

    public abstract void registerJobType();

    public abstract void validateNamespaceExisting(String connectString) throws Exception;

    public abstract void init();

    public abstract Class getExecutorConfigClass();

    public abstract void postDiscover(Map<String, String> discoveryInfo);

    public abstract void handleExecutorStartError(Throwable t);

}
