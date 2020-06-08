package com.hailin.job.schedule.executor;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Objects;

public class Main {

    // 命名空间
    private String namespace;
    // 执行器名称
    private String executorName;
    // jar
    private String saturnLibDir = getLibDir("lib");
    private String appLibDir = getLibDir("lib");
    // 执行器 类加载器
    private ClassLoader executorClassLoader;
    //  job 类加载器
    private ClassLoader jobClassLoader;
    //执行器对象
    private Object executor;

    private boolean executorClassLoaderShouldBeClosed;
    private boolean jobClassLoaderShouldBeClosed;

    public static void main(String[] args) {
        try {
            Main main = new Main();
            main.parseArgs(args);
            main.initClassLoader(null , null);
            main.startExecutor(null);
        }catch (InvocationTargetException ite){
            printThrowableAndExit(ite.getCause());
        } catch (Throwable t) {// NOSONAR
            printThrowableAndExit(t);
        }
    }

    private void startExecutor(Object application) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(executorClassLoader);
        try {
            Class<?> startExecutorClass = getScheduleExecutorClass();
            executor = startExecutorClass.getMethod("buildExecutor" , String.class , String.class , ClassLoader.class , ClassLoader.class , Object.class)
                    .invoke(null , namespace , executorName , executorClassLoader , jobClassLoader , application);
            startExecutorClass.getMethod("execute").invoke(executor);
        }finally {
            Thread.currentThread().setContextClassLoader(oldCL);

        }
    }

    private Class<?> getScheduleExecutorClass() throws ClassNotFoundException {
        return executorClassLoader.loadClass("com.hailin.job.schedule.executor.ScheduleExecutor");
    }

    private static void printThrowableAndExit(Throwable t) {
        if (t != null) {
            t.printStackTrace(); // NOSONAR
        }
        System.exit(1);
    }

    protected void parseArgs(String[] inArgs) throws Exception {
        String[] args = inArgs.clone();

        for (int i = 0; i < args.length; i++) {
            String param = args[i].trim();

            switch (param) {
                case "-namespace":
                    this.namespace = obtainParam(args, ++i, "namespace");
                    System.setProperty("app.instance.name", this.namespace);
                    System.setProperty("namespace", this.namespace);
                    break;
                case "-executorName":
                    this.executorName = obtainParam(args, ++i, "executorName");
                    break;
                case "-saturnLibDir":
                    this.saturnLibDir = obtainParam(args, ++i, "saturnLibDir");
                    break;
                case "-appLibDir":
                    this.appLibDir = obtainParam(args, ++i, "appLibDir");
                    break;
                default:
                    break;
            }
        }

        validateMandatoryParameters();
    }

    private String obtainParam(String[] args, int position, String paramName) {
        String value = null;
        if (position < args.length) {
            value = args[position].trim();
        }
        if (isBlank(value)) {
            throw new RuntimeException(String.format("Please set the value of parameter:%s", paramName));
        }
        return value;
    }

    private void validateMandatoryParameters() {
        if (isBlank(namespace)) {
            throw new RuntimeException("Please set the namespace parameter");
        }
    }

    private void initClassLoader(ClassLoader executorClassLoader , ClassLoader jobClassLoader) throws MalformedURLException {
        setExecutorClassLoader(executorClassLoader);
        setJobClassLoader(jobClassLoader);
    }

    public void launch(String[] args, ClassLoader jobClassLoader) throws Exception {
        parseArgs(args);
        initClassLoader(null, jobClassLoader);
        startExecutor(null);
    }

    public void launch(String[] args, ClassLoader jobClassLoader, Object saturnApplication) throws Exception {
        parseArgs(args);
        initClassLoader(null, jobClassLoader);
        startExecutor(saturnApplication);
    }

    public void launchInner(String[] args, ClassLoader executorClassLoader, ClassLoader jobClassLoader)
            throws Exception {
        parseArgs(args);
        initClassLoader(executorClassLoader, jobClassLoader);
        startExecutor(null);
    }

    private void setExecutorClassLoader(ClassLoader executorClassLoader) throws MalformedURLException {
        if (Objects.isNull(executorClassLoader)){
            List<URL> urls = getUrls(new File(saturnLibDir));
            this.executorClassLoader = new ScheduleClassLoader( urls.toArray(new URL[0]) , Main.class.getClassLoader());
            this.executorClassLoaderShouldBeClosed = true;
        }else {
            this.executorClassLoader = executorClassLoader;
            this.executorClassLoaderShouldBeClosed = false;
        }
    }
    private void setJobClassLoader(ClassLoader jobClassLoader) throws MalformedURLException {
        if (jobClassLoader == null) {
            if (new File(appLibDir).isDirectory()) {
                List<URL> urls = getUrls(new File(appLibDir));
                this.jobClassLoader = new JobClassLoader(urls.toArray(new URL[urls.size()]));
                this.jobClassLoaderShouldBeClosed = true;
            } else {
                this.jobClassLoader = this.executorClassLoader;
                this.jobClassLoaderShouldBeClosed = false;
            }
        } else {
            this.jobClassLoader = jobClassLoader;
            this.jobClassLoaderShouldBeClosed = false;
        }
    }

    private List<URL> getUrls(File file) throws MalformedURLException {
        List<URL> urls = Lists.newArrayList();
        if (!file.exists()){
            return urls;
        }
        if (file.isDirectory()){
            if ("classes".equals(file.getName())){
                urls.add(file.toURI().toURL());
                return urls;
            }
            File[] files = file.listFiles();
            if (ArrayUtils.isNotEmpty(files)){
                for (File f : files){
                    urls.addAll(getUrls(f));
                }
            }
            return urls;
        }
        if (file.isFile()){
            urls.add(file.toURI().toURL());
        }
        return urls;
    }

    private boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    private static String getLibDir(String dirName) {
        File root = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().getFile()).getParentFile();
        File lib = new File(root, dirName);
        if (!lib.isDirectory()) {
            return null;
        }
        return lib.getAbsolutePath();
    }

    public void shutdown() throws Exception {
        shutdown("shutdown");
    }

    public void shutdownGracefully() throws Exception {
        shutdown("shutdownGracefully");
    }

    private void shutdown(String methodName) throws Exception {
        ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(executorClassLoader);
        try {
            Class<?> startExecutorClass = getScheduleExecutorClass();
            startExecutorClass.getMethod(methodName).invoke(executor);
        } finally {
            Thread.currentThread().setContextClassLoader(oldCL);
            closeClassLoader();
        }
    }
    private void closeClassLoader() {
        try {
            if (jobClassLoaderShouldBeClosed && jobClassLoader != null && jobClassLoader instanceof Closeable) {
                ((Closeable) jobClassLoader).close();
            }
        } catch (IOException e) { // NOSONAR
        }
        try {
            if (executorClassLoaderShouldBeClosed && executorClassLoader != null
                    && executorClassLoader instanceof Closeable) {
                ((Closeable) executorClassLoader).close();
            }
        } catch (IOException e) { // NOSONAR
        }
    }
}
