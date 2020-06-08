package com.hailin.job.schedule.core.executor;

import com.google.common.base.Strings;
import com.hailin.SCHEDULE.job.common.util.SystemEnvProperties;
import com.hailin.job.schedule.core.utils.StartCheckUtil;
import com.hailin.shrine.job.common.util.LocalHostService;
import com.hailin.shrine.job.common.util.ResourceUtils;
import com.hailin.job.schedule.core.basic.threads.ScheduleThreadFactory;
import com.hailin.job.schedule.core.reg.zookeeper.ZookeeperRegistryCenter;
import com.hailin.shrine.job.common.util.SystemEnvProperties;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


public class ScheduleExecutor {

    private static final String DISCOVER_INFO_ZK_CONN_STR = "zkConnStr";

    private static final String SCHEDULE_PROPERTY_FILE_PATH = "job.properties";

    private static final String SCHEDULE_APPLICATION_CLASS = "com.hailin.job.schedule.application.ScheduleApplication";

    private static Logger logger;

    private static AtomicBoolean inited = new AtomicBoolean(false);

    private static ScheduleExecutorExtension scheduleExecutorExtension;

    private ZookeeperRegistryCenter regCenter;

//    private EnhancedConnectionStateListener connectionLostListener;

    private String executorName;

    private String namespace;

    private ClassLoader executorClassLoader;

    private ClassLoader jobClassLoader;

    private Object saturnApplication;

    private ScheduleExecutorService scheduleExecutorService;

//    private ResetCountService resetCountService;

//    private PeriodicTruncateNohupOutService periodicTruncateNohupOutService;

    private ReentrantLock shutdownLock = new ReentrantLock();

    private volatile boolean isShutdown;

    private volatile boolean needRestart = false;

    private Thread restartThread;

    private ExecutorService raiseAlarmExecutorService;

    private ExecutorService shutdownJobsExecutorService;

    private ScheduleExecutor(String namespace, String executorName, ClassLoader executorClassLoader,
                           ClassLoader jobClassLoader, Object saturnApplication) {
        this.executorName = executorName;
        this.namespace = namespace;
        this.executorClassLoader = executorClassLoader;
        this.jobClassLoader = jobClassLoader;
        this.saturnApplication = saturnApplication;
        this.raiseAlarmExecutorService = Executors
                .newSingleThreadExecutor(new ScheduleThreadFactory(executorName + "-raise-alarm-thread", false));
        this.shutdownJobsExecutorService = Executors
                .newCachedThreadPool(new ScheduleThreadFactory(executorName + "-shutdownJobSchedulers-thread", true));
        initRestartThread();
        registerShutdownHandler();
    }

    /**
     * 开始启动
     * @throws Exception
     */
    public void execute() throws Exception {
        shutdownLock.lockInterruptibly();

        try {
            if (isShutdown){
                return;
            }
            long startTime = System.currentTimeMillis();

            // todo
            //shutdown0();

            try {
                StartCheckUtil.add2CheckList(StartCheckUtil.StartCheckItem.ZK, StartCheckUtil.StartCheckItem.UNIQUE, StartCheckUtil.StartCheckItem.JOBKILL);
                logger.info( "start to discover from saturn console");

                Map<String ,String> discoveryInfo = discover();
            }

        }finally {
            shutdownLock.unlock();
        }
    }

    private Map<String, String> discover() throws Exception {
        if (SystemEnvProperties.SCHEDULE_CONSOLE_URI_LIST.isEmpty()){
            throw new Exception("Please configure the parameter " + SystemEnvProperties.NAME_SCHEDULE_CONSOLE_URI
                    + " with env or -D");
        }


    }

    /**
     * 注册退出时 资源清理回调
     */
    private void registerShutdownHandler() {
        Runnable shutdownHandler = ()->{
          if (isShutdown){
              return;
          }
          try {
              shutdownLock.lockInterruptibly();
              try {
                  shutdownGracefully0();
                  restartThread.interrupt();
                  raiseAlarmExecutorService.shutdownNow();
                  shutdownJobsExecutorService.shutdownNow();
                  isShutdown = true;
              }finally {
                  shutdownLock.unlock();
              }
          }catch (Exception e){
              logger.error( e.getMessage(), e);
          }
        };
//        ShutdownHandler.addShutdownCallback(executorName, shutdownHandler);
    }

    /**
     * Executor优雅退出 ： 把自己从集群中拿掉 ，现有的作业不停；
     * 一直到全部u哦作业执行完毕，再真正退出；设置一定超时时间，如果超过这个时间仍退出，则强行中止
     */
    private void shutdownGracefully0() throws InterruptedException {
        shutdownLock.lockInterruptibly();

        logger.info( "Try to stop executor {} gracefully", executorName);
//        if ()

    }

    private void initRestartThread() {
        final String restartThreadName = executorName + "-restart-thread";
        this.restartThread = new Thread(()->{
            try {
                while (true){
                    if (isShutdown){
                        return;
                    }
                    if (needRestart){
                        try {
                            needRestart = false;
                            execute();
                        } catch (Throwable t){
                            needRestart = true;
                            logger.error("Executor {} reinitialize failed, will retry again", executorName, t);
                        }
                    }
                    Thread.sleep(1000L);
                }
            } catch (InterruptedException e) {
                logger.info( "{} is interrupted", restartThreadName);
                Thread.currentThread().interrupt();
            }
        } , restartThreadName);
        this.restartThread.setDaemon(false);
        this.restartThread.start();
    }

    public static ScheduleExecutor buildExecutor(String namespace , String executorName , ClassLoader executorClassLoader , ClassLoader jobClassLoader , Object scheduleApplication){
        if ("$ScheduleSelf".equals(namespace)){
            throw new RuntimeException();
        }
        if (StringUtils.isEmpty(executorName)){
            String hostName = LocalHostService.getHostName();
            if ("localhost".equals(hostName) || "localhost6".equals(hostName)) {
                throw new RuntimeException(
                        "You are using hostName as executorName, it cannot be localhost or localhost6, please configure hostName.");
            }
            executorName = hostName;// NOSONAR
        }
        init(executorName , namespace , executorClassLoader , jobClassLoader);
        if (Objects.isNull(scheduleApplication)){
            scheduleApplication = validateAndScheduleApplication(jobClassLoader);
        }
        return new ScheduleExecutor(namespace , executorName , executorClassLoader , jobClassLoader , scheduleApplication);
    }

    private static Object validateAndScheduleApplication(ClassLoader jobClassLoader) {
        try {
            Properties properties = getScheduleProperty(jobClassLoader);
            if (properties == null) {
                return null;
            }
            String appClassStr = properties.getProperty("app.class");
            if (StringUtils.isBlank(appClassStr)) {
                return null;
            }

            appClassStr = appClassStr.trim();
            ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(jobClassLoader);
                Class<?> appClass = jobClassLoader.loadClass(appClassStr);
                Class<?> saturnApplicationClass = jobClassLoader.loadClass(SCHEDULE_APPLICATION_CLASS);
                if (saturnApplicationClass.isAssignableFrom(appClass)) {
                    Object saturnApplication = appClass.newInstance();
                    appClass.getMethod("init").invoke(saturnApplication);
                    logger.info( "SaturnApplication init successfully");
                    return saturnApplication;
                } else {
                    throw new RuntimeException(
                            "the app.class " + appClassStr + " must be instance of " + SCHEDULE_APPLICATION_CLASS);
                }
            } finally {
                Thread.currentThread().setContextClassLoader(oldCL);
            }
        } catch (RuntimeException e) {
            logger.error( "Fail to load SaturnApplication", e);
            throw e;
        } catch (Exception e) {
            logger.error( "Fail to load SaturnApplication", e);
            throw new RuntimeException(e);
        }
    }

    private static Properties getScheduleProperty(ClassLoader jobClassLoader) throws IOException {
        Enumeration<URL> resources = jobClassLoader.getResources(SCHEDULE_PROPERTY_FILE_PATH);
        int count = 0;
        if (resources == null || !resources.hasMoreElements()) {
            return null;
        } else {
            while (resources.hasMoreElements()) {
                resources.nextElement();
                count++;
            }
        }
        if (count == 0) {
            return null;
        }
        if (count > 1) {
            throw new RuntimeException("the file [" + SCHEDULE_PROPERTY_FILE_PATH + "] shouldn't exceed one");
        }

        Properties properties = new Properties();
        InputStream is = null;
        try {
            is = jobClassLoader.getResourceAsStream(SCHEDULE_PROPERTY_FILE_PATH);
            properties.load(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }
        return properties;
    }
    private static void init(String executorName, String namespace, ClassLoader executorClassLoader, ClassLoader jobClassLoader) {
        if (!inited.compareAndSet(false , true)){
            return;
        }
        initExtension(executorName , namespace , executorClassLoader , jobClassLoader);
        logger = LoggerFactory.getLogger(ScheduleExecutor.class);

    }

    private static synchronized void initExtension(String executorName, String namespace, ClassLoader executorClassLoader, ClassLoader jobClassLoader) {
        try {
            Properties props = ResourceUtils.getResource("properties/schedule-ext.properties");
            String extClass = props.getProperty("schedule.ext");
            if (!Strings.isNullOrEmpty(extClass)){
                Class<ScheduleExecutorExtension> loadClass = (Class<ScheduleExecutorExtension>) ScheduleExecutor.class.getClassLoader().loadClass(extClass);
                Constructor<ScheduleExecutorExtension> constructor = loadClass
                        .getConstructor(String.class, String.class, ClassLoader.class, ClassLoader.class);
                scheduleExecutorExtension = constructor
                        .newInstance(executorName, namespace, executorClassLoader, jobClassLoader);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (Objects.isNull(scheduleExecutorExtension)){
                scheduleExecutorExtension = new ScheduleExecutorExtensionDefault(executorName , namespace , executorClassLoader ,jobClassLoader);
            }
        }
    }
}
