package com.hailin.job.schedule.core.executor;

import com.google.common.base.Strings;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.reflect.TypeToken;
import com.hailin.job.schedule.core.reg.zookeeper.ZookeeperConfiguration;
import com.hailin.job.schedule.core.utils.StartCheckUtil;
import com.hailin.shrine.job.common.exception.ScheduleExecutorExceptionCode;
import com.hailin.shrine.job.common.exception.ShrineExecutorException;
import com.hailin.shrine.job.common.util.AlarmUtils;
import com.hailin.shrine.job.common.util.JsonUtils;
import com.hailin.shrine.job.common.util.LocalHostService;
import com.hailin.shrine.job.common.util.ResourceUtils;
import com.hailin.job.schedule.core.basic.threads.ScheduleThreadFactory;
import com.hailin.job.schedule.core.reg.zookeeper.ZookeeperRegistryCenter;
import com.hailin.shrine.job.common.util.ScheduleUtils;
import com.hailin.job.schedule.core.utils.ScriptPidUtils;
import com.hailin.shrine.job.common.util.SystemEnvProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
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

    private EnhancedConnectionStateListener connectionLostListener;

    private String executorName;

    private String namespace;

    private ClassLoader executorClassLoader;

    private ClassLoader jobClassLoader;

    private Object scheduleApplication;

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
                           ClassLoader jobClassLoader, Object scheduleApplication) {
        this.executorName = executorName;
        this.namespace = namespace;
        this.executorClassLoader = executorClassLoader;
        this.jobClassLoader = jobClassLoader;
        this.scheduleApplication = scheduleApplication;
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

                // 获取zk的连接地址 根据namespace
                Map<String ,String> discoveryInfo = discover();

                String zkConnectionString = discoveryInfo.get(DISCOVER_INFO_ZK_CONN_STR);
                if (StringUtils.isBlank(zkConnectionString)){
                    logger.error( "zk connection string is blank!");
                    throw new RuntimeException("zk connection string is blank!");
                }
                // 暂时没有逻辑
                scheduleExecutorExtension.postDiscover(discoveryInfo);

                // 初始化注册中心
                initRegistryCenter(zkConnectionString.trim());

                // 检测是否存在仍然有正在运行的SHELL作业
                logger.info( "start to check all exist jobs");
                checkAndKillExistedShellJobs();

                // 添加新增作业时的回调方法，启动已经存在的作业
                logger.info("start to register newJobCallback, and async start existing jobs");
                scheduleExecutorService.registerJobsWatcher();
                logger.info( "The executor {} start successfully which used {} ms",
                        executorName, System.currentTimeMillis() - startTime);
            }catch (Exception e){
                StartCheckUtil.setError(StartCheckUtil.StartCheckItem.ZK);
                throw e;
            }
        }finally {
            shutdownLock.unlock();
        }
    }

    private void checkAndKillExistedShellJobs() {
        try {
            ScriptPidUtils.checkAllExistJobs(regCenter);
            StartCheckUtil.setOk(StartCheckUtil.StartCheckItem.JOBKILL);
        }catch (IllegalStateException e){
            StartCheckUtil.setError(StartCheckUtil.StartCheckItem.JOBKILL);
        }
    }

    private void initRegistryCenter(String serverLists) throws Exception {
        try {
            // 验证namespace是否存在
            scheduleExecutorExtension.validateNamespaceExisting(serverLists);
            // 初始化注册中心
            logger.info("start to init reg center");
            ZookeeperConfiguration zkConfig = new ZookeeperConfiguration(serverLists , namespace , 1000 , 3000);
            regCenter = new ZookeeperRegistryCenter(zkConfig);
            regCenter.init();

            connectionLostListener = new EnhancedConnectionStateListener(executorName) {
                @Override
                public void onLost() {
                    needRestart = true;
                    raiseAlarm();
                }
            };
            regCenter.addConnectionStateListener(connectionLostListener);

            // 创建ScheduleExecutorService
            scheduleExecutorService = new ScheduleExecutorService(regCenter , executorName , scheduleExecutorExtension);
            scheduleExecutorService.setJobClassLoader(jobClassLoader);
            scheduleExecutorService.setExecutorClassLoader(executorClassLoader);
            scheduleExecutorService.setScheduleApplication(scheduleApplication);

            StartCheckUtil.setOk(StartCheckUtil.StartCheckItem.ZK);
        }catch (Exception e){
            StartCheckUtil.setError(StartCheckUtil.StartCheckItem.ZK);
            throw e;
        }
    }

    private void raiseAlarm() {
        logger.warn( "raise alarm to console for executor reinitialization");
        raiseAlarmExecutorService.submit(()-> raiseAlarm2Console(namespace, executorName));
    }

    private void raiseAlarm2Console(String namespace, String executorName) {

        Map<String , Object> alarmInfo = constructAlarmInfo(namespace , executorName);
        try {
            AlarmUtils.raiseAlarm(alarmInfo, namespace);
        } catch (Throwable t) {
            logger.warn( "cannot raise alarm", t);
        }
    }

    private Map<String, Object> constructAlarmInfo(String namespace, String executorName) {
        Map<String, Object> alarmInfo = new HashMap<>();
        alarmInfo.put("executorName", executorName);
        alarmInfo.put("name", "Saturn Event");
        alarmInfo.put("title", "Executor_Restart");
        alarmInfo.put("level", "WARNING");
        alarmInfo.put("message",
                "Executor_Restart: namespace:[" + namespace + "] executor:[" + executorName + "] restart on "
                        + ScheduleUtils.convertTime2FormattedString(System.currentTimeMillis()));

        return alarmInfo;
    }

    private Map<String, String> discover() throws Exception {
        if (SystemEnvProperties.SCHEDULE_CONSOLE_URI_LIST.isEmpty()){
            throw new Exception("Please configure the parameter " + SystemEnvProperties.NAME_SCHEDULE_CONSOLE_URI
                    + " with env or -D");
        }
        int size = SystemEnvProperties.SCHEDULE_CONSOLE_URI_LIST.size();
        for (int i = 0; i < size; i++) {
            String consoleUri = SystemEnvProperties.SCHEDULE_CONSOLE_URI_LIST.get(i);
            String url = consoleUri + "/rest/v1/discovery?namespace=" + namespace;
            CloseableHttpClient httpClient = null;
            try {
                httpClient = HttpClientBuilder.create().build();
                HttpGet httpGet = new HttpGet(url);
                RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(10000)
                        .build();
                httpGet.setConfig(requestConfig);
                CloseableHttpResponse httpResponse = httpClient.execute(httpGet);
                StatusLine statusLine = httpResponse.getStatusLine();
                String responseBody = EntityUtils.toString(httpResponse.getEntity());
                Integer statusCode = statusLine != null ? statusLine.getStatusCode() : null;
                if (statusLine != null && statusCode == HttpStatus.SC_OK) {
                    Map<String, String> discoveryInfo = JsonUtils.getGson()
                            .fromJson(responseBody, new TypeToken<Map<String, String>>() {
                            }.getType());
                    String connectionString = discoveryInfo.get(DISCOVER_INFO_ZK_CONN_STR);
                    if (StringUtils.isBlank(connectionString)) {
                        logger.warn( "ZK connection string is blank!");
                        continue;
                    }

                    logger.info("Discover successfully. Url: {}, discovery info: {}", url, discoveryInfo);
                    return discoveryInfo;
                } else {
                    handleDiscoverException(responseBody, statusCode);
                }
            }catch (ShrineExecutorException e){
                logger.error( e.getMessage(), e);
                if (e.getCode() != ScheduleExecutorExceptionCode.UNEXPECTED_EXCEPTION) {
                    throw e;
                }
            }catch (Throwable t){
                logger.error( "Fail to discover from Saturn Console. Url: {}", url, t);
            }finally {
                if (httpClient != null) {
                    try {
                        httpClient.close();
                    } catch (IOException e) {
                        logger.error( "Fail to close httpclient", e);
                    }
                }
            }
        }
        List<String> context = buildContext();
        String msg = "Fail to discover from Saturn Console! Please make sure that you have added the target namespace on Saturn Console, namespace:%s, context:%s";
        throw new Exception(String.format(msg, namespace, context));

    }

    private List<String> buildContext() {
        List<String> result = new ArrayList<>(5);
        for (String url : SystemEnvProperties.SCHEDULE_CONSOLE_URI_LIST) {
            String ip = getConsoleIp(url);
            String tmp = url + " - " + ip;
            result.add(tmp);
        }
        return result;
    }

    private String getConsoleIp(String consoleUrl) {
        try {
            URL url = new URL(consoleUrl);
            String host = url.getHost();
            return InetAddress.getByName(host).getHostAddress();
        } catch (Exception e) {
            logger.warn( "fail to parse url - {} to ip exception", consoleUrl, e);
            return "unknownIp";
        }
    }

    private void handleDiscoverException(String responseBody, Integer statusCode) throws ShrineExecutorException {
        String errMsgInResponse = obtainErrorResponseMsg(responseBody);

        StringBuilder sb = new StringBuilder("Fail to discover from saturn console. ");
        if (StringUtils.isNotBlank(errMsgInResponse)) {
            sb.append(errMsgInResponse);
        }
        String exceptionMsg = sb.toString();

        if (statusCode != null) {
            if (statusCode == HttpStatus.SC_NOT_FOUND) {
                throw new ShrineExecutorException(ScheduleExecutorExceptionCode.NAMESPACE_NOT_EXIST, exceptionMsg);
            }

            if (statusCode == HttpStatus.SC_BAD_REQUEST) {
                throw new ShrineExecutorException(ScheduleExecutorExceptionCode.BAD_REQUEST, exceptionMsg);
            }
        }

        throw new ShrineExecutorException(ScheduleExecutorExceptionCode.UNEXPECTED_EXCEPTION, exceptionMsg);
    }

    private String obtainErrorResponseMsg(String responseBody) {
        if (StringUtils.isNotBlank(responseBody)) {
            JsonElement message = JsonUtils.getJsonParser().parse(responseBody).getAsJsonObject().get("message");
            return message == JsonNull.INSTANCE || message == null ? "" : message.getAsString();
        }

        return "";
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
