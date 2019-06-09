package com.hailin.shrine.job.sharding.task;

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.hailin.shrine.job.integrate.service.ReportAlarmService;
import com.hailin.shrine.job.sharding.entity.Executor;
import com.hailin.shrine.job.sharding.entity.Shard;
import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import com.hailin.shrine.job.sharding.service.NamespaceShardingContentService;
import com.hailin.shrine.job.sharding.service.NamespaceShardingService;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;

public abstract class AbstractAsyncShardingTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAsyncShardingTask.class);

    private static  final int LOAD_LEVEL_DEFAULT = 1;

    protected CuratorFramework curatorFramework;

    protected ExecutorService executorService;

    protected NamespaceShardingService namespaceShardingService;

    protected NamespaceShardingContentService namespaceShardingContentService;

    protected ReportAlarmService reportAlarmService;

    public AbstractAsyncShardingTask(NamespaceShardingService namespaceShardingService) {
        this.namespaceShardingService = namespaceShardingService;
        this.curatorFramework = namespaceShardingService.getCuratorFramework();
        this.executorService = namespaceShardingService.getExecutorService();
        this.namespaceShardingContentService = namespaceShardingService.getNamespaceShardingContentService();
        reportAlarmService = namespaceShardingService.getReportAlarmService();
    }

    protected abstract void logStartInfo();

    /**
     * 需要先通知的特殊启用作业，不考虑是否更改了其分片。
     * 默认情况下，通知启用其分片已更改的作业。
     */
    protected List<String> notifyEnableJobPrior(){
        return null;
    }



    @Override
    public void run() {
        logStartInfo();
        boolean isAllShardingTask = this instanceof ExecuteAllShardingTask;

        try{
            //如果当前变成非leader，则直接返回
            if (!namespaceShardingService.isLeadershipOnly()){
                return;
            }
            //如果需要全量分片，当前线程不是全量分片线程，则直接返回
            if (namespaceShardingService.isNeedAllSharding() && !isAllShardingTask){
                LOGGER.info("the {} will be ignored, because there will be {}", this.getClass().getSimpleName(),
                        ExecuteAllShardingTask.class.getSimpleName());
                return;
            }
            List<String> allJobs = getAllJobs();
            List<String> allEnableJobs = getAllEnableJobs(allJobs);
            List<Executor> oldOnlineExecutorList = getLastOnlineExecutorList();
            List<Executor> customLastOnlineExecutorList = customLastOnlineExecutorList();
            List<Executor> lastOnlineExecutorList = customLastOnlineExecutorList == null
                    ? copyOnlineExecutorList(oldOnlineExecutorList) : customLastOnlineExecutorList;
            List<Executor> lastOnlineTrafficExecutorList = getTrafficExecutorList(lastOnlineExecutorList);

            List<Shard> shardList = Lists.newArrayList();
            //摘取
            if (pick(allJobs , allEnableJobs , shardList , lastOnlineExecutorList , lastOnlineTrafficExecutorList)){
                //放回
                putBackBalancing(allEnableJobs, shardList, lastOnlineExecutorList, lastOnlineTrafficExecutorList);
                // 如果当前变为非leader，则返回
                if (!namespaceShardingService.isLeadershipOnly()) {
                    return;
                }
                // 持久化分片结果
                if (shardingContentIsChanged(oldOnlineExecutorList, lastOnlineExecutorList)) {
                    namespaceShardingContentService.persistDirectly(lastOnlineExecutorList);
                }
                // notify the shards-changed jobs of all enable jobs.
                Map<String, Map<String, List<Integer>>> enabledAndShardsChangedJobShardContent = getEnabledAndShardsChangedJobShardContent(
                        isAllShardingTask, allEnableJobs, oldOnlineExecutorList, lastOnlineExecutorList);
                namespaceShardingContentService
                        .persistJobsNecessaryInTransaction(enabledAndShardsChangedJobShardContent);
                // sharding count ++
                increaseShardingCount();
            }
        } catch (InterruptedException e) {
            LOGGER.info("{}-{} {} is interrupted", namespaceShardingService.getNamespace(),
                    namespaceShardingService.getHostValue(), this.getClass().getSimpleName());
            Thread.currentThread().interrupt();
        }catch (Exception t){
            LOGGER.error(t.getMessage(), t);
            if (!isAllShardingTask) { // 如果当前不是全量分片，则需要全量分片来拯救异常
                namespaceShardingService.setNeedAllSharding(true);
                namespaceShardingService.shardingCountIncrementAndGet();
                executorService.submit(new ExecuteAllShardingTask(namespaceShardingService));
            } else { // 如果当前是全量分片，则告警并关闭当前服务，重选leader来做事情
                raiseAlarm();
                shutdownNamespaceShardingService();
            }
        }finally {
            if (isAllShardingTask) { // 如果是全量分片，不再进行全量分片
                namespaceShardingService.setNeedAllSharding(false);
            }
            namespaceShardingService.shardingCountDecrementAndGet();
        }

    }

    /**
     * 唤醒那些分片信息方式修改的任务
     *  key 作业名
     *      key 执行器
     *      value 执行器下的分片集合
     */
    private Map<String, Map<String, List<Integer>>> getEnabledAndShardsChangedJobShardContent(boolean isAllShardingTask, List<String> allEnableJobs, List<Executor> oldOnlineExecutorList, List<Executor> lastOnlineExecutorList) {

        Map<String, Map<String, List<Integer>>> jobShardContent = new HashMap<>();
        if (isAllShardingTask){
            for (String enableJob : allEnableJobs){
                Map<String, List<Integer>> lastShardingItems = namespaceShardingContentService.getShardingItems(lastOnlineExecutorList , enableJob);
                jobShardContent.put(enableJob , lastShardingItems);
            }
            return jobShardContent;
        }
        List<String> enableJobsPrior = notifyEnableJobPrior();
        for (String enableJob :    allEnableJobs) {
            Map<String, List<Integer>> lastShardingItems = namespaceShardingContentService
                    .getShardingItems(lastOnlineExecutorList, enableJob);
            if (enableJobsPrior != null && enableJobsPrior.contains(enableJob)){
                jobShardContent.put(enableJob , lastShardingItems);
                continue;
            }
            Map<String, List<Integer>> oldShardingItems = namespaceShardingContentService
                    .getShardingItems(oldOnlineExecutorList, enableJob);
            boolean isChanged = hasShardChanged(oldShardingItems , lastShardingItems) || hasShardChanged(  lastShardingItems , oldShardingItems);

            if (isChanged) {
                jobShardContent.put(enableJob, lastShardingItems);
            }
        }
        return jobShardContent;
    }
    private boolean hasShardChanged(Map<String, List<Integer>> oldShardingItems , Map<String, List<Integer>> lastShardingItems){
        boolean isChanged = false;
        for (Map.Entry<String , List<Integer>> entry : oldShardingItems.entrySet() ){
            String executorName = entry.getKey();
            if (!lastShardingItems.containsKey(executorName)){
                isChanged = true;
                break;
            }
            List<Integer> shards = entry.getValue();
            List<Integer> newShard = lastShardingItems.get(executorName);
            if (shards == null && newShard != null){
                isChanged = true;
                break;
            }
            if (shards == null || newShard == null){
                continue;
            }
            if (hasShardChanged(shards , newShard)){
                isChanged = true;
                break;
            }
        }
        return false;
    }

    private boolean hasShardChanged(List<Integer> shards, List<Integer> oldShard) {
            for (Integer shard : shards) {
                if (!oldShard.contains(shard)) {
                    return true;
                }
            }
            return false;
        }

    private void increaseShardingCount() throws Exception {
        Integer shardingCount = 1;
        if (null != curatorFramework.checkExists().forPath(ShrineExecutorsNode.SHARDING_CONTENTNODE_PATH)){
            byte[] shardingCountData = curatorFramework.getData().forPath(ShrineExecutorsNode.SHARDING_CONTENTNODE_PATH);
            if (shardingCountData != null) {
                try {
                    shardingCount = Integer.parseInt(new String(shardingCountData , StandardCharsets.UTF_8) ) + 1;
                } catch (NumberFormatException e) {
                    LOGGER.error("parse shardingCount error", e);
                }
            }
            curatorFramework.setData().forPath(ShrineExecutorsNode.SHARDING_COUNT_PATH,
                    shardingCount.toString().getBytes(StandardCharsets.UTF_8.name()));
        }else {
            curatorFramework.create().forPath(ShrineExecutorsNode.SHARDING_COUNT_PATH,
                    shardingCount.toString().getBytes(StandardCharsets.UTF_8.name()));
        }
    }

    private boolean shardingContentIsChanged(List<Executor> oldOnlineExecutorList,
                                             List<Executor> lastOnlineExecutorList) {
        return !namespaceShardingContentService.toShardingContent(oldOnlineExecutorList)
                .equals(namespaceShardingContentService
                        .toShardingContent(lastOnlineExecutorList));
    }

    protected void putBackBalancing(List<String> allEnableJobs, List<Shard> shardList, List<Executor> lastOnlineExecutorList, List<Executor> lastOnlineTrafficExecutorList) throws Exception {
        if (lastOnlineExecutorList.isEmpty()) {
            LOGGER.warn("Unnecessary to put shards back to executors balanced because of no executor");
            return;
        }
        sortShardList(shardList);
        //获取所有非容器的executors
        List<Executor> notDockerExecutors = getNotDockerExecutors(lastOnlineTrafficExecutorList);

        // 获取shardList中的作业能够被接管的executors
        Map<String, List<Executor>> noDockerTrafficExecutorsMapByJob = new HashMap<>();
        Map<String, List<Executor>> lastOnlineTrafficExecutorListMapByJob = new HashMap<>();
        // 是否为本地模式作业的映射
        Map<String, Boolean> localModeMap = new HashMap<>();
        // 是否配置优先节点的作业的映射
        Map<String, Boolean> preferListIsConfiguredMap = new HashMap<>();
        // 优先节点的作业的映射
        Map<String, List<String>> preferListConfiguredMap = new HashMap<>();
        // 是否使用非优先节点的作业的映射
        Map<String, Boolean> useDispreferListMap = new HashMap<>();

        Iterator<Shard> iterator = shardList.iterator();
        while (iterator.hasNext()) {
            String jobName = iterator.next().getJobName();

            checkAndPutIntoMap(jobName,
                    filterExecutorsByJob(notDockerExecutors, jobName), noDockerTrafficExecutorsMapByJob);

            checkAndPutIntoMap(jobName, filterExecutorsByJob(lastOnlineTrafficExecutorList, jobName),
                    lastOnlineTrafficExecutorListMapByJob);

            checkAndPutIntoMap(jobName, isLocalMode(jobName), localModeMap);

            checkAndPutIntoMap(jobName, preferListIsConfigured(jobName), preferListIsConfiguredMap);

            checkAndPutIntoMap(jobName, getPreferListConfigured(jobName), preferListConfiguredMap);

            checkAndPutIntoMap(jobName, useDispreferList(jobName), useDispreferListMap);

        }

        // 整体算法放回算法：拿取Shard，放进负荷最小的executor

        // 1、放回localMode的Shard
        // 如果配置了preferList，则选取preferList中的executor。
        // 如果preferList中的executor都挂了，则不转移；否则，选取没有接管该作业的executor列表的loadLevel最小的一个。
        // 如果没有配置preferList，则选取没有接管该作业的executor列表的loadLevel最小的一个。
        putBackShardWithLocalMode(shardList, noDockerTrafficExecutorsMapByJob,
                lastOnlineTrafficExecutorListMapByJob,
                localModeMap, preferListIsConfiguredMap, preferListConfiguredMap);

        // 2、放回配置了preferList的Shard
        putBackShardWithPreferList(shardList, lastOnlineTrafficExecutorListMapByJob, preferListIsConfiguredMap,
                preferListConfiguredMap, useDispreferListMap);

        // 3、放回没有配置preferList的Shard
        putBackShardWithoutPreferlist(shardList, noDockerTrafficExecutorsMapByJob);
    }
    private void putBackShardWithoutPreferlist(List<Shard> shardList,
                                               Map<String, List<Executor>> noDockerTrafficExecutorsMapByJob) {
        Iterator<Shard> iterator = shardList.iterator();
        while (iterator.hasNext()) {
            Shard shard = iterator.next();
            Executor executor = getExecutorWithMinLoadLevel(
                    noDockerTrafficExecutorsMapByJob.get(shard.getJobName()));
            putShardIntoExecutor(shard, executor);
            iterator.remove();
        }
    }
    private Executor getExecutorWithMinLoadLevel(List<Executor> executorList) {
        Executor minLoadLevelExecutor = null;
        for (int i = 0; i < executorList.size(); i++) {
            Executor executor = executorList.get(i);
            if (minLoadLevelExecutor == null
                    || minLoadLevelExecutor.getTotalLoadLevel() > executor.getTotalLoadLevel()) {
                minLoadLevelExecutor = executor;
            }
        }
        return minLoadLevelExecutor;
    }

    private void putBackShardWithPreferList(List<Shard> shardList,
                                            Map<String, List<Executor>> lastOnlineTrafficExecutorListMapByJob,
                                            Map<String, Boolean> preferListIsConfiguredMap, Map<String, List<String>> preferListConfiguredMap,
                                            Map<String, Boolean> useDispreferListMap) {
        Iterator<Shard> iterator = shardList.iterator();
        while (iterator.hasNext()) {
            Shard shard = iterator.next();
            String jobName = shard.getJobName();
            if (preferListIsConfiguredMap.get(jobName)) { // fix,
                // preferList为空不能作为判断是否配置preferList的依据，比如说配置了容器资源，但是全部下线了。
                List<String> preferList = preferListConfiguredMap.get(jobName);
                List<Executor> preferExecutorList = getPreferExecutors(
                        lastOnlineTrafficExecutorListMapByJob, jobName, preferList);
                // 如果preferList的Executor都offline，则放回到全部online的Executor中某一个。如果是这种情况，则后续再操作，避免不均衡的情况
                // 如果存在preferExecutor，择优放回
                if (!preferExecutorList.isEmpty()) {
                    Executor executor = getExecutorWithMinLoadLevel(preferExecutorList);
                    putShardIntoExecutor(shard, executor);
                    iterator.remove();
                } else { // 如果不存在preferExecutor
                    // 如果“只使用preferExecutor”，则丢弃；否则，等到后续（在第3步）进行放回操作，避免不均衡的情况
                    if (!useDispreferListMap.get(jobName)) {
                        iterator.remove();
                    }
                }
            }
        }
    }

    private void putBackShardWithLocalMode(List<Shard> shardList, Map<String, List<Executor>> noDockerTrafficExecutorsMapByJob, Map<String, List<Executor>> lastOnlineTrafficExecutorListMapByJob, Map<String, Boolean> localModeMap, Map<String, Boolean> preferListIsConfiguredMap, Map<String, List<String>> preferListConfiguredMap) {
        for (Shard shard : shardList){
            String jobName = shard.getJobName();
            if(!localModeMap.get(jobName)){
                continue;
            }
            if (preferListIsConfiguredMap.get(jobName)) {

                List<String> preferListConfigured = preferListConfiguredMap.get(jobName);
                if (CollectionUtils.isEmpty(preferListConfigured)) {
                    continue;
                }
                List<Executor> preferExecutorList = getPreferExecutors(lastOnlineTrafficExecutorListMapByJob,
                        jobName, preferListConfigured);
                if (!preferExecutorList.isEmpty()) {
                    Executor executor = getExecutorWithMinLoadLevelAndNoThisJob(preferExecutorList,
                            jobName);
                    putShardIntoExecutor(shard, executor);
                }
            }else {
                Executor executor = getExecutorWithMinLoadLevelAndNoThisJob(
                        noDockerTrafficExecutorsMapByJob.get(jobName),
                        jobName);
                putShardIntoExecutor(shard, executor);
            }

        }
    }

    private void putShardIntoExecutor(Shard shard, Executor executor) {
        if (executor != null) {
            if (isIn(shard, executor.getShardList())) {
                LOGGER.error("The shard({}-{}) is running in the executor of {}, cannot be put again",
                        shard.getJobName(), shard.getItem(), executor.getExecutorName());
            } else {
                executor.getShardList().add(shard);
                executor.setTotalLoadLevel(executor.getTotalLoadLevel() + shard.getLoadLevel());
            }
        } else {
            LOGGER.info("No executor to take over the shard: {}-{}", shard.getJobName(), shard.getItem());
        }
    }

    protected boolean isIn(Shard shard, List<Shard> shardList) {
        for (int i = 0; i < shardList.size(); i++) {
            Shard tmp = shardList.get(i);
            if (tmp.getJobName().equals(shard.getJobName()) && tmp.getItem() == shard.getItem()) {
                return true;
            }
        }
        return false;
    }

    private Executor getExecutorWithMinLoadLevelAndNoThisJob(List<Executor> executorList, String jobName) {
        Executor minLoadLevelExecutor = null;
        for (int i = 0; i < executorList.size(); i++) {
            Executor executor = executorList.get(i);
            List<Shard> shardList = executor.getShardList();
            boolean containThisJob = false;
            for (int j = 0; j < shardList.size(); j++) {
                Shard shard = shardList.get(j);
                if (shard.getJobName().equals(jobName)) {
                    containThisJob = true;
                    break;
                }
            }
            if (!containThisJob && (minLoadLevelExecutor == null
                    || minLoadLevelExecutor.getTotalLoadLevel() > executor.getTotalLoadLevel())) {
                minLoadLevelExecutor = executor;
            }
        }
        return minLoadLevelExecutor;
    }

    private List<Executor> getPreferExecutors(Map<String, List<Executor>> lastOnlineTrafficExecutorListMapByJob, String jobName, List<String> preferListConfigured) {
        List<Executor> preferExecutorList = new ArrayList<>();
        List<Executor> lastOnlineTrafficExecutorListByJob = lastOnlineTrafficExecutorListMapByJob
                .get(jobName);
        for (int i = 0; i < lastOnlineTrafficExecutorListByJob.size(); i++) {
            Executor executor = lastOnlineTrafficExecutorListByJob.get(i);
            if (preferListConfigured.contains(executor.getExecutorName())) {
                preferExecutorList.add(executor);
            }
        }
        return preferExecutorList;
    }

    private<T> void checkAndPutIntoMap(String key , T value, Map<String, T> targetMap) {
        targetMap.putIfAbsent(key , value);
    }

    /**
     * 是否使用非preferList:
     * 1、存在结点，并且该结点值为false，返回false；
     * 2、其他情况，返回true
     */
    protected boolean useDispreferList(String jobName) throws Exception {
        String jobConfigUseDispreferListNodePath = ShrineExecutorsNode
                .getJobConfigUseDispreferListNodePath(jobName);
        if (curatorFramework.checkExists().forPath(jobConfigUseDispreferListNodePath)
                != null) {
            byte[] useDispreferListData = curatorFramework.getData()
                    .forPath(jobConfigUseDispreferListNodePath);
            if (useDispreferListData != null && !Boolean
                    .parseBoolean(new String(useDispreferListData, StandardCharsets.UTF_8.name()))) {
                return false;
            }
        }
        return true;
    }

    private List<Executor> filterExecutorsByJob(List<Executor> executorList, String jobName) {
        List<Executor> executors = Lists.newArrayList();
        for (Executor executor : executorList ) {
            List<String> jobNameList = executor.getJobNameList();
            if (jobNameList != null && jobNameList.contains(jobName)){
                executors.add(executor);
            }
        }
        return executors;
    }

    private List<Executor> getNotDockerExecutors(List<Executor> lastOnlineTrafficExecutorList) throws Exception {
        if (NamespaceShardingService.CONTAINER_ALIGN_WITH_PHYSICAL){
            return lastOnlineTrafficExecutorList;
        }
        List<Executor> notDockerExecutors = Lists.newArrayList();

        for (Executor executor : lastOnlineTrafficExecutorList){
            String executorName = executor.getExecutorName();
            if (curatorFramework.checkExists().forPath(ShrineExecutorsNode.getExecutorTaskNodePath(executorName)) == null){
                notDockerExecutors.add(executor);
            }
        }
        return notDockerExecutors;
    }

    /**
     * 按照loadLevel降序排序，如果loadLevel相同，按照作业名降序排序
     */
    protected void sortShardList(List<Shard> shardList) {
        Collections.sort(shardList, new Comparator<Shard>() {
            @Override
            public int compare(Shard o1, Shard o2) {
                int loadLevelSub = o2.getLoadLevel() - o1.getLoadLevel();
                return loadLevelSub == 0 ? o2.getJobName().compareTo(o1.getJobName()) : loadLevelSub;
            }
        });
    }
    private void raiseAlarm() {
        if (reportAlarmService != null) {
            try {
                reportAlarmService.allShardingError(namespaceShardingService.getNamespace(),
                        namespaceShardingService.getHostValue());
            } catch (Throwable t) {
                LOGGER.error(t.getMessage(), t);
            }
        }
    }

    /**
     * 摘取
     *
     * @param allJobs 该域下所有作业
     * @param allEnableJobs 该域下所有启用的作业
     * @param shardList 默认为空集合
     * @param lastOnlineExecutorList 默认为当前存储的数据，如果不想使用存储数据，请重写{@link #customLastOnlineExecutorList()}}方法
     * @param lastOnlineTrafficExecutorList lastOnlineExecutorList中所有noTraffic为false的Executor，注意Executor是同一个对象
     * @return true摘取成功；false摘取失败，不需要继续下面的逻辑
     */
    protected abstract boolean pick(List<String> allJobs, List<String> allEnableJobs, List<Shard> shardList,
                                    List<Executor> lastOnlineExecutorList, List<Executor> lastOnlineTrafficExecutorList) throws Exception;
    private List<Executor> getTrafficExecutorList(List<Executor> executorList) {
        List<Executor> trafficExecutorList = new ArrayList<>();
        for (Executor executor : executorList) {
            if (!executor.isNoTraffic()) {
                trafficExecutorList.add(executor);
            }
        }
        return trafficExecutorList;
    }
    private List<Executor> copyOnlineExecutorList(List<Executor> oldOnlineExecutorList) {
        List<Executor> newOnlineExecutorList = new ArrayList<>();
        for (Executor oldExecutor : oldOnlineExecutorList) {
            Executor newExecutor = new Executor();
            newExecutor.setTotalLoadLevel(oldExecutor.getTotalLoadLevel());
            newExecutor.setIp(oldExecutor.getIp());
            newExecutor.setNoTraffic(oldExecutor.isNoTraffic());
            newExecutor.setExecutorName(oldExecutor.getExecutorName());
            if (oldExecutor.getJobNameList() != null) {
                newExecutor.setJobNameList(new ArrayList<String>());
                for (String jobName : oldExecutor.getJobNameList()) {
                    newExecutor.getJobNameList().add(jobName);
                }
            }
            if (oldExecutor.getShardList() != null) {
                newExecutor.setShardList(new ArrayList<Shard>());
                for (Shard oldShard : oldExecutor.getShardList()) {
                    Shard newShard = new Shard();
                    newShard.setItem(oldShard.getItem());
                    newShard.setJobName(oldShard.getJobName());
                    newShard.setLoadLevel(oldShard.getLoadLevel());
                    newExecutor.getShardList().add(newShard);
                }
            }
            newOnlineExecutorList.add(newExecutor);
        }
        return newOnlineExecutorList;
    }
    protected List<Executor> customLastOnlineExecutorList() throws Exception {
        return null;
    }
    /**
     * 获取Executor集合，默认从sharding/content获取
     */
    private List<Executor> getLastOnlineExecutorList() throws Exception {
        return namespaceShardingContentService.getExecutorList();
    }

    /**
     * 修正lastOnlineExecutorList中的jobNameList
     */
    protected boolean fixJobNameList(List<Executor> lastOnlineExecutorList, String jobName) throws Exception {
        boolean fixed = false;
        for (int i = 0; i < lastOnlineExecutorList.size(); i++) {
            Executor executor = lastOnlineExecutorList.get(i);
            if (executor.getJobNameList() == null) {
                executor.setJobNameList(new ArrayList<String>());
            }
            List<String> jobNameList = executor.getJobNameList();
            String jobServersExecutorStatusNodePath = ShrineExecutorsNode
                    .getJobServersExecutorStatusNodePath(jobName, executor.getExecutorName());
            if (curatorFramework.checkExists().forPath(jobServersExecutorStatusNodePath)
                    != null) {
                if (!jobNameList.contains(jobName)) {
                    jobNameList.add(jobName);
                    fixed = true;
                }
            } else {
                if (jobNameList.contains(jobName)) {
                    jobNameList.remove(jobName);
                    fixed = true;
                }
            }
        }
        return fixed;
    }

    /**
     * 获取该域下的所有enable的作业
     */
    private List<String> getAllEnableJobs(List<String> allJobs) throws Exception {
        List<String> allEnableJob = Lists.newArrayList();
        for (String job : allJobs) {
            if (curatorFramework.checkExists()
                    .forPath(ShrineExecutorsNode.getJobConfigEnableNodePath(job)) != null) {
                byte[] enableData = curatorFramework.getData()
                        .forPath(ShrineExecutorsNode.getJobConfigEnableNodePath(job));
                if (enableData != null && Boolean.parseBoolean(new String(enableData, StandardCharsets.UTF_8.name()))) {
                    allEnableJob.add(job);
                }
            }
        }
        return allEnableJob;
    }

    /**
     * 获取这个namespace下的所有作业
     */
    private List<String> getAllJobs() throws Exception {
        List<String> allJob = new ArrayList<>();
        if (curatorFramework.checkExists().forPath(ShrineExecutorsNode.JOBSNODE_PATH)
                == null) {
            curatorFramework.create().creatingParentsIfNeeded()
                    .forPath(ShrineExecutorsNode.JOBSNODE_PATH);
        }
        List<String> tmp = curatorFramework.getChildren()
                .forPath(ShrineExecutorsNode.JOBSNODE_PATH);
        if (tmp != null) {
            allJob.addAll(tmp);
        }
        return allJob;
    }

    private void shutdownNamespaceShardingService(){
        try {
            namespaceShardingService.shutdownInner(false);
        } catch (InterruptedException e) {
            LOGGER.info("{}-{} {}-shutdownInner is interrupted", namespaceShardingService.getNamespace(),
                    namespaceShardingService.getHostValue(),
                    this.getClass().getSimpleName());
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
        }
    }

    /**
     * 创建分片信息
     * @param jobName 作业名
     * @param lastOnlineExecutorList 活跃的执行器
     */
    protected List<Shard> createShards(String jobName , List<Executor> lastOnlineExecutorList) throws Exception {
        List<Shard> shardList = Lists.newArrayList();
        boolean preferListIsConfigured = preferListIsConfigured(jobName);
        List<String> preferListConfigured = getPreferListConfigured(jobName);
        List<Executor> preferListOnlineByJob = getPreferListOnlineByJob(jobName , preferListConfigured ,lastOnlineExecutorList);
        boolean localMode = isLocalMode(jobName);
        int shardingTotalCount = getShardingTotalCount(jobName);
        int loadLevel = getLoadLevel(jobName);
        if (localMode){
            if (preferListIsConfigured){
                //如果当前作业存在优先节点，则新建在线的优先节点的数量的分片
                if (!preferListOnlineByJob.isEmpty()){
                    shardList.addAll(createShards(jobName , preferListOnlineByJob.size() , loadLevel ));
                }
            }else {
                // 新建在线的executor的数量的分片
                shardList.addAll(createShards(jobName, lastOnlineExecutorList.size(), loadLevel));
            }
        }else {
            // 新建shardingTotalCount数量的分片
            shardList.addAll(createShards(jobName, shardingTotalCount, loadLevel));

        }
        return shardList;
    }

    private List<Shard> createShards(String jobName, int number, int loadLevel) {
        List<Shard> shards = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            Shard shard = new Shard();
            shard.setJobName(jobName);
            shard.setItem(i);
            shard.setLoadLevel(loadLevel);
            shards.add(shard);
        }
        return shards;
    }
    /**
     * 获取作业的运行级别
     * @param jobName 作业名
     */
    protected int getLoadLevel(String jobName) throws Exception {
        int loadLevel = LOAD_LEVEL_DEFAULT;
        String jobConfigLoadLevelNodePath = ShrineExecutorsNode.getJobConfigLoadLevelNodePath(jobName);
        if (curatorFramework.checkExists().forPath(jobConfigLoadLevelNodePath) != null) {
            byte[] loadLevelData = curatorFramework.getData()
                    .forPath(jobConfigLoadLevelNodePath);
            try {
                if (loadLevelData != null) {
                    loadLevel = Integer.parseInt(new String(loadLevelData, StandardCharsets.UTF_8.name()));
                }
            } catch (NumberFormatException e) {
                LOGGER.error("parse loadLevel error, will use the default value", e);
            }
        }
        return loadLevel;
    }

    /**
     * 获取作业的分片总数
     * @param jobName 作业名
     */
    private int getShardingTotalCount(String jobName) throws Exception {
        int shardingTotalCount = 0;
        String jobConfigShardingTotalCountNodePath = ShrineExecutorsNode
                .getJobConfigShardingTotalCountNodePath(jobName);
        if (curatorFramework.checkExists().forPath(jobConfigShardingTotalCountNodePath)
                != null) {
            byte[] shardingTotalCountData = curatorFramework.getData()
                    .forPath(jobConfigShardingTotalCountNodePath);
            if (shardingTotalCountData != null) {
                try {
                    shardingTotalCount = Integer
                            .parseInt(new String(shardingTotalCountData, StandardCharsets.UTF_8.name()));
                } catch (NumberFormatException e) {
                    LOGGER .error("parse shardingTotalCount error, will use the default value", e);
                }
            }
        }
        return shardingTotalCount;
    }

    /**
     * 判断作业是不是本地模式
     * @param jobName 作业名
     * @return
     */
    private boolean isLocalMode(String jobName) throws Exception {
        String localNodePath = ShrineExecutorsNode.getJobConfigLocalModeNodePath(jobName);
        if (curatorFramework.checkExists().forPath(localNodePath) != null) {
            byte[] data = curatorFramework.getData().forPath(localNodePath);
            if (data != null) {
                return Boolean.parseBoolean(new String(data, StandardCharsets.UTF_8.name()));
            }
        }
        return false;
    }

    private List<Executor> getPreferListOnlineByJob(String jobName, List<String> preferListConfigured, List<Executor> lastOnlineExecutorList) {
        List<Executor> preferListOnlineByJob = new ArrayList<>();
        for (int i = 0; i < lastOnlineExecutorList.size(); i++) {
            Executor executor = lastOnlineExecutorList.get(i);
            if (preferListConfigured.contains(executor.getExecutorName())
                    && executor.getJobNameList().contains(jobName)) {
                preferListOnlineByJob.add(executor);
            }
        }
        return preferListOnlineByJob;

    }

    /**
     * 获取这个作业配置的优先执行节点的列表，即使配置的executor不存在，也返回
     * @param jobName 作业名
     */
    private List<String> getPreferListConfigured(String jobName) throws Exception {
        List<String> preferList = Lists.newLinkedList();
        if (curatorFramework.checkExists().forPath(ShrineExecutorsNode.getJobConfigPreferListNodePath(jobName)) != null){
            byte[] preferListData = curatorFramework.getData().forPath(ShrineExecutorsNode.getJobConfigPreferListNodePath(jobName));
            if (preferListData != null){
                List<String> allExistsExecutors = getAllExistingExecutors();
                String[] split = new String(preferListData, StandardCharsets.UTF_8.name()).split(",");
                for (String tmp : split) {
                    String tmpTrim = tmp.trim();
                    if (!"".equals(tmpTrim)) {
                        fillRealPreferListIfIsDockerOrNot(preferList, tmpTrim, allExistsExecutors);
                    }
                }
            }
        }
        return preferList;
    }
    /**
     * 如果prefer不是docker容器，并且preferList不包含，则直接添加；
     *
     * 如果prefer是docker容器（以@开头），则prefer为task，获取该task下的所有executor，如果不包含，添加进preferList。
     */
    private void fillRealPreferListIfIsDockerOrNot(List<String> preferList, String prefer,
                                                   List<String> allExistsExecutors) throws Exception {
        if (!prefer.startsWith("@")) { // not docker server
            if (!preferList.contains(prefer)) {
                preferList.add(prefer);
            }
            return;
        }

        String task = prefer.substring(1);
        for (int i = 0; i < allExistsExecutors.size(); i++) {
            String executor = allExistsExecutors.get(i);
            if (curatorFramework.checkExists()
                    .forPath(ShrineExecutorsNode.getExecutorTaskNodePath(executor)) != null) {
                byte[] taskData = curatorFramework.getData()
                        .forPath(ShrineExecutorsNode.getExecutorTaskNodePath(executor));
                if (taskData != null && task.equals(new String(taskData, StandardCharsets.UTF_8.name())) && !preferList
                        .contains(executor)) {
                    preferList.add(executor);
                }
            }
        }
    }

    /**
     * 获取所有存在Executor
     */
    private List<String> getAllExistingExecutors() throws Exception {
        List<String> allExistsExecutors = new ArrayList<>();
        if (curatorFramework.checkExists().forPath(ShrineExecutorsNode.getExecutorsNodePath())
                != null) {
            List<String> executors = curatorFramework.getChildren()
                    .forPath(ShrineExecutorsNode.getExecutorsNodePath());
            if (executors != null) {
                allExistsExecutors.addAll(executors);
            }
        }
        return allExistsExecutors;
    }


    /**
     * 判断这个作业是否配置优先执行列表
     * @param jobName 作业名
     */
    protected  boolean preferListIsConfigured(String jobName) throws Exception {
        if (curatorFramework.checkExists().forPath(ShrineExecutorsNode.getJobConfigPreferListNodePath(jobName)) != null){
                byte[] preferListData = curatorFramework.getData().forPath(ShrineExecutorsNode.getJobConfigPreferListNodePath(jobName));
                if (preferListData == null){
                    return new String(preferListData , StandardCharsets.UTF_8).length() > 0;
                }
        }
        return false;
    }

    /**
     * 判断这个executor是否摘机
     */
    protected boolean getExecutorNoTraffic(String executorName) throws Exception {
        return curatorFramework.checkExists().forPath(ShrineExecutorsNode.getExecutorNoTrafficNodePath(executorName)) != null;
    }
}
