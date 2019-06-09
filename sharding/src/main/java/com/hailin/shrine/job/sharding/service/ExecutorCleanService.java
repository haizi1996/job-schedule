package com.hailin.shrine.job.sharding.service;

import com.hailin.shrine.job.integrate.entity.JobConfigInfo;
import com.hailin.shrine.job.integrate.service.UpdateJobConfigService;
import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import com.hailin.shrine.job.sharding.utils.CuratorUtils;
import lombok.AllArgsConstructor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class ExecutorCleanService {
    private static final Logger log = LoggerFactory.getLogger(ExecutorCleanService.class);

    private CuratorFramework curatorFramework;

    private UpdateJobConfigService updateJobConfigService;

    /**
     * delete $SaturnExecutors/executors/xxx<br> delete $Jobs/job/servers/xxx<br> delete $Jobs/job/config/preferList
     * content about xxx
     */
    public void clean(String executorName) {
        List<JobConfigInfo> jobConfigInfos = new ArrayList<>();
        try {
            String cleanNodePath = ShrineExecutorsNode.getExecutorCleanNodePath(executorName);
            byte[] cleanNodeBytes = curatorFramework.getData().forPath(cleanNodePath);
            if (cleanNodeBytes == null || cleanNodeBytes.length == 0) {
                return;
            }

            String cleanNodeData = new String(cleanNodeBytes, "UTF-8");
            if (!Boolean.parseBoolean(cleanNodeData)) {
                return;
            }

            if (curatorFramework.checkExists()
                    .forPath(ShrineExecutorsNode.getExecutorIpNodePath(executorName)) == null) {
                log.info("Clean the executor {}", executorName);
                // delete $SaturnExecutors/executors/xxx
                deleteExecutor(executorName);
                List<String> jobs = getJobList();
                for (String jobName : jobs) {
                    // delete $Jobs/job/servers/xxx
                    deleteJobServerExecutor(jobName, executorName);
                    // delete $Jobs/job/config/preferList content about xxx
                    String preferList = updateJobConfigPreferListContentToRemoveDeletedExecutor(jobName,
                            executorName);
                    if (preferList != null) {
                        JobConfigInfo jobConfigInfo = new JobConfigInfo(curatorFramework.getNamespace(),
                                jobName, preferList);
                        jobConfigInfos.add(jobConfigInfo);
                    }
                }
            } else {
                log.info("The executor {} is online now, no necessary to clean", executorName);
            }
        } catch (KeeperException.NoNodeException e) {
            log.debug("No clean node found for executor:" + executorName, e);
        } catch (Exception e) {
            log.error("Clean the executor " + executorName + " error", e);
        } finally {
            updatePreferListQuietly(jobConfigInfos);
        }
    }

    /**
     * delete $Jobs/job/servers/xxx
     */
    private void deleteJobServerExecutor(String jobName, String executorName)
            throws KeeperException.ConnectionLossException, InterruptedException {
        try {
            String jobServersExecutorNodePath = ShrineExecutorsNode.getJobServersExecutorNodePath(jobName,
                    executorName);
            CuratorUtils.deletingChildrenIfNeeded(curatorFramework, jobServersExecutorNodePath);
        } catch (KeeperException.ConnectionLossException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            log.error("Clean the executor, deleteJobServerExecutor(" + jobName + ", " + executorName + ") error", e);
        }
    }
    private void updatePreferListQuietly(List<JobConfigInfo> jobConfigInfos) {
        try {
            if (updateJobConfigService != null) {
                updateJobConfigService.batchUpdatePreferList(jobConfigInfos);
            }
        } catch (Exception e) {
            log.warn("batchUpdatePreferList  error", e); // just log a warn.
        }
    }
    private List<String> getJobList() throws KeeperException.ConnectionLossException, InterruptedException {
        List<String> jobList = new ArrayList<>();
        try {
            String jobsNodePath = ShrineExecutorsNode.JOBSNODE_PATH;
            if (curatorFramework.checkExists().forPath(jobsNodePath) != null) {
                List<String> tmp = curatorFramework.getChildren().forPath(jobsNodePath);
                if (tmp != null && !tmp.isEmpty()) {
                    jobList.addAll(tmp);
                }
            }
        } catch (KeeperException.NoNodeException e) { // NOSONAR
            // ignore
        } catch (KeeperException.ConnectionLossException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            log.error("Clean the executor, getJobList error", e);
        }
        return jobList;
    }
    /**
     * delete $SaturnExecutors/executors/xxx
     */
    private void deleteExecutor(String executorName)
            throws KeeperException.ConnectionLossException, InterruptedException {
        try {
            String executorNodePath = ShrineExecutorsNode.getExecutorNodePath(executorName);
            CuratorUtils.deletingChildrenIfNeeded(curatorFramework, executorNodePath);
        } catch (KeeperException.ConnectionLossException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            log.error("Clean the executor, deleteExecutor(" + executorName + ") error", e);
        }
    }
    /**
     * delete $Jobs/job/config/preferList content about xxx
     */
    private String updateJobConfigPreferListContentToRemoveDeletedExecutor(String jobName, String executorName)
            throws KeeperException.ConnectionLossException, InterruptedException {
        try {
            String jobConfigPreferListNodePath = ShrineExecutorsNode.getJobConfigPreferListNodePath(jobName);
            Stat stat = new Stat();
            byte[] jobConfigPreferListNodeBytes = curatorFramework.getData().storingStatIn(stat)
                    .forPath(jobConfigPreferListNodePath);

            if (jobConfigPreferListNodeBytes == null || jobConfigPreferListNodeBytes.length == 0) {
                return null;
            }

            // build the new prefer list string
            StringBuilder sb = new StringBuilder();
            String[] split = new String(jobConfigPreferListNodeBytes, "UTF-8").split(",");
            boolean found = false;
            for (String tmp : split) {
                String tmpTrim = tmp.trim();
                if (!tmpTrim.equals(executorName)) {
                    if (sb.length() > 0) {
                        sb.append(',');
                    }
                    sb.append(tmpTrim);
                } else {
                    found = true;
                }
            }
            curatorFramework.setData().withVersion(stat.getVersion()).forPath(jobConfigPreferListNodePath,
                    sb.toString().getBytes("UTF-8"));
            return found ? sb.toString() : null;
        } catch (KeeperException.NoNodeException | KeeperException.BadVersionException e) { // NOSONAR
            // ignore
        } catch (KeeperException.ConnectionLossException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            log.error("Clean the executor, updateJobConfigPreferListContentToRemoveDeletedExecutor(" + jobName + ", "
                    + executorName + ") error", e);
        }
        return null;
    }

}
