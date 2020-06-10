package com.hailin.shrine.job.common.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SystemEnvProperties {

	private static final Logger LOGGER = LoggerFactory.getLogger(SystemEnvProperties.class);

	private static final String NAME_SCHEDULE_MAX_NUMBER_OF_JOBS = "SCHEDULE_MAX_NUMBER_OF_JOBS";
	/**
	 * 每个域最大作业数量
	 */
	public static int SCHEDULE_MAX_NUMBER_OF_JOBS = 500;

	private static final String NAME_SCHEDULE_EXECUTOR_CLEAN = "SCHEDULE_EXECUTOR_CLEAN";
	/**
	 * Executor离线时，其zk节点信息是否被清理
	 */
	public static boolean SCHEDULE_EXECUTOR_CLEAN = Boolean.parseBoolean(
			System.getProperty(NAME_SCHEDULE_EXECUTOR_CLEAN, System.getenv(NAME_SCHEDULE_EXECUTOR_CLEAN)));

	/**
	 * <pre>
	 * shell作业的结果回写的文件全路径（如果需要返回一些执行结果，只需要将结果写入该文件），JSON结构:
	 *  {
	 *    returnMsg: 返回消息内容
	 *    errorGroup: 200=SUCCESS, 500/550: error
	 *    returnCode: 自定义返回码,
	 *    prop: {k:v} 属性对
	 *  }
	 * </pre>
	 */
	public static final String NAME_SCHEDULE_OUTPUT_PATH = "SCHEDULE_OUTPUT_PATH";

	private static final String NAME_SCHEDULE_DCOS_TASK = "SCHEDULE_DCOS_TASK";
	private static final String NAME_SCHEDULE_K8S_DEPLOYMENT = "SCHEDULE_K8S_DEPLOYMENT";
	public static String SCHEDULE_CONTAINER_DEPLOYMENT_ID;

	/**
	 * Executor优雅退出的全局默认超时时间（单位：精确到秒，默认1分钟）
	 */
	public static int SCHEDULE_SHUTDOWN_TIMEOUT = 60;
	public static int SCHEDULE_SHUTDOWN_TIMEOUT_MAX = 5 * 60 - 10;

	/**
	 * SCHEDULE Console URI.
	 */
	public static final String NAME_SCHEDULE_CONSOLE_URI = "SCHEDULE_CONSOLE_URI";
	public static String SCHEDULE_CONSOLE_URI = trim(
			System.getProperty(NAME_SCHEDULE_CONSOLE_URI, System.getenv(NAME_SCHEDULE_CONSOLE_URI)));
	public static List<String> SCHEDULE_CONSOLE_URI_LIST = new ArrayList<>();

	private static final String NAME_SCHEDULE_SHUTDOWN_TIMEOUT = "SCHEDULE_SHUTDOWN_TIMEOUT";

	/**
	 * Executor ZK Client session timeout
	 */
	public static final String NAME_SCHEDULE_ZK_CLIENT_SESSION_TIMEOUT_IN_SECONDS = "SCHEDULE_ZK_CLIENT_SESSION_TIMEOUT";
	public static int SCHEDULE_ZK_CLIENT_SESSION_TIMEOUT_IN_SECONDS = -1;

	/**
	 * Executor ZK Client connection timeout
	 */
	public static final String NAME_SCHEDULE_ZK_CLIENT_CONNECTION_TIMEOUT_IN_SECONDS = "SCHEDULE_ZK_CLIENT_CONNECTION_TIMEOUT";
	public static int SCHEDULE_ZK_CLIENT_CONNECTION_TIMEOUT_IN_SECONDS = -1;

	/**
	 * Executor ZK Client retry times
	 */
	public static final String NAME_SCHEDULE_ZK_CLIENT_RETRY_TIMES = "SCHEDULE_ZK_CLIENT_RETRY_TIMES";
	public static int SCHEDULE_ZK_CLIENT_RETRY_TIMES = -1;


	private static final String NAME_SCHEDULE_USE_UNSTABLE_NETWORK_SETTING = "SCHEDULE_USE_UNSTABLE_NETWORK_SETTING";
	public static boolean SCHEDULE_USE_UNSTABLE_NETWORK_SETTING = false;

	// For restart and dump
	public static boolean SCHEDULE_ENABLE_EXEC_SCRIPT = Boolean.getBoolean("SCHEDULE_ENABLE_EXEC_SCRIPT");
	public static final String NAME_SCHEDULE_PRG = "SCHEDULE_PRG";
	public static String SCHEDULE_PRG = System.getProperty(NAME_SCHEDULE_PRG);
	public static final String NAME_SCHEDULE_LOG_OUTFILE = "SCHEDULE_LOG_OUTFILE";
	public static String SCHEDULE_LOG_OUTFILE = System.getProperty(NAME_SCHEDULE_LOG_OUTFILE);

	// nohup file size checking by default is 600 seconds
	public static int SCHEDULE_CHECK_NOHUPOUT_SIZE_INTERVAL_IN_SEC = 600;
	private static final String NAME_SCHEDULE_CHECK_NOHUPOUT_SIZE_INTERVAL_IN_SEC = "SCHEDULE_CHECK_NOHUPOUT_SIZE_INTERVAL";

	// nohup max file size by default is 500 MB
	public static long SCHEDULE_NOHUPOUT_SIZE_LIMIT_IN_BYTES = 500 * 1024 * 1024L;
	private static final String NAME_SCHEDULE_NOHUPOUT_SIZE_LIMIT_IN_BYTES = "SCHEDULE_NOHUPOUT_SIZE_LIMIT";

	public static int SCHEDULE_SESSION_TIMEOUT_IN_SECONDS_IN_UNSTABLE_NETWORK = 40;
	public static int SCHEDULE_CONNECTION_TIMEOUT_IN_SECONDS_IN_UNSTABLE_NETWORK = 40;
	public static int SCHEDULE_RETRY_TIMES_IN_UNSTABLE_NETWORK = 9;

	// switch for disable job init failed alarm, false by default
	private static final String NAME_SCHEDULE_DISABLE_JOB_INIT_FAILED_ALARM = "SCHEDULE_DISABLE_JOB_INIT_FAILED_ALARM";
	public static boolean SCHEDULE_DISABLE_JOB_INIT_FAILED_ALARM = Boolean.parseBoolean(
			System.getProperty(NAME_SCHEDULE_DISABLE_JOB_INIT_FAILED_ALARM,
					System.getenv(NAME_SCHEDULE_DISABLE_JOB_INIT_FAILED_ALARM)));

	// Just init the jobs that are in the groups, if the variable is configured
	public static final String NAME_SCHEDULE_INIT_JOB_BY_GROUPS = "SCHEDULE_INIT_JOB_BY_GROUPS";
	public static Set<String> SCHEDULE_INIT_JOB_BY_GROUPS = new HashSet<>();

	static {
		loadProperties();
	}

	public static void loadProperties() {
		String maxNumberOfJobs = System
				.getProperty(NAME_SCHEDULE_MAX_NUMBER_OF_JOBS, System.getenv(NAME_SCHEDULE_MAX_NUMBER_OF_JOBS));
		if (StringUtils.isNotBlank(maxNumberOfJobs)) {
			try {
				SCHEDULE_MAX_NUMBER_OF_JOBS = Integer.parseInt(maxNumberOfJobs);
			} catch (Throwable t) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, t.getMessage(), t);
			}
		}

		String shutdownTimeout = System
				.getProperty(NAME_SCHEDULE_SHUTDOWN_TIMEOUT, System.getenv(NAME_SCHEDULE_SHUTDOWN_TIMEOUT));
		if (StringUtils.isNotBlank(shutdownTimeout)) {
			try {
				SCHEDULE_SHUTDOWN_TIMEOUT = Integer.parseInt(shutdownTimeout);
			} catch (Throwable t) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, t.getMessage(), t);
			}
		}
		if (SCHEDULE_SHUTDOWN_TIMEOUT > SCHEDULE_SHUTDOWN_TIMEOUT_MAX) {
			SCHEDULE_SHUTDOWN_TIMEOUT = SCHEDULE_SHUTDOWN_TIMEOUT_MAX;
		}

		String dcosTaskId = System.getProperty(NAME_SCHEDULE_DCOS_TASK, System.getenv(NAME_SCHEDULE_DCOS_TASK));
		if (StringUtils.isNotBlank(dcosTaskId)) {
			SCHEDULE_CONTAINER_DEPLOYMENT_ID = dcosTaskId;
		} else {
			SCHEDULE_CONTAINER_DEPLOYMENT_ID = System
					.getProperty(NAME_SCHEDULE_K8S_DEPLOYMENT, System.getenv(NAME_SCHEDULE_K8S_DEPLOYMENT));
		}

		String zkClientSessionTimeoutStr = System.getProperty(NAME_SCHEDULE_ZK_CLIENT_SESSION_TIMEOUT_IN_SECONDS,
				System.getenv(NAME_SCHEDULE_ZK_CLIENT_SESSION_TIMEOUT_IN_SECONDS));
		if (StringUtils.isNotBlank(zkClientSessionTimeoutStr)) {
			try {
				SCHEDULE_ZK_CLIENT_SESSION_TIMEOUT_IN_SECONDS = Integer.parseInt(zkClientSessionTimeoutStr);
			} catch (Throwable t) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, t.getMessage(), t);
			}
		}

		String zkClientConnectionTimeoutStr = System
				.getProperty(NAME_SCHEDULE_ZK_CLIENT_CONNECTION_TIMEOUT_IN_SECONDS,
						System.getenv(NAME_SCHEDULE_ZK_CLIENT_CONNECTION_TIMEOUT_IN_SECONDS));
		if (StringUtils.isNotBlank(zkClientConnectionTimeoutStr)) {
			try {
				SCHEDULE_ZK_CLIENT_CONNECTION_TIMEOUT_IN_SECONDS = Integer.parseInt(zkClientConnectionTimeoutStr);
			} catch (Throwable t) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, t.getMessage(), t);
			}
		}

		String zkClientRetryTimes = System.getProperty(NAME_SCHEDULE_ZK_CLIENT_RETRY_TIMES,
				System.getenv(NAME_SCHEDULE_ZK_CLIENT_RETRY_TIMES));
		if (StringUtils.isNotBlank(zkClientRetryTimes)) {
			try {
				SCHEDULE_ZK_CLIENT_RETRY_TIMES = Integer.parseInt(zkClientRetryTimes);
			} catch (Throwable t) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, t.getMessage(), t);
			}
		}

		String useUnstableNetworkSettings = System.getProperty(NAME_SCHEDULE_USE_UNSTABLE_NETWORK_SETTING,
				System.getenv(NAME_SCHEDULE_USE_UNSTABLE_NETWORK_SETTING));
		if (StringUtils.isNotBlank(useUnstableNetworkSettings)) {
			SCHEDULE_USE_UNSTABLE_NETWORK_SETTING = Boolean.parseBoolean(useUnstableNetworkSettings);
		}

		String checkNohupOutSizeInterval = System.getProperty(NAME_SCHEDULE_CHECK_NOHUPOUT_SIZE_INTERVAL_IN_SEC,
				System.getenv(NAME_SCHEDULE_CHECK_NOHUPOUT_SIZE_INTERVAL_IN_SEC));
		if (StringUtils.isNotBlank(checkNohupOutSizeInterval)) {
			try {
				int interval_in_sec = Integer.parseInt(checkNohupOutSizeInterval);
				if (interval_in_sec > 0) {
					SCHEDULE_CHECK_NOHUPOUT_SIZE_INTERVAL_IN_SEC = interval_in_sec;
				}
			} catch (Throwable t) {
				LOGGER.error(  t.getMessage(), t);
			}
		}

		String noHupOutSizeLimit = System.getProperty(NAME_SCHEDULE_NOHUPOUT_SIZE_LIMIT_IN_BYTES,
				System.getenv(NAME_SCHEDULE_NOHUPOUT_SIZE_LIMIT_IN_BYTES));
		if (StringUtils.isNotBlank(noHupOutSizeLimit)) {
			try {
				long sizeLimit = Long.parseLong(noHupOutSizeLimit);
				if (sizeLimit > 0) {
					SCHEDULE_NOHUPOUT_SIZE_LIMIT_IN_BYTES = sizeLimit;
				}
			} catch (Throwable t) {
				LOGGER.error(  t.getMessage(), t);
			}
		}

		Set<String> tempSet = new HashSet<>();
		String initJobByGroups = System
				.getProperty(NAME_SCHEDULE_INIT_JOB_BY_GROUPS, System.getenv(NAME_SCHEDULE_INIT_JOB_BY_GROUPS));
		if (StringUtils.isNotBlank(initJobByGroups)) {
			try {
				String[] split = initJobByGroups.split(",");
				for (String temp : split) {
					if (StringUtils.isNotBlank(temp)) {
						tempSet.add(temp.trim());
					}
				}
			} catch (Throwable t) {
				LOGGER.error(  t.getMessage(), t);
			}
		}
		SCHEDULE_INIT_JOB_BY_GROUPS = tempSet;
		if (!SCHEDULE_INIT_JOB_BY_GROUPS.isEmpty()) {
			LOGGER.info(  "the {} is set to {}",
					NAME_SCHEDULE_INIT_JOB_BY_GROUPS, SCHEDULE_INIT_JOB_BY_GROUPS);
		}

	}

	protected static String trim(String property) {
		if (property != null && property.length() > 0) {
			return property.trim();
		}
		return property;
	}

	public static void init() {
		if (SCHEDULE_CONSOLE_URI != null) {
			String[] split = SCHEDULE_CONSOLE_URI.split(",");
			if (split != null) {
				for (String tmp : split) {
					tmp = tmp.trim();
					if (!tmp.isEmpty()) {
						SCHEDULE_CONSOLE_URI_LIST.add(tmp);
					}
				}
			}
		}
	}

}
