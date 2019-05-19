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

	private static final String NAME_SHRINE_MAX_NUMBER_OF_JOBS = "SHRINE_MAX_NUMBER_OF_JOBS";
	/**
	 * 每个域最大作业数量
	 */
	public static int SHRINE_MAX_NUMBER_OF_JOBS = 500;

	private static final String NAME_SHRINE_EXECUTOR_CLEAN = "SHRINE_EXECUTOR_CLEAN";
	/**
	 * Executor离线时，其zk节点信息是否被清理
	 */
	public static boolean SHRINE_EXECUTOR_CLEAN = Boolean.parseBoolean(
			System.getProperty(NAME_SHRINE_EXECUTOR_CLEAN, System.getenv(NAME_SHRINE_EXECUTOR_CLEAN)));

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
	public static final String NAME_SHRINE_OUTPUT_PATH = "SHRINE_OUTPUT_PATH";

	private static final String NAME_SHRINE_DCOS_TASK = "SHRINE_DCOS_TASK";
	private static final String NAME_SHRINE_K8S_DEPLOYMENT = "SHRINE_K8S_DEPLOYMENT";
	public static String SHRINE_CONTAINER_DEPLOYMENT_ID;

	/**
	 * Executor优雅退出的全局默认超时时间（单位：精确到秒，默认1分钟）
	 */
	public static int SHRINE_SHUTDOWN_TIMEOUT = 60;
	public static int SHRINE_SHUTDOWN_TIMEOUT_MAX = 5 * 60 - 10;

	/**
	 * SHRINE Console URI.
	 */
	public static final String NAME_SHRINE_CONSOLE_URI = "SHRINE_CONSOLE_URI";
	public static String SHRINE_CONSOLE_URI = trim(
			System.getProperty(NAME_SHRINE_CONSOLE_URI, System.getenv(NAME_SHRINE_CONSOLE_URI)));
	public static List<String> SHRINE_CONSOLE_URI_LIST = new ArrayList<>();

	private static final String NAME_SHRINE_SHUTDOWN_TIMEOUT = "SHRINE_SHUTDOWN_TIMEOUT";

	/**
	 * Executor ZK Client session timeout
	 */
	public static final String NAME_SHRINE_ZK_CLIENT_SESSION_TIMEOUT_IN_SECONDS = "SHRINE_ZK_CLIENT_SESSION_TIMEOUT";
	public static int SHRINE_ZK_CLIENT_SESSION_TIMEOUT_IN_SECONDS = -1;

	/**
	 * Executor ZK Client connection timeout
	 */
	public static final String NAME_SHRINE_ZK_CLIENT_CONNECTION_TIMEOUT_IN_SECONDS = "SHRINE_ZK_CLIENT_CONNECTION_TIMEOUT";
	public static int SHRINE_ZK_CLIENT_CONNECTION_TIMEOUT_IN_SECONDS = -1;

	/**
	 * Executor ZK Client retry times
	 */
	public static final String NAME_SHRINE_ZK_CLIENT_RETRY_TIMES = "SHRINE_ZK_CLIENT_RETRY_TIMES";
	public static int SHRINE_ZK_CLIENT_RETRY_TIMES = -1;


	private static final String NAME_SHRINE_USE_UNSTABLE_NETWORK_SETTING = "SHRINE_USE_UNSTABLE_NETWORK_SETTING";
	public static boolean SHRINE_USE_UNSTABLE_NETWORK_SETTING = false;

	// For restart and dump
	public static boolean SHRINE_ENABLE_EXEC_SCRIPT = Boolean.getBoolean("SHRINE_ENABLE_EXEC_SCRIPT");
	public static final String NAME_SHRINE_PRG = "SHRINE_PRG";
	public static String SHRINE_PRG = System.getProperty(NAME_SHRINE_PRG);
	public static final String NAME_SHRINE_LOG_OUTFILE = "SHRINE_LOG_OUTFILE";
	public static String SHRINE_LOG_OUTFILE = System.getProperty(NAME_SHRINE_LOG_OUTFILE);

	// nohup file size checking by default is 600 seconds
	public static int SHRINE_CHECK_NOHUPOUT_SIZE_INTERVAL_IN_SEC = 600;
	private static final String NAME_SHRINE_CHECK_NOHUPOUT_SIZE_INTERVAL_IN_SEC = "SHRINE_CHECK_NOHUPOUT_SIZE_INTERVAL";

	// nohup max file size by default is 500 MB
	public static long SHRINE_NOHUPOUT_SIZE_LIMIT_IN_BYTES = 500 * 1024 * 1024L;
	private static final String NAME_SHRINE_NOHUPOUT_SIZE_LIMIT_IN_BYTES = "SHRINE_NOHUPOUT_SIZE_LIMIT";

	public static int SHRINE_SESSION_TIMEOUT_IN_SECONDS_IN_UNSTABLE_NETWORK = 40;
	public static int SHRINE_CONNECTION_TIMEOUT_IN_SECONDS_IN_UNSTABLE_NETWORK = 40;
	public static int SHRINE_RETRY_TIMES_IN_UNSTABLE_NETWORK = 9;

	// switch for disable job init failed alarm, false by default
	private static final String NAME_SHRINE_DISABLE_JOB_INIT_FAILED_ALARM = "SHRINE_DISABLE_JOB_INIT_FAILED_ALARM";
	public static boolean SHRINE_DISABLE_JOB_INIT_FAILED_ALARM = Boolean.parseBoolean(
			System.getProperty(NAME_SHRINE_DISABLE_JOB_INIT_FAILED_ALARM,
					System.getenv(NAME_SHRINE_DISABLE_JOB_INIT_FAILED_ALARM)));

	// Just init the jobs that are in the groups, if the variable is configured
	public static final String NAME_SHRINE_INIT_JOB_BY_GROUPS = "SHRINE_INIT_JOB_BY_GROUPS";
	public static Set<String> SHRINE_INIT_JOB_BY_GROUPS = new HashSet<>();

	static {
		loadProperties();
	}

	public static void loadProperties() {
		String maxNumberOfJobs = System
				.getProperty(NAME_SHRINE_MAX_NUMBER_OF_JOBS, System.getenv(NAME_SHRINE_MAX_NUMBER_OF_JOBS));
		if (StringUtils.isNotBlank(maxNumberOfJobs)) {
			try {
				SHRINE_MAX_NUMBER_OF_JOBS = Integer.parseInt(maxNumberOfJobs);
			} catch (Throwable t) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, t.getMessage(), t);
			}
		}

		String shutdownTimeout = System
				.getProperty(NAME_SHRINE_SHUTDOWN_TIMEOUT, System.getenv(NAME_SHRINE_SHUTDOWN_TIMEOUT));
		if (StringUtils.isNotBlank(shutdownTimeout)) {
			try {
				SHRINE_SHUTDOWN_TIMEOUT = Integer.parseInt(shutdownTimeout);
			} catch (Throwable t) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, t.getMessage(), t);
			}
		}
		if (SHRINE_SHUTDOWN_TIMEOUT > SHRINE_SHUTDOWN_TIMEOUT_MAX) {
			SHRINE_SHUTDOWN_TIMEOUT = SHRINE_SHUTDOWN_TIMEOUT_MAX;
		}

		String dcosTaskId = System.getProperty(NAME_SHRINE_DCOS_TASK, System.getenv(NAME_SHRINE_DCOS_TASK));
		if (StringUtils.isNotBlank(dcosTaskId)) {
			SHRINE_CONTAINER_DEPLOYMENT_ID = dcosTaskId;
		} else {
			SHRINE_CONTAINER_DEPLOYMENT_ID = System
					.getProperty(NAME_SHRINE_K8S_DEPLOYMENT, System.getenv(NAME_SHRINE_K8S_DEPLOYMENT));
		}

		String zkClientSessionTimeoutStr = System.getProperty(NAME_SHRINE_ZK_CLIENT_SESSION_TIMEOUT_IN_SECONDS,
				System.getenv(NAME_SHRINE_ZK_CLIENT_SESSION_TIMEOUT_IN_SECONDS));
		if (StringUtils.isNotBlank(zkClientSessionTimeoutStr)) {
			try {
				SHRINE_ZK_CLIENT_SESSION_TIMEOUT_IN_SECONDS = Integer.parseInt(zkClientSessionTimeoutStr);
			} catch (Throwable t) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, t.getMessage(), t);
			}
		}

		String zkClientConnectionTimeoutStr = System
				.getProperty(NAME_SHRINE_ZK_CLIENT_CONNECTION_TIMEOUT_IN_SECONDS,
						System.getenv(NAME_SHRINE_ZK_CLIENT_CONNECTION_TIMEOUT_IN_SECONDS));
		if (StringUtils.isNotBlank(zkClientConnectionTimeoutStr)) {
			try {
				SHRINE_ZK_CLIENT_CONNECTION_TIMEOUT_IN_SECONDS = Integer.parseInt(zkClientConnectionTimeoutStr);
			} catch (Throwable t) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, t.getMessage(), t);
			}
		}

		String zkClientRetryTimes = System.getProperty(NAME_SHRINE_ZK_CLIENT_RETRY_TIMES,
				System.getenv(NAME_SHRINE_ZK_CLIENT_RETRY_TIMES));
		if (StringUtils.isNotBlank(zkClientRetryTimes)) {
			try {
				SHRINE_ZK_CLIENT_RETRY_TIMES = Integer.parseInt(zkClientRetryTimes);
			} catch (Throwable t) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, t.getMessage(), t);
			}
		}

		String useUnstableNetworkSettings = System.getProperty(NAME_SHRINE_USE_UNSTABLE_NETWORK_SETTING,
				System.getenv(NAME_SHRINE_USE_UNSTABLE_NETWORK_SETTING));
		if (StringUtils.isNotBlank(useUnstableNetworkSettings)) {
			SHRINE_USE_UNSTABLE_NETWORK_SETTING = Boolean.parseBoolean(useUnstableNetworkSettings);
		}

		String checkNohupOutSizeInterval = System.getProperty(NAME_SHRINE_CHECK_NOHUPOUT_SIZE_INTERVAL_IN_SEC,
				System.getenv(NAME_SHRINE_CHECK_NOHUPOUT_SIZE_INTERVAL_IN_SEC));
		if (StringUtils.isNotBlank(checkNohupOutSizeInterval)) {
			try {
				int interval_in_sec = Integer.parseInt(checkNohupOutSizeInterval);
				if (interval_in_sec > 0) {
					SHRINE_CHECK_NOHUPOUT_SIZE_INTERVAL_IN_SEC = interval_in_sec;
				}
			} catch (Throwable t) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, t.getMessage(), t);
			}
		}

		String noHupOutSizeLimit = System.getProperty(NAME_SHRINE_NOHUPOUT_SIZE_LIMIT_IN_BYTES,
				System.getenv(NAME_SHRINE_NOHUPOUT_SIZE_LIMIT_IN_BYTES));
		if (StringUtils.isNotBlank(noHupOutSizeLimit)) {
			try {
				long sizeLimit = Long.parseLong(noHupOutSizeLimit);
				if (sizeLimit > 0) {
					SHRINE_NOHUPOUT_SIZE_LIMIT_IN_BYTES = sizeLimit;
				}
			} catch (Throwable t) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, t.getMessage(), t);
			}
		}

		Set<String> tempSet = new HashSet<>();
		String initJobByGroups = System
				.getProperty(NAME_SHRINE_INIT_JOB_BY_GROUPS, System.getenv(NAME_SHRINE_INIT_JOB_BY_GROUPS));
		if (StringUtils.isNotBlank(initJobByGroups)) {
			try {
				String[] split = initJobByGroups.split(",");
				for (String temp : split) {
					if (StringUtils.isNotBlank(temp)) {
						tempSet.add(temp.trim());
					}
				}
			} catch (Throwable t) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, t.getMessage(), t);
			}
		}
		SHRINE_INIT_JOB_BY_GROUPS = tempSet;
		if (!SHRINE_INIT_JOB_BY_GROUPS.isEmpty()) {
			LOGGER.info( LogEvents.ExecutorEvent.COMMON, "the {} is set to {}",
					NAME_SHRINE_INIT_JOB_BY_GROUPS, SHRINE_INIT_JOB_BY_GROUPS);
		}

	}

	protected static String trim(String property) {
		if (property != null && property.length() > 0) {
			return property.trim();
		}
		return property;
	}

	public static void init() {
		if (SHRINE_CONSOLE_URI != null) {
			String[] split = SHRINE_CONSOLE_URI.split(",");
			if (split != null) {
				for (String tmp : split) {
					tmp = tmp.trim();
					if (!tmp.isEmpty()) {
						SHRINE_CONSOLE_URI_LIST.add(tmp);
					}
				}
			}
		}
	}

}
