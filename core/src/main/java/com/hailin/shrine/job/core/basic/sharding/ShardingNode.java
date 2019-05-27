
package com.hailin.shrine.job.core.basic.sharding;


import com.hailin.shrine.job.core.basic.election.LeaderNode;
import com.hailin.shrine.job.core.basic.server.ServerNode;
import com.hailin.shrine.job.core.basic.storage.JobNodePath;

/**
 * Saturn分片节点名称的常量类.
 * 
 * 
 */
public final class ShardingNode {

	/**
	 * 执行状态根节点.
	 */
	public static final String ROOT = "sharding";

	public static final String LEADER_SHARDING_ROOT = LeaderNode.ROOT + "/sharding";

	public static final String NECESSARY = LEADER_SHARDING_ROOT + "/necessary";

	public static final String PROCESSING = LEADER_SHARDING_ROOT + "/processing";

	private static final String SERVER_SHARDING = ServerNode.ROOT + "/%s/sharding";

	static final String MISFIRE = ROOT + "/%s/misfire";

	private final String jobName;

	static final String INSTANCE_APPENDIX = "instance";

	public static final String INSTANCE = ROOT + "/%s/" + INSTANCE_APPENDIX;

	static final String RUNNING_APPENDIX = "running";

	static final String RUNNING = ROOT + "/%s/" + RUNNING_APPENDIX;


	static final String DISABLED = ROOT + "/%s/disabled";

//	static final String LEADER_ROOT = LeaderNode.ROOT + "/" + ROOT;

	private JobNodePath jobNodePath;





	public static String getInstanceNode(final int item) {
		return String.format(INSTANCE, item);
	}

	/**
	 * 获取作业运行状态节点路径.
	 *
	 * @param item 作业项
	 * @return 作业运行状态节点路径
	 */
	public static String getRunningNode(final int item) {
		return String.format(RUNNING, item);
	}



	public static String getDisabledNode(final int item) {
		return String.format(DISABLED, item);
	}

	/**
	 * 根据运行中的分片路径获取分片项.
	 *
	 * @param path 运行中的分片路径
	 * @return 分片项, 不是运行中的分片路径获则返回null
	 */
	public Integer getItemByRunningItemPath(final String path) {
		if (!isRunningItemPath(path)) {
			return null;
		}
		return Integer.parseInt(path.substring(jobNodePath.getFullPath(ROOT).length() + 1, path.lastIndexOf(RUNNING_APPENDIX) - 1));
	}

	private boolean isRunningItemPath(final String path) {
		return path.startsWith(jobNodePath.getFullPath(ROOT)) && path.endsWith(RUNNING_APPENDIX);
	}

	public ShardingNode(String jobName) {
		this.jobName = jobName;
	}

	public static String getShardingNode(final String executorName) {
		return String.format(SERVER_SHARDING, executorName);
	}


	/**
	 * 判断是否为需要重新做sharding的Path
	 * 
	 * @param path 节点路径
	 * @return 判断是否为需要重新做sharding的Path
	 */
	public boolean isShardingNecessaryPath(final String path) {
		return JobNodePath.getNodeFullPath(jobName, NECESSARY).equals(path);
	}

	public static String getMisfireNode(final int item) {
		return String.format(MISFIRE, item);
	}
}
