
package com.hailin.shrine.job.core.basic.sharding;


import com.hailin.shrine.job.core.basic.election.ElectionNode;
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

	public static final String LEADER_SHARDING_ROOT = ElectionNode.ROOT + "/sharding";

	public static final String NECESSARY = LEADER_SHARDING_ROOT + "/necessary";

	public static final String PROCESSING = LEADER_SHARDING_ROOT + "/processing";

	private static final String SERVER_SHARDING = ServerNode.ROOT + "/%s/sharding";

	static final String MISFIRE = ROOT + "/%s/misfire";

	private final String jobName;

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

	static String getMisfireNode(final int item) {
		return String.format(MISFIRE, item);
	}
}
