package com.hailin.shrine.job.sharding.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CuratorUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(CuratorUtils.class);

	private CuratorUtils() {
	}

	public static void deletingChildrenIfNeeded(final CuratorFramework curatorFramework, final String path)
			throws Exception {
		List<String> children;
		try {
			children = curatorFramework.getChildren().forPath(path);
		} catch (KeeperException.NoNodeException e) {
			LOGGER.debug("no node exception throws during get children of path:" + path, e);
			return;
		}

		if (children != null) {
			for (String child : children) {
				deletingChildrenIfNeeded(curatorFramework, path + "/" + child);
			}
		}

		try {
			curatorFramework.delete().guaranteed().forPath(path);
		} catch (KeeperException.NotEmptyException e) {
			LOGGER.debug("try to delete path:" + path + " but fail for NotEmptyException", e);
			deletingChildrenIfNeeded(curatorFramework, path);
		} catch (KeeperException.NoNodeException e) {
			LOGGER.debug("try to delete path:" + path + " but fail for NoNodeException", e);
		}
	}

}
