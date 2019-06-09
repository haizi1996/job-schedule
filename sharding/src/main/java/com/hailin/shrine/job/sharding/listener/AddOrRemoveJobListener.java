package com.hailin.shrine.job.sharding.listener;

import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import com.hailin.shrine.job.sharding.service.AddJobListenersService;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AllArgsConstructor
public class AddOrRemoveJobListener extends AbstractTreeCacheListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AddOrRemoveJobListener.class);

    private AddJobListenersService addJobListenersService;

    @Override
    protected void childEvent(TreeCacheEvent.Type type, String path, String nodeData) {
        try {
            String job = StringUtils.substringAfterLast(path, "/");
            if (!ShrineExecutorsNode.JOBS_NODE.equals(job)) {
                if (isAddJob(type)) {
                    LOGGER.info("job: {} created", job);
                    addJobListenersService.addJobPathListener(job);
                } else if (isRemoveJob(type)) {
                    LOGGER.info("job: {} removed", job);
                    addJobListenersService.removeJobPathTreeCache(job);
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
    private boolean isAddJob(TreeCacheEvent.Type type) {
        return type == TreeCacheEvent.Type.NODE_ADDED;
    }

    private boolean isRemoveJob(TreeCacheEvent.Type type) {
        return type == TreeCacheEvent.Type.NODE_REMOVED;
    }
}
