package com.hailin.job.schedule.core.reg.exception;

import com.hailin.shrine.job.common.util.LogEvents;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 抛出RegException的异常处理类
 * @author zhanghailin
 */
public class RegExceptionHandler {
    private static Logger LOGGER = LoggerFactory.getLogger(RegExceptionHandler.class);

    private RegExceptionHandler() {
    }

    public static void handleException(final Exception cause){
        if(cause == null){
            throw new RegException(null);
        }
        if(isIgnoredException(cause) || isIgnoredException(cause.getCause())){
            LOGGER.debug(LogEvents.ExecutorEvent.COMMON, "Shrine job: ignored exception for: {}",
                    cause.getMessage());
        }else if (cause instanceof InterruptedException){
            Thread.currentThread().interrupt();
        }else {
            throw new RegException(cause);
        }
    }

    private static boolean isIgnoredException(Throwable cause) {
        if(cause == null){
            return false;
        }
        return cause instanceof KeeperException.ConnectionLossException
                || cause instanceof KeeperException.NoNodeException
                || cause instanceof KeeperException.NodeExistsException;
    }
}
