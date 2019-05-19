package com.hailin.shrine.job.common.exception;


import org.apache.commons.lang3.ArrayUtils;

/**
 * 分布式作业抛出的异常基类
 * @author zhanghailin
 */
public class JobException extends RuntimeException {
    private static final long serialVersionUID = -5323792555332165319L;

    public JobException(final String errorMessage, final Object... args) {
        super(ArrayUtils.isEmpty(args) ? errorMessage : String.format(errorMessage, args));
    }

    public JobException(final Exception cause) {
        super(cause);
    }

    public JobException(Throwable cause) {
        super(cause);
    }
}
