package com.hailin.shrine.job.common.exception;

/**
 * 分批am序列号和参数格式化错误异常
 */
public class ShardingItemParametersException extends JobException {

    private static final long serialVersionUID = 6743804578144596967L;

    /**
     * @param errorMessage the format of error message
     * @param args Arguments referenced by the format specifiers in the format string
     */
    public ShardingItemParametersException(final String errorMessage, final Object... args) {
        super(errorMessage, args);
    }
}
