package com.hailin.shrine.job.common.exception;

/**
 * 本机与注册中心的时间误差超过容忍范围抛出的异常.
 * 
 * 
 */
public final class TimeDiffIntolerableException extends Exception {

	private static final long serialVersionUID = -6287464997081326084L;

	private static final String ERROR_MSG = "Time different between job server and register center exceed [%s] seconds, max time different is [%s] seconds.";

	public TimeDiffIntolerableException(final int timeDiffSeconds, final int maxTimeDiffSeconds) {
		super(String.format(ERROR_MSG, timeDiffSeconds, maxTimeDiffSeconds));
	}
}
