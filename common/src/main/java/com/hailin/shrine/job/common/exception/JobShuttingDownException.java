
package com.hailin.shrine.job.common.exception;

/**
 * 调用了shutdown， 但作业还在调度时抛该异常。
 * 
 * @author chembo.huang
 */
public final class JobShuttingDownException extends Exception {

	private static final long serialVersionUID = -6287464997081326084L;

	private static final String ERROR_MSG = "Job is shutting down, job shouldn't be invoked.";

	public JobShuttingDownException() {
		super(ERROR_MSG);
	}
}
