package com.hailin.shrine.job.common.exception;

public class JobExecutionEnvironmentException extends RuntimeException {

    public JobExecutionEnvironmentException(Exception ex){
        super(ex);
    }
    public JobExecutionEnvironmentException(final String errorMessage, final Object... args) {
        super(String.format(errorMessage, args));
    }
}
