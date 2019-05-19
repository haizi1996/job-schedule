package com.hailin.shrine.job.common.exception;

public class ShrineExecutorException extends Exception{

    private final int code ;

    public ShrineExecutorException(String message){
        super(message);
        this.code = 0;
    }
    public ShrineExecutorException(int code ,String message){
        super(message);
        this.code = code;
    }

    public ShrineExecutorException(String message , Throwable cause){
        super(message ,cause);
        this.code = 0;
    }

    public int getCode() {
        return code;
    }
}
