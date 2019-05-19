package com.hailin.shrine.job.common.exception;

import java.io.IOException;

/**
 *     网络主机异常.
 * @author zhanghailin
 */
public class HostException extends RuntimeException{



    public HostException(final IOException cause){
        super(cause);
    }

}
