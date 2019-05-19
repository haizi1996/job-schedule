package com.hailin.shrine.job.core.basic.threads;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 作业执行的线程工厂
 * @author zhanghailin
 */
public class ShrineThreadFactory implements ThreadFactory {

    private static final AtomicInteger poolNumber = new AtomicInteger(1);

    private AtomicInteger threadNumber = new AtomicInteger(1);

    private boolean isMultiple = true;

    private String threadName;

    public ShrineThreadFactory(String threadName){
        this.threadName = "Shrine-" + threadName + "-" + poolNumber.getAndIncrement() + "-thread-";
    }

    public ShrineThreadFactory(String threadName, boolean isMultiple) {
        this.isMultiple = isMultiple;
        this.threadName = threadName;
    }

    @Override
    public Thread newThread(Runnable r) {
        String name = isMultiple ? threadName + threadNumber.getAndIncrement() : threadName;
        Thread t = new Thread(r, name);
        if (t.isDaemon()) {
            t.setDaemon(false);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }
}
