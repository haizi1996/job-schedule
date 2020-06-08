package com.hailin.job.schedule.core.executor;

import com.hailin.job.schedule.core.basic.threads.ScheduleThreadFactory;
import com.hailin.shrine.job.sharding.entity.Executor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public abstract class EnhancedConnectionStateListener implements ConnectionStateListener {

    private static final Logger log = LoggerFactory.getLogger(EnhancedConnectionStateListener.class);

    private String executorName;

    private volatile boolean connected = false;

    private volatile boolean closed = false;

    private ExecutorService checkLostThread;

    public EnhancedConnectionStateListener(String executorName) {
        this.executorName = executorName;
        this.checkLostThread = Executors.newSingleThreadExecutor(new ScheduleThreadFactory(executorName + "-check-lost-thread" , false));
    }

    private long getSessionId(CuratorFramework client){
        long sessionId ;
        try {
            return client.getZookeeperClient().getZooKeeper().getSessionId();
        }catch (Exception e){
            return -1;
        }
    }

    @Override
    public void stateChanged(CuratorFramework curatorFramework, ConnectionState newState) {
        if (closed){
            return;
        }
        final String clientStr = curatorFramework.toString();
        if (ConnectionState.SUSPENDED == newState ){
            connected = false;
            log.warn("The executor {} found zk is SUSPENDED, client is {}", executorName, clientStr);
            final long sessionId = getSessionId(curatorFramework);
            if (!closed){
                checkLostThread.submit(new Runnable() {
                    @Override
                    public void run() {
                        do {
                            try {
                                Thread.sleep(1000L);
                            } catch (InterruptedException e) {
                                log.debug(EnhancedConnectionStateListener.class.getCanonicalName(),
                                        "checkLostThread is interrupted");
                            }
                            if (closed) {
                                break;
                            }
                            long newSessionId = getSessionId(curatorFramework);
                            if (sessionId != newSessionId) {
                                log.warn("The executor {} is going to restart for zk lost, client is {}", executorName,
                                        clientStr);

                                onLost();
                                break;
                            }
                        } while (!closed && !connected);
                    }
                });
            }
        }else if (ConnectionState.RECONNECTED == newState){
            log.warn(
                    "The executor {} found zk is RECONNECTED, client is {}", executorName, clientStr);
            connected = true;
        }
    }

    public abstract void onLost();

    public void close(){
        this.closed = true;
        this.checkLostThread.shutdownNow();
    }
}
