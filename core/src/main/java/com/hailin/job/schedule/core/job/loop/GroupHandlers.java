package com.hailin.job.schedule.core.job.loop;

import com.hailin.job.schedule.core.basic.threads.ScheduleThreadFactory;
import com.hailin.job.schedule.core.job.trigger.ScheduleWorker;

public class GroupHandlers {

    private static volatile  MultithreadEventExecutorGroup group = new MultithreadEventExecutorGroup(new ScheduleThreadFactory("GroupHandlers"));

    /**
     * 增加一个调度作业
     */
    public static void registerScheduleWorker(ScheduleWorker saturnWorker){
        group.next().registerScheduleWorker(saturnWorker);

    }


}
