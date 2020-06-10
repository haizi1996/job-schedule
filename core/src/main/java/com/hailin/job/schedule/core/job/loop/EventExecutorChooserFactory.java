package com.hailin.job.schedule.core.job.loop;

public interface EventExecutorChooserFactory {

    EventExecutorChooser newChooser(EventExecutor[] executors);

    interface EventExecutorChooser {

        EventExecutor next();
    }
}
