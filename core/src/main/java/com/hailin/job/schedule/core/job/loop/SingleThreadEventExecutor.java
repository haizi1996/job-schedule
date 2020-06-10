package com.hailin.job.schedule.core.job.loop;

import com.google.common.base.Preconditions;
import com.hailin.job.schedule.core.job.trigger.ScheduleWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class SingleThreadEventExecutor extends AbstractEventExecutor {

    private static Logger logger = LoggerFactory.getLogger(SingleThreadEventExecutor.class);

    static final int DEFAULT_MAX_PENDING_EXECUTOR_TASKS = 16;

    static final Runnable WAKEUP_TASK = new Runnable() {
        @Override
        public void run() {
        } // Do nothing
    };

    private final BlockingQueue<Runnable> taskQueue;

    //500 毫秒
    private static final int SLEEP_TIME = 500;

    private volatile Thread thread;

    private static final long START_TIME = System.nanoTime();
    private long lastExecutionTime;


    private static final int ST_NOT_STARTED = 1;
    private static final int ST_STARTED = 2;
    private static final int ST_SHUTTING_DOWN = 3;
    private static final int ST_SHUTDOWN = 4;
    private static final int ST_TERMINATED = 5;

    private volatile int state = ST_NOT_STARTED;

    private final boolean addTaskWakesUp;
    private final int maxPendingTasks;
    private final RejectedExecutionHandler rejectedExecutionHandler;
    private static final AtomicIntegerFieldUpdater<SingleThreadEventExecutor> STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(SingleThreadEventExecutor.class, "state");

    private final CountDownLatch threadLock = new CountDownLatch(1);

    private Executor executor;
    private volatile boolean interrupted;

    private volatile long gracefulShutdownQuietPeriod;
    private volatile long gracefulShutdownTimeout;
    private long gracefulShutdownStartTime;

    //小顶堆
    private volatile ScheduleWorker[] workers = new  ScheduleWorker[16];
    private PriorityQueue<ScheduleWorker>  priorityQueue = new PriorityQueue<>(cm);

    private static final Comparator<ScheduleWorker> cm = (o1, o2) -> {
        //立即触发
        if (o1.getTriggered().isYes()) {
            return -1;
        } else if (o2.getTriggered().isYes()) {
            return 1;
        } else {
            return o1.getNextFireTimePausePeriodEffected().compareTo(o2.getNextFireTimePausePeriodEffected());
        }

    };

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleThreadEventExecutor.class);

    protected SingleThreadEventExecutor(Executor executor,
                                        boolean addTaskWakesUp, BlockingQueue<Runnable> taskQueue,
                                        RejectedExecutionHandler rejectedHandler) {
        this.addTaskWakesUp = addTaskWakesUp;
        this.maxPendingTasks = DEFAULT_MAX_PENDING_EXECUTOR_TASKS;
        this.taskQueue = Preconditions.checkNotNull(taskQueue, "taskQueue");
        this.rejectedExecutionHandler = Preconditions.checkNotNull(rejectedHandler, "rejectedHandler");
    }

    public void execute(Runnable task) {
        Preconditions.checkNotNull(task, "task");
        boolean inEventLoop = inEventLoop();
        addTask(task);
        if (!inEventLoop) {
            startThread();
            if (isShutdown()) {
                boolean reject = false;
                try {
                    if (removeTask(task)) {
                        reject = true;
                    }
                } catch (UnsupportedOperationException e) {
                    // The task queue does not support removal so the best thing we can do is to just move on and
                    // hope we will be able to pick-up the task before its completely terminated.
                    // In worst case we will log on termination.
                }
                if (reject) {
                    reject();
                }
            }
        }
        if (!addTaskWakesUp) {
            wakeup(inEventLoop);
        }
    }

    protected boolean removeTask(Runnable task) {
        return taskQueue.remove(Preconditions.checkNotNull(task, "task"));
    }

    protected void wakeup(boolean inEventLoop) {
        if (!inEventLoop) {
            // Use offer as we actually only need this to unblock the thread and if offer fails we do not care as there
            // is already something in the queue.
            taskQueue.offer(WAKEUP_TASK);
        }
    }

    protected void addTask(Runnable task) {
        Preconditions.checkNotNull(task, "task");
        if (!offerTask(task)) {
            reject(task);
        }
    }


    final boolean offerTask(Runnable task) {
        if (isShutdown()) {
            reject();
        }
        return taskQueue.offer(task);
    }

    protected static void reject() {
        throw new RejectedExecutionException("event executor terminated");
    }

    @Override
    public boolean inEventLoop(Thread thread) {
        return thread == this.thread;
    }

    protected void reject(Runnable task) {
        rejectedExecutionHandler.rejected(task, this);
    }

    private void startThread() {
        if (state == ST_NOT_STARTED) {
            if (STATE_UPDATER.compareAndSet(this, ST_NOT_STARTED, ST_STARTED)) {
                boolean success = false;
                try {
                    doStartThread();
                    success = true;
                } finally {
                    if (!success) {
                        STATE_UPDATER.compareAndSet(this, ST_STARTED, ST_NOT_STARTED);
                    }
                }
            }
        }
    }

    private void doStartThread() {
        assert thread == null;
        executor.execute(new Runnable() {
            @Override
            public void run() {
                thread = Thread.currentThread();
                if (interrupted) {
                    thread.interrupt();
                }

                boolean success = false;
                updateLastExecutionTime();
                try {
                    SingleThreadEventExecutor.this.run();
                    success = true;
                } catch (Throwable t) {
                    logger.warn("Unexpected exception from an event executor: ", t);
                } finally {
                    for (; ; ) {
                        int oldState = state;
                        if (oldState >= ST_SHUTTING_DOWN || STATE_UPDATER.compareAndSet(
                                SingleThreadEventExecutor.this, oldState, ST_SHUTTING_DOWN)) {
                            break;
                        }
                    }

                    // Check if confirmShutdown() was called at the end of the loop.
                    if (success && gracefulShutdownStartTime == 0) {
                        if (logger.isErrorEnabled()) {
                            logger.error("Buggy " + EventExecutor.class.getSimpleName() + " implementation; " +
                                    SingleThreadEventExecutor.class.getSimpleName() + ".confirmShutdown() must " +
                                    "be called before run() implementation terminates.");
                        }
                    }

                    try {
                        // Run all remaining tasks and shutdown hooks. At this point the event loop
                        // is in ST_SHUTTING_DOWN state still accepting tasks which is needed for
                        // graceful shutdown with quietPeriod.
                        for (; ; ) {
                            if (confirmShutdown()) {
                                break;
                            }
                        }

                        // Now we want to make sure no more tasks can be added from this point. This is
                        // achieved by switching the state. Any new tasks beyond this point will be rejected.
                        for (; ; ) {
                            int oldState = state;
                            if (oldState >= ST_SHUTDOWN || STATE_UPDATER.compareAndSet(
                                    SingleThreadEventExecutor.this, oldState, ST_SHUTDOWN)) {
                                break;
                            }
                        }

                        // We have the final set of tasks in the queue now, no more can be added, run all remaining.
                        // No need to loop here, this is the final pass.
                        confirmShutdown();
                    } finally {


                        STATE_UPDATER.set(SingleThreadEventExecutor.this, ST_TERMINATED);

                    }
                }
            }
        });
    }

    /**
     * 执行 executor的任务
     * @param timeout 执行时间 毫秒
     */
    public void doTaskQueue(Integer timeout) throws InterruptedException {
        Runnable task = taskQueue.poll(timeout , TimeUnit.MILLISECONDS);
        if (Objects.nonNull(task )){
            task.run();
        }
    }
    private void run() throws InterruptedException {
        while (!isShutdown()){

            ScheduleWorker worker = priorityQueue.peek();
            if(Objects.isNull(worker)) {
                doTaskQueue(SLEEP_TIME);
                // 如果是暂停或者停止都需要移除调度队列
            }else if(worker.isPaused() || worker.getHalted().get()){
                priorityQueue.remove(worker);
                // 如果是立即执行 或者到了执行时间点 获取自旋转成功(临近执行时间)
            }else if (worker.getTriggered().isYes() || spinToExecute(worker)){
                priorityQueue.remove(worker);
                //todo 执行任务
                worker.getOperableTrigger().triggered(null);
                worker.execute();
            }
        }
    }

    private boolean spinToExecute(ScheduleWorker worker) throws InterruptedException {
        long now = System.currentTimeMillis();
        if (worker.getNextFireTimePausePeriodEffected().getTime() - now > 500){
            doTaskQueue(500);
            return false;
        }
        return worker.getNextFireTimePausePeriodEffected().getTime() - now <= 2;

    }


    protected boolean confirmShutdown() {
        if (!isShuttingDown()) {
            return false;
        }

        if (!inEventLoop()) {
            throw new IllegalStateException("must be invoked from an event loop");
        }


        if (gracefulShutdownStartTime == 0) {
            gracefulShutdownStartTime = System.nanoTime() - START_TIME;
        }


        if (isShutdown()) {
            // Executor shut down - no new tasks anymore.
            return true;
        }

        // There were tasks in the queue. Wait a little bit more until no tasks are queued for the quiet period or
        // terminate if the quiet period is 0.
        // See https://github.com/netty/netty/issues/4241
        if (gracefulShutdownQuietPeriod == 0) {
            return true;
        }
        taskQueue.offer(WAKEUP_TASK);


        final long nanoTime = System.nanoTime() - START_TIME;

        if (isShutdown() || nanoTime - gracefulShutdownStartTime > gracefulShutdownTimeout) {
            return true;
        }

        if (nanoTime - lastExecutionTime <= gracefulShutdownQuietPeriod) {
            // Check if any tasks were added to the queue every 100ms.
            // TODO: Change the behavior of takeTask() so that it returns on timeout.
            taskQueue.offer(WAKEUP_TASK);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // Ignore
            }

            return false;
        }

        // No tasks were added for last quiet period - hopefully safe to shut down.
        // (Hopefully because we really cannot make a guarantee that there will be no execute() calls by a user.)
        return true;
    }

    protected Runnable takeTask() {
        assert inEventLoop();
        if (!(taskQueue instanceof BlockingQueue)) {
            throw new UnsupportedOperationException();
        }

        BlockingQueue<Runnable> taskQueue = (BlockingQueue<Runnable>) this.taskQueue;
        for (; ; ) {

            Runnable task = null;
            try {
                task = taskQueue.take();
                if (task == WAKEUP_TASK) {
                    task = null;
                }
            } catch (InterruptedException e) {
                // Ignore
            }
            return task;
        }
    }

    @Override
    public boolean isShuttingDown() {
        return state >= ST_SHUTTING_DOWN;
    }

    @Override
    public boolean isShutdown() {
        return state >= ST_SHUTDOWN;
    }

    private void updateLastExecutionTime() {
        lastExecutionTime = System.nanoTime() - START_TIME;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public void submit(Runnable task) {
        execute(task);
    }





    private boolean ensureThreadStarted(int oldState) {
        if (oldState == ST_NOT_STARTED) {
            try {
                doStartThread();
            } catch (Throwable cause) {
                STATE_UPDATER.set(this, ST_TERMINATED);
            }
        }
        return false;
    }

    @Override
    public boolean awaitTermination(int timeout, TimeUnit unit) throws InterruptedException {
        Preconditions.checkNotNull(unit, "unit");
        if (inEventLoop()) {
            throw new IllegalStateException("cannot await termination of the current thread");
        }

        threadLock.await(timeout, unit);
        return isTerminated();
    }

    @Override
    public void shutdownGracefully() {

    }


    @Override
    public boolean registerScheduleWorker(ScheduleWorker saturnWorker) {
        submit( ()->priorityQueue.add(saturnWorker));
        return true;
    }
}
