package com.hailin.job.schedule.core.job.shell;

import com.hailin.job.schedule.core.utils.LRUList;
import org.apache.commons.exec.LogOutputStream;

/**
 * 封装子进程的输出信息
 */
public class ProcessOutputStream extends LogOutputStream {

    private static final int MAX_LINE = 100;

    /** 保存最近运行的100条记录 */
    // private LRUMap map = new LRUMap(MAX_LINE);
    LRUList<String> lruList = new LRUList<>(MAX_LINE);

    Object lock = new Object();

    public ProcessOutputStream(int level) {
        super(level);
    }

    @Override
    protected void processLine(String line, int level) {
        synchronized (lock) {
            lruList.put(line);
        }
    }

    /**
     * @return 获取运行作业的日志
     */
    public String getJobLog() {
        StringBuilder sb = new StringBuilder();
        synchronized (lock) {
            for (String line : lruList) {
                sb.append(line).append(System.lineSeparator());
            }
        }
        return sb.toString();
    }
}
