package com.hailin.job.schedule.core.job.shell;

import org.apache.commons.exec.LogOutputStream;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 捕获shell作业的输出流
 */
public class ScheduleLogOutputStream extends LogOutputStream {

    public static final int LEVEL_INFO = 1;
    public static final int LEVEL_ERROR = 2;

    private Logger log;


    public ScheduleLogOutputStream(Logger log, int level) {
        super(level);
        this.log = log;
    }

    @Override
    protected void processLine(String line, int logLevel) {
        if (logLevel == LEVEL_INFO) {
            log.info( line);
        } else if (logLevel == LEVEL_ERROR) {
            log.error(line);
        }
    }
}
