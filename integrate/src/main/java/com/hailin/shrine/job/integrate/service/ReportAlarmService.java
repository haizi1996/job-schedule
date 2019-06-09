package com.hailin.shrine.job.integrate.service;

import com.hailin.shrine.job.integrate.entity.AlarmInfo;
import com.hailin.shrine.job.integrate.exception.ReportAlarmException;

/**
 *提供报警服务。使用异步线程报告，当它很重时。
 */
public interface ReportAlarmService {

    void allShardingError(String namespace, String hostValue) throws ReportAlarmException;

    /**
     *
     *    执行程序重启的警报。
     */
    void executorRestart(String namespace, String executorName, String restartTime) throws ReportAlarmException;


    /**
     * 提高自定义警报。
     */
    void raise(String namespace, String jobName, String executorName, Integer shardItem, AlarmInfo alarmInfo)
            throws ReportAlarmException;
}
