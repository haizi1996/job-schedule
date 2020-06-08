package com.hailin.job.schedule.core.job.trigger;

import com.hailin.shrine.job.common.exception.JobException;
import org.apache.commons.lang3.StringUtils;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

/**
 * @author zhanghailin
 */
public class CronTrigger extends AbstractTrigger {

    private static final String DEFAULT_CRON = "* * * * * ? 2099";
    private static final Logger LOGGER = LoggerFactory.getLogger(CronTrigger.class);

    @Override
    public Trigger createQuartzTrigger() {
        return null;
    }

    public Trigger createQuartzTrigger(boolean yes, String upStreamDataStr) {

        String cron = job.getConfigurationService().getCron();
        if (StringUtils.isBlank(cron)){
            cron = DEFAULT_CRON;
        }else {
            cron = cron.trim();
        }
        CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(cron);
        cronScheduleBuilder = cronScheduleBuilder.inTimeZone(job.getConfigurationService().getTimeZone())
                .withMisfireHandlingInstructionDoNothing();
        org.quartz.CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(job.getExecutorName() + "_" + job.getJobName()).withSchedule(cronScheduleBuilder).build();
        if (trigger instanceof org.quartz.spi.MutableTrigger) {
            ((org.quartz.spi.MutableTrigger) trigger)
                    .setMisfireInstruction(org.quartz.CronTrigger.MISFIRE_INSTRUCTION_DO_NOTHING);
        }
        return trigger;
    }
    private void validateCron(String cron) {
        try {
            CronExpression.validateExpression(cron);
        } catch (ParseException e) {
            LOGGER.error("validate cron failed", e);
            throw new JobException(e);
        }
    }

    @Override
    public boolean isInitialTriggered() {
        return false;
    }

    @Override
    public void enabledJob() {

    }

    @Override
    public void disableJob() {

    }

    @Override
    public void onReSharding() {

    }

    @Override
    public boolean isFailoverSupported() {
        return false;
    }
}
