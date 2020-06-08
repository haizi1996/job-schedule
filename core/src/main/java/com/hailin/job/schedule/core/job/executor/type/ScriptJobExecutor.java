
package com.hailin.job.schedule.core.job.executor.type;

import com.google.common.base.Strings;
import com.hailin.job.schedule.core.config.script.ScriptJobConfiguration;
import com.hailin.job.schedule.core.executor.ShardingContext;
import com.hailin.job.schedule.core.job.JobFacade;
import com.hailin.shrine.job.common.exception.JobConfigurationException;
import com.hailin.shrine.job.common.util.GsonFactory;
import com.hailin.job.schedule.core.job.executor.AbstractShrineJobExecutor;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;

import java.io.IOException;

/**
 * 脚本作业执行器.
 * 
 */
public final class ScriptJobExecutor extends AbstractShrineJobExecutor {
    
    public ScriptJobExecutor(final JobFacade jobFacade) {
        super(jobFacade);
    }
    
    @Override
    protected void process(final ShardingContext shardingContext) {
        final String scriptCommandLine = ((ScriptJobConfiguration) getJobRootConfig().getTypeConfig()).getScriptCommandLine();
        if (Strings.isNullOrEmpty(scriptCommandLine)) {
            throw new JobConfigurationException("Cannot find script command line for job '%s', job is not executed.", shardingContext.getJobName());
        }
        executeScript(shardingContext, scriptCommandLine);
    }
    
    private void executeScript(final ShardingContext shardingContext, final String scriptCommandLine) {
        CommandLine commandLine = CommandLine.parse(scriptCommandLine);
        commandLine.addArgument(GsonFactory.getGson().toJson(shardingContext), false);
        try {
            new DefaultExecutor().execute(commandLine);
        } catch (final IOException ex) {
            throw new JobConfigurationException("Execute script failure.", ex);
        }
    }
}
