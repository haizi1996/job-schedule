package com.hailin.shrine.job.api;


import com.hailin.shrine.job.application.AbstractShrineApplication;
import org.springframework.context.ApplicationContext;

/**
 * @author hebelala
 */
public abstract class AbstractSpringShrineApplication extends AbstractShrineApplication {

	protected ApplicationContext applicationContext;

	@Override
	public <J> J getJobInstance(Class<J> jobClass) {
		return applicationContext != null ? applicationContext.getBean(jobClass) : null;
	}
}
