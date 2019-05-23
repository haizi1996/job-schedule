package com.hailin.shrine.job.application;

public abstract class AbstractShrineApplication implements ShrineApplication {

	@Override
	public <J> J getJobInstance(Class<J> jobClass) {
		return null;
	}
}
