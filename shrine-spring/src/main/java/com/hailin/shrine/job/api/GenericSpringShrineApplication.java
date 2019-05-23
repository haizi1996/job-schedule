package com.hailin.shrine.job.api;


import com.hailin.shrine.job.api.annotation.Schedule;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;

import java.util.List;
import java.util.Map;

public class GenericSpringShrineApplication extends AbstractSpringShrineApplication  implements ApplicationListener<ContextRefreshedEvent> {


	@Override
	public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
		if (contextRefreshedEvent != null){
			applicationContext = contextRefreshedEvent.getApplicationContext();
		}
	}

	@Override
	public void init() {
		Map<String , Object> jobs = applicationContext.getBeansWithAnnotation(Schedule.class);
		for (Map.Entry<String, Object> entry: jobs.entrySet()){
			if ()
		}

	}

	@Override
	public void destroy() {
		if (applicationContext != null) {
			if (applicationContext instanceof ConfigurableApplicationContext) {
				((ConfigurableApplicationContext) applicationContext).close();
			}
			applicationContext = null;
		}
	}



}
