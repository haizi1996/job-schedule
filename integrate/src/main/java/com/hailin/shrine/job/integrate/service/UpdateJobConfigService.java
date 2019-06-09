package com.hailin.shrine.job.integrate.service;


import com.hailin.shrine.job.integrate.entity.JobConfigInfo;
import com.hailin.shrine.job.integrate.exception.UpdateJobConfigException;

import java.util.List;

/**
 * 更新Job配置
 * 
 */
public interface UpdateJobConfigService {

	/**
	 * 批量更新作业的perferList属性
	 * 
	 * @param jobConfigInfos 作业配置信息
	 */
	void batchUpdatePreferList(List<JobConfigInfo> jobConfigInfos) throws UpdateJobConfigException;

}
