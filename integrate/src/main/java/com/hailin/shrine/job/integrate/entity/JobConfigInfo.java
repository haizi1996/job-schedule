package com.hailin.shrine.job.integrate.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class JobConfigInfo {

	private String namespace;

	private String jobName;

	private String perferList;

	@Override
	public String toString() {
		return "JobConfigInfo [namespace=" + namespace + ", jobName=" + jobName + ", perferList=" + perferList + "]";
	}
}
