package com.hailin.shrine.job;

import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Set;

public final class ScheduleSystemErrorGroup {

	public static final int SUCCESS = 200;

	// general fail
	public static final int FAIL = 500;

	public static final int TIMEOUT = 550;

	// alarm will be raised with this error code
	public static final int FAIL_NEED_RAISE_ALARM = 551;

	public static Set<Integer> getAllSystemErrorGroups(){
		Set<Integer> resultSet = Sets.newHashSet();
		resultSet.add(SUCCESS);
		resultSet.add(FAIL);
		resultSet.add(TIMEOUT);
		resultSet.add(FAIL_NEED_RAISE_ALARM);

		return resultSet;
	}

}
