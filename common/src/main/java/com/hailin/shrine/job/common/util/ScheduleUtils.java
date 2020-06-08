package com.hailin.shrine.job.common.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ScheduleUtils {

	public static String convertTime2FormattedString(long time) {
		Date date = new Date(time);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
		return sdf.format(date);
	}

}
