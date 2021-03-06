package com.hailin.job.schedule.core.utils;


import org.apache.commons.exec.LogOutputStream;

import java.io.PrintStream;

/**
 * 捕获System.out输出
 *
 */
public class ScheduleSystemOutputStream extends LogOutputStream {

	private static final int MAX_LINE = 100;

	private static ThreadLocal<LRUList<String>> lists = new InheritableThreadLocal<LRUList<String>>() {
		@Override
		protected LRUList<String> initialValue() {
			return new LRUList<>(MAX_LINE);
		}
	};
	private static PrintStream catchedOut = new PrintStream(new ScheduleSystemOutputStream(1));
	private static PrintStream out = System.out; // NOSONAR

	static {
		System.setOut(catchedOut);
	}

	private ScheduleSystemOutputStream(int level) {
		super(level);
	}

	protected void processLine(String line, int level) {
		LRUList<String> lruList = lists.get();
		lruList.put(line);
		out.println(line);
	}

	public static void initLogger() {
		lists.get().clear();
	}

	private static void clearCache() {
		lists.remove();
	}

	public static String clearAndGetLog() {
		try {
			StringBuilder sb = new StringBuilder();
			LRUList<String> lruList = lists.get();
			for (String line : lruList) {
				sb.append(line).append(System.lineSeparator());
			}
			lruList.clear();
			clearCache();
			return sb.toString();
		} catch (Exception e) {// NOSONAR
			return "";
		}
	}

}