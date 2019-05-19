
package com.hailin.shrine.job.common.util;

public final class BlockUtils {

	public static void waitingShortTime() {
		sleep(100L);
	}

	public static void sleep(final long millis) {
		try {
			Thread.sleep(millis);
		} catch (final InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}
}
