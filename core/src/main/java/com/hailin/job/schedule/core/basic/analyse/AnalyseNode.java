
package com.hailin.job.schedule.core.basic.analyse;

/**
 * 节点有作业的执行总次数与失败总次数
 * 
 * @author zhanghailin
 */
public final class AnalyseNode {

	public static final String ROOT = "analyse";

	/** 执行总次数 **/
	public static final String PROCESS_COUNT = ROOT + "/processCount";

	/** 失败总次数 **/
	public static final String ERROR_COUNT = ROOT + "/errorCount";

	/** reset 统计数据 **/
	public static final String RESET = ROOT + "/reset";

}
