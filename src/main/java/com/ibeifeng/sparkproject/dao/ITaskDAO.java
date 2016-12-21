package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.Task;

/**
 * 任务管理DAO接口
 * 数据链路层又叫做DAO
 * @author Administrator
 *
 */
public interface ITaskDAO {

	/**
	 * 根据主见查询任务
	 * @param taskid
	 * @return
	 */
	Task findById(long taskid);
}
