package com.ibeifeng.sparkproject.dao;

import java.util.List;

import com.ibeifeng.sparkproject.domain.AdUserClickCount;

/**
 * 用户广告点击量DAO接口
 * @author Administrator
 *
 */
public interface IAdUserClickCountDAO {

	/**
	 * 批量更新用户广告点击量
	 * @param adUserClickCounts
	 */
	void updateBatch(List<AdUserClickCount> adUserClickCounts);
	
	/**
	 * 根据多个key查询用户广告点击量
	 * @param date
	 * @param userid
	 * @param adid
	 * @return
	 */
	int findClickCountByMultiKey(String date,long userid,long adid);
}
