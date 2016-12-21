package com.ibeifeng.sparkproject.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.ibeifeng.sparkproject.dao.IAdBlacklistDAO;
import com.ibeifeng.sparkproject.domain.AdBlacklist;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * 广告黑名单DAO实现类
 * @author Administrator
 *
 */
public class AdBlacklistDAOImpl implements IAdBlacklistDAO {

	public void insertBatch(List<AdBlacklist> adBlacklists) {
		String sql = "INSERT INTO ad_blacklist VALUES(?)";
		
		List<Object[]> paramsList = new ArrayList<Object[]>();
		
		for(AdBlacklist adBlacklist : adBlacklists) {
			Object[] params = new Object[]{adBlacklist.getUserid()};
			paramsList.add(params);
		}
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(sql, paramsList);
		
	}

	public List<AdBlacklist> findALL() {
		String sql = "SELECT * FROM ad_blacklist";
		
		final List<AdBlacklist> adBlacklists = new ArrayList<AdBlacklist>();
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		jdbcHelper.executeQuery(sql, null, new JDBCHelper.QueryCallback() {
			
			public void process(ResultSet rs) throws Exception {
				while(rs.next()) {
					long userid = Long.valueOf(String.valueOf(rs.getInt(1)));
					
					AdBlacklist adBlacklist = new AdBlacklist();
					adBlacklist.setUserid(userid);
					
					adBlacklists.add(adBlacklist);
					
				}
			}
		});
		
		return adBlacklists;
	}
	
	

}
