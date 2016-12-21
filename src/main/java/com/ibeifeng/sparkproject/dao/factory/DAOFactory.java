package com.ibeifeng.sparkproject.dao.factory;

import com.ibeifeng.sparkproject.dao.IAdBlacklistDAO;
import com.ibeifeng.sparkproject.dao.IAdClickTrendDAO;
import com.ibeifeng.sparkproject.dao.IAdProvinceTop3DAO;
import com.ibeifeng.sparkproject.dao.IAdStatDAO;
import com.ibeifeng.sparkproject.dao.IAdUserClickCountDAO;
import com.ibeifeng.sparkproject.dao.IAreaTop3ProductDAO;
import com.ibeifeng.sparkproject.dao.IPageSplitConvertRateDAO;
import com.ibeifeng.sparkproject.dao.ISessionAggrStatDAO;
import com.ibeifeng.sparkproject.dao.ISessionDetailDAO;
import com.ibeifeng.sparkproject.dao.ISessionRandomExtractDAO;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.ITop10CategoryDAO;
import com.ibeifeng.sparkproject.dao.ITop10SessionDAO;
import com.ibeifeng.sparkproject.dao.impl.AdBlacklistDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.AdClickTrendDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.AdProvinceTop3DAOImpl;
import com.ibeifeng.sparkproject.dao.impl.AdStatDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.AdUserClickCountDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.AreaTop3ProductDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.PageSplitConvertRateDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.SessionAggrStatDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.SessionDetailDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.SessionRandomExtractDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.TaskDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.Top10CategoryDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.Top10SessionImpl;

/**
 * DAO工厂类
 * 如果程序里面有100个地方使用到了TaskDAOImpl，如果不使用工厂设计模式
 * 那么程序里面出现的所有的ITaskDAO task = new TaskDAOImpl都要全部修改
 * 维护将非常困难。
 * @author Administrator
 *
 */
public class DAOFactory {

	/**
	 * 获取任务管理DAO
	 * @return
	 */
	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}
	
	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}
	
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
		return new SessionRandomExtractDAOImpl();
	}
	
	public static ISessionDetailDAO getSessionDetailDAO() {
		return new SessionDetailDAOImpl();
	}
	
	public static ITop10CategoryDAO getTop10CategoryDAO() {
		return new Top10CategoryDAOImpl();
	}
	
	public static ITop10SessionDAO getTop10SessionDAO() {
		return new Top10SessionImpl();
	}
	
	public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
		return new PageSplitConvertRateDAOImpl();
	}
	
	public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
		return new AreaTop3ProductDAOImpl();
	}
	
	public static IAdUserClickCountDAO getAdUserClickCountDAO() {
		return new AdUserClickCountDAOImpl();
	}
	
	public static IAdBlacklistDAO getAdBlacklistDAO() {
		return new AdBlacklistDAOImpl();
	}
	
	public static IAdStatDAO getAdStatDAO() {
		return new AdStatDAOImpl();
	}
	
	public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
		return new AdProvinceTop3DAOImpl();
	}
	
	public static IAdClickTrendDAO getAdClickTrendDAO() {
		return new AdClickTrendDAOImpl();
	}

}
