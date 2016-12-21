package com.ibeifeng.sparkproject.util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.test.MockData;

/**
 * Spark工具类
 * @author Administrator
 *
 */
public class SparkUtils {

	/**
	 * 根据当前是否本地测试的配置
	 * 决定，如何设置 SparkConf的master
	 * @param conf
	 */
	public static void setMaster(SparkConf conf) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			conf.setMaster("local");
		}
	}
	
	/**
	 * 生成模拟数据
	 * 如果spark。local设置为true，则生成模拟数据，否则不生成
	 * @param sc
	 * @param sqlContext
	 */
	public static void mockData(JavaSparkContext sc,SQLContext sqlContext) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			MockData.mock(sc, sqlContext);
		}
	}
	
	/**
	 * 获取SQLContext
	 * 如果spark。local设置为ture，那么就创建SQLContext，否则创建HiveContext
	 * @param sc
	 * @return
	 */
	public static SQLContext getSQLContext(SparkContext sc) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc);
		}
	}
	
	/**
	 * 获取指定日期范围内的用户行为数据RDD
	 * @param sqlContext
	 * @param taskParam
	 * @return
	 */
	public static JavaRDD<Row> getActionRDDByDateRange(
			SQLContext sqlContext,JSONObject taskParam) {
		System.out.println("hanshulimian :" + taskParam);
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
	
		String sql = "select * "
				+ "from user_visit_action "
				+ "where date >='" + startDate + "' "
				+ "and date <='" + endDate + "'"; 
		
		DataFrame actionDF = sqlContext.sql(sql);
	
		/**
		 * 这里就可能发生问题
		 * 比如说，Spark SQL默认会给第一个stage设置了20个task，但是根据你的数据量以及
		 * 算法的复杂度，实际上，你需要1000个task去并行执行
		 * 
		 * 所以说，这里就可以对spark sql查询出来的RDD执行repartition重分区操作
		 */
		//return actionDF.javaRDD().repartition(100);
		
		return actionDF.javaRDD();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
