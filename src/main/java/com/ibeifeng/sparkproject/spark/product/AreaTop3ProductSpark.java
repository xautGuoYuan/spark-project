package com.ibeifeng.sparkproject.spark.product;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.IAreaTop3ProductDAO;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.AreaTop3Product;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.SparkUtils;

import scala.Tuple2;

/**
 * 各区域top3热门商品统计Spark作业
 * 
 * @author Administrator
 *
 */
public class AreaTop3ProductSpark {

	public static void main(String[] args) {
		//创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("AreaTop3ProductSpark");
		SparkUtils.setMaster(conf);
		
		//构建Spark上下文
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
//		sqlContext.setConf("spark.sql.shuffle.partitions", "1000"); 
//		sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "20971520");
		
		//注册自定义函数--最后一个参数是返回类型
		sqlContext.udf().register("concat_long_string",
				new ConcatLongStringUDF(),DataTypes.StringType);
		sqlContext.udf().register("get_json_object",
				new GetJsonObjectUDF(),DataTypes.StringType);
		sqlContext.udf().register("random_prefix", 
				new RandomPrefixUDF(), DataTypes.StringType);
		sqlContext.udf().register("remove_random_prefix", 
				new RemoveRandomPrefixUDF(), DataTypes.StringType);
		sqlContext.udf().register("group_concat_distinct",
				new GroupConcatDistinctUDAF());
		
		//准备模拟数据
		SparkUtils.mockData(sc, sqlContext);
		
		//获取命令行传入的taskid，查询对应的任务参数
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		long taskid = ParamUtils.getTaskIdFromArgs(args, 
				Constants.SPARK_LOCAL_TASKID_PRODUCT);
		Task task = taskDAO.findById(taskid);
		
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		
		//查询用户指定日期范围内的点击行为数据(city_id)
		JavaPairRDD<Long,Row> cityid2clickActionRDD = getcityid2ClickActionRDDByDate(
				sqlContext, startDate, endDate);
		
		//从MySQL中查询城市信息
		JavaPairRDD<Long,Row> cityid2cityInfoRDD = getcityid2CityInfoRDD(sqlContext);
		
		//生成点击商品基础信息临时表
		generateTempClickProductBasicTable(sqlContext, 
				cityid2clickActionRDD, cityid2cityInfoRDD);
		
		//生成各区域商品点击次数的临时表
		generateAreaProductClickCountTable(sqlContext);
		
		//生成包含完整商品信息的各区域各商品点击次数的临时表
		generateTempAreaProductFullProductClickCountTable(sqlContext);
		
		//使用开窗函数获取各个区域内点击次数排名前3的热门商品
		JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sqlContext);
		
		//写入mysql
		//因为获取的数据比较小，所以采用collect()到本地，然后批量插入到mysql，一次性写入
		
		List<Row> rows = areaTop3ProductRDD.collect();
		persistAreaTop3Product(taskid, rows);
		
		sc.close();	
	}
	
	

	/**
	 * 查询指定日期范围内的点击行为数据
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	private static JavaPairRDD<Long,Row> getcityid2ClickActionRDDByDate(
			SQLContext sqlContext,String startDate,String endDate) {
		String sql = "SELECT "
						+ "city_id,"
						+ "click_product_id product_id "
					 + "FROM user_visit_action "
					 + "WHERE click_product_id IS NOT NULL " //代表着点击行为
					 + "AND date >= '" + startDate + "' "
					 + "AND date <= '" + endDate + "'";
		
		DataFrame clickActionDF = sqlContext.sql(sql);
		
		JavaRDD<Row> clickActionRDD =  clickActionDF.javaRDD();
		
		JavaPairRDD<Long,Row> cityid2clickActionRDD = clickActionRDD.mapToPair(
				
				new PairFunction<Row, Long, Row>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Long, Row> call(Row row) throws Exception {
						Long cityid = row.getLong(0);
						return new Tuple2<Long, Row>(cityid, row);
					}
				});
		
		return cityid2clickActionRDD;
		
	}
	
	/**
	 * 使用spark SQL从MySQL中查询城市信息
	 * @param sqlContext
	 * @return
	 */
	private static JavaPairRDD<Long,Row> getcityid2CityInfoRDD(SQLContext sqlContext) {
		//构建MySQL连接配置信息（直接从配置文件中获取）
		String url = null;
		String user = null;
		String password = null;
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			url = ConfigurationManager.getProperty(Constants.JDBC_URL);
			user = ConfigurationManager.getProperty(Constants.JDBC_USER);
			password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
			
		} else {
			url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
			user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
			password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
		}
		
		Map<String, String> options = new HashMap<String, String>();
		options.put("url", url);
		options.put("dbtable", "city_info");
		options.put("user", user);
		options.put("password",password);
		
		//通过SQLContext去从MySQL中查询数据
		DataFrame cityInfoDF = sqlContext.read().format("jdbc").options(options).load();
		
		//返回RDD
		JavaRDD<Row> cityInfoRDD =  cityInfoDF.javaRDD();
		
		JavaPairRDD<Long, Row> cityid2cityInfoRDD = cityInfoRDD.mapToPair(
				
				new PairFunction<Row, Long, Row>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Long, Row> call(Row row) throws Exception {
						long cityid = Long.valueOf(String.valueOf(row.get(0)));
						return new Tuple2<Long, Row>(cityid, row);
					}
				});
		
		return cityid2cityInfoRDD;
	}
	
	/**
	 * 生成点击商品基础信息临时表
	 * @param cityid2clickActionRDD
	 * @param cityid2cityInfoRDD
	 */
	private static void generateTempClickProductBasicTable(
			SQLContext  sqlContext,
			JavaPairRDD<Long, Row> cityid2clickActionRDD,
			JavaPairRDD<Long, Row> cityid2cityInfoRDD) {
		JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD = 
				cityid2clickActionRDD.join(cityid2cityInfoRDD);
		
		//强上面的JavaPairRDD，转换成一个JavaRDD<Row>，这个样子才能将RDD转换成DataFrame
		JavaRDD<Row> mappedRDD = joinedRDD.map(
				
				new Function<Tuple2<Long,Tuple2<Row,Row>>, Row>() {

					private static final long serialVersionUID = 1L;

					public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple) throws Exception {
						long cityid = tuple._1;
						Row clickAction = tuple._2._1;
						Row cityInfo = tuple._2._2;
						
						long productid = clickAction.getLong(1);
						String cityName = cityInfo.getString(1);
						String area = cityInfo.getString(2);
						
						return RowFactory.create(cityid,cityName,area,productid);
						
					}
				});
		
		//基于JavaRDD<Row>的格式，就可以将其转换为DateFrame
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
		structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));
		
		StructType schema = DataTypes.createStructType(structFields);
		
		DataFrame df = sqlContext.createDataFrame(mappedRDD, schema);
		
		//注册成临时表
		df.registerTempTable("tmp_click_product_basic");
		System.out.println("tmp_click_product_basic");
		df.show();
		
	}
	
	/**
	 * 生成各区域各商品点击次数临时表
	 * @param sqlContext
	 */
	private static void generateAreaProductClickCountTable(SQLContext sqlContext) {
		//按照area和product_id两个字段进行分组
		//计算出各区域各商品的点击次数
		//可以获取到每个area下的每个product_id的城市信息拼接起来的串
		String sql = 
				"SELECT "
					+ "area,"
					+ "product_id,"
					+ "count(*) click_count,"
					+ "group_concat_distinct(concat_long_String(city_id,city_name,':')) city_infos "
				+ "FROM tmp_click_product_basic "
				+ "GROUP BY area,product_id";
		
		/**
		 * 双重group by
		 */
		
//		String _sql = 
//				"SELECT "
//					+ "product_id_area,"
//					+ "count(click_count) click_count,"
//					+ "group_concat_distinct(city_infos) city_infos "
//				+ "FROM ( "
//					+ "SELECT "
//						+ "remove_random_prefix(product_id_area) product_id_area,"
//						+ "click_count,"
//						+ "city_infos "
//					+ "FROM ( "
//						+ "SELECT "
//							+ "product_id_area,"
//							+ "count(*) click_count,"
//							+ "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos " 
//						+ "FROM ( "
//							+ "SELECT "  
//								+ "random_prefix(concat_long_string(product_id,area,':'), 10) product_id_area,"
//								+ "city_id,"
//								+ "city_name "
//							+ "FROM tmp_click_product_basic "
//						+ ") t1 "
//						+ "GROUP BY product_id_area "
//					+ ") t2 "  
//				+ ") t3 "
//				+ "GROUP BY product_id_area "; 
		
		//执行sql
		DataFrame df = sqlContext.sql(sql);
		System.out.println("tmp_area_product_click_count");
		df.show();
		
		//各区域各商品的点击次数（以及额外的城市列表）
		df.registerTempTable("tmp_area_product_click_count");
	}
	
	/**
	 * 生成各区域商品点击次数临时表（包含了商品的完整信息）
	 * @param sqlContext
	 */
	private static void generateTempAreaProductFullProductClickCountTable(SQLContext sqlContext){
		//通过product_id获取product_name和product_status
		String sql = 
				"SELECT "
					+ "tapcc.area,"
					+ "tapcc.product_id,"
					+ "tapcc.click_count,"
					+ "tapcc.city_infos,"
					+ "pi.product_name,"
					+ "if(get_json_object(pi.extend_info,'product_status')='0','self','Third Party') product_status "
				+ "FROM tmp_area_product_click_count tapcc "
				+ "JOIN product_info pi ON tapcc.product_id=pi.product_id";
		
//		JavaRDD<Row> rdd = sqlContext.sql("select * from product_info").javaRDD();
//		JavaRDD<Row> flattedRDD = rdd.flatMap(new FlatMapFunction<Row, Row>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Iterable<Row> call(Row row) throws Exception {
//				List<Row> list = new ArrayList<Row>();
//				
//				for(int i = 0; i < 10; i ++) {
//					long productid = row.getLong(0);
//					String _productid = i + "_" + productid;
//					
//					Row _row = RowFactory.create(_productid, row.get(1), row.get(2));
//					list.add(_row);
//				}
//				
//				return list;
//			}
//			
//		});
//		
//		StructType _schema = DataTypes.createStructType(Arrays.asList(
//				DataTypes.createStructField("product_id", DataTypes.StringType, true),
//				DataTypes.createStructField("product_name", DataTypes.StringType, true),
//				DataTypes.createStructField("product_status", DataTypes.StringType, true)));
//		
//		DataFrame _df = sqlContext.createDataFrame(flattedRDD, _schema);
//		_df.registerTempTable("tmp_product_info");  
//		
//		String _sql = 
//				"SELECT "
//					+ "tapcc.area,"
//					+ "remove_random_prefix(tapcc.product_id) product_id," 
//					+ "tapcc.click_count,"
//					+ "tapcc.city_infos,"
//					+ "pi.product_name,"
//					+ "if(get_json_object(pi.extend_info,'product_status')=0,'自营商品','第三方商品') product_status "
//				+ "FROM ("
//					+ "SELECT "
//						+ "area,"
//						+ "random_prefix(product_id, 10) product_id,"
//						+ "click_count,"
//						+ "city_infos "
//					+ "FROM tmp_area_product_click_count "
//				+ ") tapcc "
//				+ "JOIN tmp_product_info pi ON tapcc.product_id=pi.product_id ";
		DataFrame df = sqlContext.sql(sql);
		System.out.println("tmp_area_fullprod_click_count");
		df.show();
		
		df.registerTempTable("tmp_area_fullprod_click_count");
	}
	
	/**
	 * 获取各区域top3热门商品
	 * @param sqlContext
	 * @return
	 */
	private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext) {
		//area:华北，华东，华南，华中，西北，西南，东北
		//A级：华北和华东
		//B级：华南和华中
		//C级：西北和西南
		//D级：东北
		
		String sql = 
				"SELECT "
					+ "area,"
					+ "CASE "
						+ "WHEN area='China North' OR area='China East' THEN 'A level' "
						+ "WHEN area='China South' OR area='China Middle' THEN 'B level' "
						+ "WHEN area='West North' OR area='West South' THEN 'C level' "
						+ "ELSE 'D level' "
					+ "END area_level,"
					+ "product_id,"
					+ "click_count,"
					+ "city_infos,"
					+ "product_name,"
					+ "product_status "
				+ "FROM ("
					+ "SELECT "
						+ "area,"
						+ "product_id,"
						+ "click_count,"
						+ "city_infos,"
						+ "product_name,"
						+ "product_status,"
						+ "ROW_NUMBER() OVER (PARTITION BY area ORDER BY click_count DESC) rank "
					+ "FROM tmp_area_fullprod_click_count "
				+ ") t "
				+ "WHERE rank<=3";
		
		DataFrame df = sqlContext.sql(sql);
		System.out.println("窗口函数：");
		df.show();
		return df.javaRDD();
	}
	
	/**
	 * 将计算出来的各区域top3热门商品写入MySQL中
	 * @param rows
	 */
	private static void persistAreaTop3Product(Long taskid,List<Row> rows) {
		List<AreaTop3Product> areaTop3Products = new ArrayList<AreaTop3Product>();
		
		for(Row row : rows) {
			System.out.println(row);
			AreaTop3Product areaTop3Product = new AreaTop3Product();
			areaTop3Product.setTaskid(taskid); 
			areaTop3Product.setArea(row.getString(0));  
			areaTop3Product.setAreaLevel(row.getString(1));  
			areaTop3Product.setProductid(row.getLong(2)); 
			areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));    
			areaTop3Product.setCityInfos(row.getString(4));  
			areaTop3Product.setProductName(row.getString(5));  
			areaTop3Product.setProductStatus(row.getString(6));  
			areaTop3Products.add(areaTop3Product);
		}
		
		IAreaTop3ProductDAO areTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
		areTop3ProductDAO.insertBatch(areaTop3Products);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
