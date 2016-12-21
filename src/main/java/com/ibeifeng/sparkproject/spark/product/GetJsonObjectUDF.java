package com.ibeifeng.sparkproject.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import com.alibaba.fastjson.JSONObject;

/**
 * 
 * @author Administrator
 *
 */
public class GetJsonObjectUDF implements UDF2<String,String,String>{ //第一个是Json，第二个是json里面字段的名称，第三个是返回值

	private static final long serialVersionUID = 1L;

	public String call(String json, String field) throws Exception {
		try {
			JSONObject jsonObject = JSONObject.parseObject(json);
			return jsonObject.getString(field);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
