package com.ibeifeng.sparkproject.test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * JDBC辅助组件测试类
 * @author Administrator
 *
 */
public class JDBCHelperTest {

	public static void main(String[] args) {
		//获取JDBCHelper单例
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		/*//测试普通的增删改语句
		jdbcHelper.executeUpdate(
				"insert into test_user(name,age) values(?,?)",
				new Object[]{"王二",28});*/
		
		//测试查询语句
		/*final Map<String, Object> testUser = new HashMap<String, Object>();
		
		jdbcHelper.executeQuery(
				"select name,age from test_user where id = ?",
				new Object[]{5}, 
				new JDBCHelper.QueryCallback() {
					
					public void process(ResultSet rs) throws Exception {
						if(rs.next()) {
							String name = rs.getString(1);
							int age = rs.getInt(2);
							
							//匿名内部类，如果要访问外部类中的一些成员，比如方法内的
							//局部变量，那么，必须将局部变量，声明为final类型，才
							//可以访问，否则访问不了
							testUser.put("name", name);
							testUser.put("age", age);
						}
					}
				});
		System.out.println(testUser.get("name") + ":" + testUser.get("age"));*/
		
		//测试批量执行SQL语句
		String sql = "insert into test_user(name,age) values(?,?)";
		List<Object[]> paramsList = new ArrayList<Object[]>();
		paramsList.add(new Object[]{"麻子",30});
		paramsList.add(new Object[]{"王五",35});
		jdbcHelper.executeBatch(sql, paramsList);
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
	}
}
