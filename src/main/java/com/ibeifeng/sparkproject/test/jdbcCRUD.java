package com.ibeifeng.sparkproject.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * jdbc增删改查师范
 * 
 * @author Administrator
 *
 */
public class jdbcCRUD {

	public static void main(String[] args) {
		// insert();
		// update();
		// delete();
		// select();
		preparedStatement();
	}

	/**
	 * 测试插入数据
	 */
	private static void insert() {
		// 定义数据库连接对象
		Connection conn = null;
		// 定义SQL语句执行句柄：Statement对象
		Statement stmt = null;

		try {
			// 1,加载驱动,我们都是面向java.sql包下的接口在编程，所以
			// 要想让JDBC代码能够真正操作数据库，那么就必须加载进来要操作的数据库的
			// 驱动类
			// Class.froName（）是Java提供的一种基于发射的方式，直接根据类的全名
			// 从类坐在的磁盘文件(.class文件)中加载类对应的聂荣，并创建对应的class对象
			Class.forName("com.mysql.jdbc.Driver");
			// 获取连接
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project", "root", "root");
			// 创建statement对象
			stmt = conn.createStatement();
			// 插入一条数据
			String sql = "insert into test_user(name,age) values ('李四',26)";
			int rtn = stmt.executeUpdate(sql);

			System.out.println("SQL语句影响了  " + rtn + "行");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (stmt != null) {
					stmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

	}

	private static void update() {
		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project", "root", "root");
			stmt = conn.createStatement();
			String sql = "update test_user set age=29 where name = '李四' ";
			int rtn = stmt.executeUpdate(sql);
			System.out.println("SQL语句影响了  " + rtn + "行");
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}

	private static void delete() {
		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project", "root", "root");
			stmt = conn.createStatement();
			String sql = "delete from test_user where name = '李四' ";
			int rtn = stmt.executeUpdate(sql);
			System.out.println("SQL语句影响了  " + rtn + "行");
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}

	private static void select() {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project", "root", "root");
			stmt = conn.createStatement();
			String sql = "select * from test_user";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				int id = rs.getInt(1);
				String name = rs.getString(2);
				int age = rs.getInt(3);
				System.out.println("id=" + id + ", name=" + name + ", age=" + age);
			}
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}

	/**
	 * 好处：1，防止 sql注入，2，性能更好
	 */
	private static void preparedStatement() {
		Connection conn = null;
		PreparedStatement pstmt = null;

		try {

			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project", "root", "root");
			String sql = "insert into test_user(name,age) values (?,?)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, "李四");
			pstmt.setInt(2, 26);
			int rtn = pstmt.executeUpdate();

			System.out.println("SQL语句影响了  " + rtn + "行");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (pstmt != null) {
					pstmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

	}
}
