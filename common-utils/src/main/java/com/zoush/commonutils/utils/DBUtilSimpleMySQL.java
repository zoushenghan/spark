package com.zoush.commonutils.utils;

import java.sql.*;
import java.util.Map;
import java.util.Properties;

/**
 * 数据库连接工具类-MySQL
 * 简易版本
 * @author zoushenghan
 */

public class DBUtilSimpleMySQL {

	private String driver;
	private String url;
	private String username;
	private String password;

	public DBUtilSimpleMySQL(String dbIdentifier) {
		driver=PropertiesUtils.getProperty(dbIdentifier+".driver");
		url=PropertiesUtils.getProperty(dbIdentifier+".url");
		username=PropertiesUtils.getProperty(dbIdentifier+".username");
		password=PropertiesUtils.getProperty(dbIdentifier+".password");

		try {
			Class.forName(driver);
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException("Database connection initialize failed", e);
		}
	}

	/**
	 * 获取连接
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		return DriverManager.getConnection(url, username, password);
	}

	/**
	 * 获取连接
	 * @param props
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection(Properties props) throws SQLException {
		props.put("user", username);
		props.put("password", password);

		return DriverManager.getConnection(url, props);
	}

	/**
	 * 执行sql语句，如DML或DDL，不适用于查询
	 * @param sql
	 * @throws SQLException
	 */
	public void executeUpdate(String sql) throws SQLException {
		Connection conn=null;
		Statement stat=null;
		try {
			conn=this.getConnection();
			conn.setAutoCommit(false);
			stat=conn.createStatement();
			stat.executeUpdate(sql);
			conn.commit();
		}
		finally {
			this.close(stat);
			this.close(conn);
		}
	}

	/**
	 * 关闭连接
	 * @param conn
	 */
	public void close(Connection conn) {
		if (conn!=null) {
			try {
				conn.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 关闭PreparedStatement
	 * @param ps
	 */
	public void close(PreparedStatement ps) {
		if (ps!=null) {
			try {
				ps.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 关闭Statement
	 * @param stat
	 */
	public void close(Statement stat) {
		if (stat!=null) {
			try {
				stat.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 关闭ResultSet
	 * @param rs
	 */
	public void close(ResultSet rs) {
		if (rs!=null) {
			try {
				rs.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		Properties defaultProps=new Properties();
		defaultProps.put("rewriteBatchedStatements", "true");
		defaultProps.put("useServerPrepStmts", "false");

		Properties p=new Properties();
		p.putAll(defaultProps);
		p.put("a", "1");

		for(Map.Entry<Object, Object> e: p.entrySet()){
			System.out.println(e.getKey()+"="+e.getValue());
		}
	}
}
