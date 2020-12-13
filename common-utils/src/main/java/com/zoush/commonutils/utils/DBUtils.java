package com.zoush.commonutils.utils;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据库连接工具
 * Druid连接池官方文档 https://github.com/alibaba/druid/wiki/DruidDataSource%E9%85%8D%E7%BD%AE%E5%B1%9E%E6%80%A7%E5%88%97%E8%A1%A8
 * @author zoushenghan
 */

public class DBUtils {

	private static Map<String, DruidDataSource> dataSources=Collections.synchronizedMap(new HashMap<String, DruidDataSource>());

	/**
	 * 获取连接
	 * @param DBIdentifier
	 * @return
	 * @throws SQLException
	 */
	public static synchronized Connection getConnection(String DBIdentifier) throws SQLException {
		return getDruidDataSource(DBIdentifier).getConnection();
	}

	/**
	 * 获取数据源
	 * @param DBIdentifier
	 * @return
	 */
	private static synchronized DruidDataSource getDruidDataSource(String DBIdentifier){
		if(!dataSources.containsKey(DBIdentifier)){
			createDataSource(DBIdentifier);
		}

		return dataSources.get(DBIdentifier);
	}

	/**
	 * 创建数据源
	 * @param DBIdentifier
	 */
	private static synchronized void createDataSource(String DBIdentifier){
		DruidDataSource dataSource=new DruidDataSource();

		dataSource.setName(DBIdentifier); //数据源名称，当存在多数据源时，方便区分和监控

		dataSource.setDriverClassName(PropertiesUtils.getProperty(DBIdentifier+".driver"));
		dataSource.setUrl(PropertiesUtils.getProperty(DBIdentifier+".url"));
		dataSource.setUsername(PropertiesUtils.getProperty(DBIdentifier+".username"));
		dataSource.setPassword(PropertiesUtils.getProperty(DBIdentifier+".password"));

		dataSource.setInitialSize(PropertiesUtils.getInteger(DBIdentifier+".initialSize", 1)); //初始连接数量，默认为0
		dataSource.setMaxActive(PropertiesUtils.getInteger(DBIdentifier+".maxActive", 20)); //最大连接数量，默认为8
		dataSource.setMinIdle(PropertiesUtils.getInteger(DBIdentifier+".minIdle", 1)); //最小连接数量，默认为0
		dataSource.setMaxWait(PropertiesUtils.getInteger(DBIdentifier+".maxWait", 60*1000)); //获取连接时最大等待时间，单位毫秒，默认会一直等待

		//用来检测连接是否有效的sql，要求是一个查询语句，常用select 'x'。如果validationQuery为null，testOnBorrow、testOnReturn、testWhileIdle都不会起作用
		dataSource.setValidationQuery(PropertiesUtils.getProperty(DBIdentifier+".validationQuery", "select 'x'"));

		//申请连接时执行validationQuery检测连接是否有效，做了这个配置会降低性能，默认为false
		dataSource.setTestOnBorrow(PropertiesUtils.getBoolean(DBIdentifier+".testOnBorrow", false));

		//归还连接时执行validationQuery检测连接是否有效，做了这个配置会降低性能，默认为false
		dataSource.setTestOnReturn(PropertiesUtils.getBoolean(DBIdentifier+".testOnReturn", false));

		//默认为true，建议配置为true，不影响性能，并且保证安全性。申请连接的时候检测，如果空闲时间大于timeBetweenEvictionRunsMillis，执行检测连接是否有效
		dataSource.setTestWhileIdle(PropertiesUtils.getBoolean(DBIdentifier+".testWhileIdle", true));

		//有两个含义：1. Destroy线程会检测连接的间隔时间，如果连接空闲时间大于等于minEvictableIdleTimeMillis则关闭物理连接。2. testWhileIdle的判断依据，详细看testWhileIdle属性的说明。默认为1分钟，单位毫秒
		dataSource.setTimeBetweenEvictionRunsMillis(PropertiesUtils.getLong(DBIdentifier+".timeBetweenEvictionRunsMillis", 60*1000));

		//连接保持空闲而不被驱逐的最小时间，默认为30分钟，单位毫秒
		dataSource.setMinEvictableIdleTimeMillis(PropertiesUtils.getLong(DBIdentifier+".minEvictableIdleTimeMillis", 5*60*1000));

		dataSources.put(DBIdentifier, dataSource);
	}

	/**
	 * 执行sql语句，如DML或DDL，不适用于查询
	 * @param DBIdentifier
	 * @param sql
	 * @throws SQLException
	 */
	public static synchronized void executeUpdate(String DBIdentifier, String sql) throws SQLException {
		Connection conn=null;
		Statement stat=null;
		try {
			conn=getConnection(DBIdentifier);
			conn.setAutoCommit(false);
			stat=conn.createStatement();
			stat.executeUpdate(sql);
			conn.commit();
		}
		finally {
			close(stat);
			close(conn);
		}
	}

	/**
	 * 关闭资源
	 * @param obj
	 */
	public static void close(Object obj){
		if(obj instanceof Connection){
			Connection conn=(Connection)obj;
			if(conn!=null){
				try{
					conn.close();
				}
				catch (Exception e){
					e.printStackTrace();
				}
			}
		}
		else if(obj instanceof PreparedStatement){
			PreparedStatement ps=(PreparedStatement)obj;
			if(ps!=null){
				try{
					ps.close();
				}
				catch (Exception e){
					e.printStackTrace();
				}
			}
		}
		else if(obj instanceof Statement){
			Statement stat=(Statement)obj;
			if(stat!=null){
				try{
					stat.close();
				}
				catch (Exception e){
					e.printStackTrace();
				}
			}
		}
		else if(obj instanceof ResultSet){
			ResultSet rs=(ResultSet)obj;
			if(rs!=null){
				try{
					rs.close();
				}
				catch (Exception e){
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) throws SQLException {
		Connection conn=DBUtils.getConnection("mysql.fruit");
		/*PreparedStatement ps=conn.prepareStatement("select * from user_info");
		ResultSet rs=ps.executeQuery();
		while(rs.next()){
			System.out.println(rs.getString("user_name")+","+rs.getLong("mobile"));
		}

		DBUtils.close(rs);
		DBUtils.close(ps);*/

		Connection conn2=DBUtils.getConnection("mysql.fruit");
		Connection conn3=DBUtils.getConnection("mysql.fruit");

		Connection conn4=DBUtils.getConnection("mysql.bigdata.upgrade");
		Connection conn5=DBUtils.getConnection("mysql.bigdata.upgrade");

		DBUtils.close(conn);
		DBUtils.close(conn2);
		DBUtils.close(conn3);
		DBUtils.close(conn4);
		DBUtils.close(conn5);
	}
}
