package com.zoush.sparkorder.act

import com.zoush.commonutils.utils.{DateUtil, MySQLSparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.zoush.commonutils.utils.CommonTool.printMessage

/**
  * 拼多多-染色计划
  * @author zoushenghan
  */

object DwDyePlanOdrCnt {
	def main(args: Array[String]): Unit= {
		val appName=this.getClass.getSimpleName.replaceAll("\\$", "") //应用名称

		val conf=new SparkConf()
		conf.setAppName(appName)
		conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")

		val ss=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
		val sc=ss.sparkContext
		import ss.sql

		//设置日记消息级别
		if (args.length >= 1 && args(0) == "INFO") { //只有当第1个参数为INFO时，才设置消息日志级别为INFO
			sc.setLogLevel("INFO")
			Logger.getLogger("org").setLevel(Level.INFO)
		}
		else {
			sc.setLogLevel("WARN")
			Logger.getLogger("org").setLevel(Level.WARN)
		}

		val today=DateUtil.getCurrDayLong //今天

		printMessage("******  parameter list start  ******")
		printMessage(s"today=${today}")
		printMessage(s"today=${today}")
		printMessage("******  parameter list end  ******")

		val startTime=System.currentTimeMillis() //记录程序开始执行时间

		sql("use peanut")

		sql("drop table if exists dw_dye_plan_odr_cnt")

		sql("""
			create table dw_dye_plan_odr_cnt as
			select cast(father_id as bigint)  user_id
				,substr(create_time,1,10)  date
				,count(case when status<>4 and goods_price>2 then order_id else null end)  order_count
			from pdd_order_info_h06
			where substr(create_time,1,19) between '2020-12-16 15:00:00' and '2020-12-31 15:00:00' --正式活动时间为2020-12-16 15:00:00至2020-12-31 15:00:00
				and status<>5
				and is_direct=1
				and father_id is not null
			group by cast(father_id as bigint)
				,substr(create_time,1,10)
		""")

		printMessage("tag 1")

		val tgtDF=sql("select * from dw_dye_plan_odr_cnt")

		printMessage(s"cnt=${tgtDF.count()}")
		printMessage(s"numPartitions=${tgtDF.rdd.getNumPartitions}")

		MySQLSparkUtils.upsert(tgtDF, "mysql.smt.pdd-mall", "daily_order_info", showSql = true)

		printMessage("save over")

		printMessage("all over, elapsed time is "+String.format("%.2f", Double.box((System.currentTimeMillis()-startTime)*1.0/1000/60))+" minutes")
	}
}
