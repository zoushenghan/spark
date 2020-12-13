package com.zoush.sparkuser.act

import com.zoush.commonutils.utils.{DateUtil, MySQLSparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.zoush.commonutils.utils.CommonTool.printMessage

/**
  * 拼多多-高佣计划活动-每30分钟执行一次
  * @author zoushenghan
  */

object DwHighCommEstInfo {
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
		printMessage("******  parameter list end  ******")

		val startTime=System.currentTimeMillis() //记录程序开始执行时间

		sql("use peanut")

		val df=sql("""
			select
				 a.order_id
				,substr(regexp_replace(a.create_time,'-',''),1,8)  order_date
				,substr(regexp_replace(a.create_time,'-',''),1,6)  order_month
				,substr(regexp_replace(a.balance_time,'-',''),1,8)  balance_date
				,substr(regexp_replace(a.balance_time,'-',''),1,6)  balance_month
				,case when a.type='1' then 0 else a.estimate_commission end  estimate_commission --活动订单-预估佣金置为0
				,case when a.type='1' then 0 else a.balance_sum end  balance_sum --活动订单-结算佣金置为0
				,a.balance_flag
				,a.father_id
				,a.grandfather_id
				,a.operator
			  --,a.create_time
				,a.status
			  --,a.balance_time
				,substr(a.balance_time,1,19)  balance_time
				,case when a.status<>4 then a.goods_price else 0 end  goods_price --有效销售额

				,case when substr(a.create_time,1,10)>='2020-10-01' then (1-0.04)*0.58/1.07
				    when substr(a.create_time,1,10)>='2020-08-01' then 0.55/1.07
				    else (1-0.09)*0.55/1.07
				 end  fstLvlRt --一级分佣比例

				,case when substr(a.create_time,1,10)>='2020-10-01' then (1-0.04)*0.14/1.07
				    when substr(a.create_time,1,10)>='2020-08-01' then 0.15/1.07
				    else (1-0.09)*0.05/1.07
				 end  secLvlRt --二级分佣比例

				,case when substr(a.create_time,1,10)>='2020-10-01' then (1-0.04)*0.15/1.07
				    when substr(a.create_time,1,10)>='2020-08-01' then 0.15/1.07
				    else (1-0.09)*0.22/1.07
				 end  thrLvlRt --三级分佣比例

				,case when substr(a.create_time,1,10)>='2020-10-01' then (1-0.04)*(0.58+0.14+0.15)/1.07
				    when substr(a.create_time,1,10)>='2020-08-01' then (0.55+0.15+0.15)/1.07
				    else (1-0.09)*(0.55+0.05+0.22)/1.07
				 end  oprSlfBuyOdrRt --运营商分佣比例-运营商自购订单

				,case when substr(a.create_time,1,10)>='2020-10-01' then (1-0.04)*(0.14+0.15)/1.07
				    when substr(a.create_time,1,10)>='2020-08-01' then (0.15+0.15)/1.07
				    else (1-0.09)*(0.05+0.22)/1.07
				 end  oprFstFasOdrRt --运营商分佣比例-直属粉丝订单

				,cast((case when a.order_type=2102 then coalesce(a.father_est_subsidy,0) else 0 end) as double)  father_est_subsidy --一级预估补贴
				,cast((case when a.order_type=2102 then coalesce(a.father_bal_subsidy,0) else 0 end) as double)  father_bal_subsidy --一级结算补贴
				,coalesce(a.order_type,0)  order_type --订单类型: 2102-拼多多超级返
			from pdd_order_info_h03 a
			where a.father_id is not null
				and a.status<>5 --剔除待付款订单
				and (
				    substr(a.create_time,1,10)<'2020-07-07' or
				    (substr(a.create_time,1,10)>='2020-07-07' and a.is_direct=1)
				)
		""")
		
		df.createOrReplaceTempView("spark_order_info")

		// 1. 对应1人-运营商自购订单
		val df1=sql("""
			--运营商
			select operator  user_id
			    ,sum(case when order_month='202011' then estimate_commission*oprSlfBuyOdrRt+father_est_subsidy else 0 end)  commission_202011
				,sum(case when order_month='202012' then estimate_commission*oprSlfBuyOdrRt+father_est_subsidy else 0 end)  commission_202012
			from spark_order_info
			where father_id=grandfather_id and grandfather_id=operator
			group by operator
		""")

		// 2. 对应2人-直属粉丝订单
		val df21=sql("""
			--2.1 超级会员
			select father_id  user_id
			    ,sum(case when order_month='202011' then estimate_commission*fstLvlRt+father_est_subsidy else 0 end)  commission_202011
				,sum(case when order_month='202012' then estimate_commission*fstLvlRt+father_est_subsidy else 0 end)  commission_202012
			from spark_order_info
			where father_id<>grandfather_id and grandfather_id=operator
			group by father_id
		""")

		// 3. 对应3人-推荐粉丝订单
		val df31=sql("""
			--3.1 超级会员
			select father_id  user_id
			    ,sum(case when order_month='202011' then estimate_commission*fstLvlRt+father_est_subsidy else 0 end)  commission_202011
				,sum(case when order_month='202012' then estimate_commission*fstLvlRt+father_est_subsidy else 0 end)  commission_202012
			from spark_order_info
			where father_id<>grandfather_id and grandfather_id<>operator and father_id<>operator
			group by father_id
		""")

		val dfUnion=df1.union(df21).union(df31)
		dfUnion.createOrReplaceTempView("spark_order_info_all")

		printMessage("tag 1")

		sql("drop table if exists dw_high_commission_estimate_info")

		sql("""
			create table dw_high_commission_estimate_info as
			select cast(user_id as bigint)  user_id
			    ,cast(sum(commission_202011) as decimal(10,2))  november_estimate
				,cast(sum(commission_202012) as decimal(10,2))  december_estimate
			from spark_order_info_all
			group by cast(user_id as bigint)
		""")

		printMessage("tag 2")

		val tgtDF=sql("select * from dw_high_commission_estimate_info")

		printMessage(s"cnt=${tgtDF.count()}")
		printMessage(s"numPartitions=${tgtDF.rdd.getNumPartitions}")

		MySQLSparkUtils.upsert(tgtDF, "mysql.smt.pdd-mall", "high_commission_estimate_info", showSql = true)

		printMessage("save over")

		printMessage("all over, elapsed time is "+String.format("%.2f", Double.box((System.currentTimeMillis()-startTime)*1.0/1000/60))+" minutes")
	}
}
