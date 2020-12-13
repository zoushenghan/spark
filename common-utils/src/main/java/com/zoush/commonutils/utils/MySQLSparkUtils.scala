package com.zoush.commonutils.utils

import java.util.Properties

import org.apache.spark.sql.DataFrame

/**
  * MySQL与Spark交互工具类
  * @author zoushenghan
  */

object MySQLSparkUtils {

    /**
      * 创建sql
      * @param table
      * @param cols
      * @param updateCols
      * @param isAccumulate 是否对字段累加
      * @return
      */
    def createSql(table: String, cols: Array[String], updateCols: Array[String], isAccumulate: Boolean = false): String = {
        val sqlStr=new StringBuilder
        val sqlValue=new StringBuilder

        sqlStr.append(s"insert into ${table}(")
        sqlValue.append(s" values(")

        for(i <- cols.indices){
            val field=cols(i)
            sqlStr.append(s"${field},")
            sqlValue.append(s"?,")
        }

        sqlStr.replace(sqlStr.length-1, sqlStr.length, ")").append(sqlValue.replace(sqlValue.length-1, sqlValue.length, ") on duplicate key update "))

        for(i <- updateCols.indices){
            val field=updateCols(i)
            if(isAccumulate){
                sqlStr.append(s"${field}=${field}+values(${field}),")
            }
            else {
                sqlStr.append(s"${field}=values(${field}),")
            }
        }

        sqlStr.substring(0, sqlStr.length-1)
    }

    /**
      * 将DataFrame写入表，如果记录已存在，则更新，如果不存在则插入
      * @param df
      * @param DBIdentifier
      * @param table
      * @param updateCols
      * @param isAccumulate 是否对字段累加
      * @param batchSize
      * @param numPartitions
      * @param props
      * @param showSql
      */
    def upsert(df: DataFrame, DBIdentifier: String, table: String, updateCols: Array[String] = null, isAccumulate: Boolean = false, batchSize: Int = 1000, numPartitions: Int = 0, props: Properties = null, showSql: Boolean = false): Unit = {
        val cols=df.columns
        var tmpUpdateCols=cols
        if(updateCols!=null){
            tmpUpdateCols=updateCols
        }

        val sql=createSql(table, cols, tmpUpdateCols, isAccumulate)

        if(showSql){
            println(s"====  ${sql}  ====")
        }

        var dfRep=df
        if(numPartitions>0){
            dfRep=df.repartition(numPartitions)
        }

        dfRep.foreachPartition(iterator=>{
            if(!iterator.isEmpty){
                val dbUtil=new DBUtilSimpleMySQL(DBIdentifier)

                val defaultProps=new Properties()
                defaultProps.put("rewriteBatchedStatements", "true")
                defaultProps.put("useServerPrepStmts", "false")

                if(props!=null){
                    defaultProps.putAll(props)
                }

                val conn=dbUtil.getConnection(defaultProps)
                val ps=conn.prepareStatement(sql)

                var cnt=0

                try{
                    conn.setAutoCommit(false)

                    iterator.foreach(row=>{
                        for(i <- cols.indices){
                            val value=row.get(i)
                            ps.setObject(i+1, value)
                        }

                        ps.addBatch()
                        cnt+=1
                        if(cnt%batchSize == 0){
                            ps.executeBatch()
                            conn.commit()
                            println(s"commit over, currentThread=${Thread.currentThread().getId}, cnt=${cnt}")
                        }
                    })

                    ps.executeBatch()
                    conn.commit()
                    println(s"final commit over, currentThread=${Thread.currentThread().getId}, cnt=${cnt}")
                }
                catch {
                    case e: Exception => throw new RuntimeException("Access database failed", e)
                }
                finally {
                    dbUtil.close(ps)
                    dbUtil.close(conn)
                }
            }
        })
    }
}
