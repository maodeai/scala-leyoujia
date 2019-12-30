package cn.com.leyoujia.dca

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer


object sparkSqljob_analysis {
  Logger.getLogger("org.apapche.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apapche.kafka").setLevel(Level.WARN)
  Logger.getLogger("org.apapche.hadoop").setLevel(Level.WARN)
  Logger.getLogger("org.apapche.flume").setLevel(Level.WARN)
  Logger.getLogger("org.apapche.zookeeper").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    //    val startDate = getday(-1)
    //    val endDate = getday(-8)
    //    println("startDate" + startDate)
    //    println("endDate" + endDate)
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()


    val group_df = spark.sql("SELECT id, GROUP_NAME, GROUP_RULE_SQL FROM tmp.ANALYSIS_USER_GROUP WHERE status = 1 AND group_name  != '今日访问用户' and GROUP_RULE_SQL is not null and GROUP_RULE_SQL !=''")

    group_df.show()

    val group_sql = group_df.rdd.map(
      x => {
        val id = x.getAs[Int]("id")
        val group_sql = x.getAs[String]("GROUP_RULE_SQL")
        (id, group_sql)
      }
    ).map(y => {
      val id = y._1
      val sql = y._2.replaceAll("LYJ_DW_V3", "dw")
      (id, sql)
    }).filter(_._2.nonEmpty).collect()


    val finalrdd = group_sql.map(rdd => {
      val id = rdd._1
      val sql = rdd._2
      val tcount = spark.sql(s"select count(DISTINCT UUID) as num from dw.WECHAT_USER_INFO_LATEST30D  where and ${sql}").first().getAs[Int]("num")
      val lseverd = spark.sql(s"select count(DISTINCT uuid) as num from dw.WECHAT_USER_INFO_LATEST30D where uuid in ( SELECT distinct uuid FROM dw.WECHAT_USER_INFO_LATEST30D WHERE ${sql} ) and L_DATE >= '2019-07-10' and L_DATE <= '2019-07-16' ").first().getAs[Int]("num")
      (id, tcount, lseverd)
    })


    //    group_sql.foreachPartition(iter => {
    //      iter.foreach(rdd => {
    //        val id = rdd._1
    //        val sql = rdd._2
    //        val tcount = spark.sql(s"select count(DISTINCT UUID) as num from dw.WECHAT_USER_INFO_LATEST30D  where  ${sql}").first().getAs[Int]("num")
    //        val lseverd = spark.sql(s"select count(DISTINCT uuid) as num from dw.WECHAT_USER_INFO_LATEST30D where uuid in ( SELECT distinct uuid FROM dw.WECHAT_USER_INFO_LATEST30D WHERE ${sql} ) and L_DATE >= '2019-07-10' and L_DATE <= '2019-07-16' ").first().getAs[Int]("num")
    //
    //      })
    //    })


    val groupCount = new ArrayBuffer[(Any, Any)]()
    for (sqla <- group_sql) {
      val id = sqla._1.toInt
      val sql = sqla._2
      val tcount = spark.sql(s"select count(DISTINCT UUID) as num from dw.WECHAT_USER_INFO_LATEST30D  where  $sql").first().getAs("num")
      val lseverd = spark.sql(s"select count(DISTINCT uuid) as num from dw.WECHAT_USER_INFO_LATEST30D where uuid in ( SELECT distinct uuid FROM dw.WECHAT_USER_INFO_LATEST30D WHERE $sql ) and L_DATE >= '2019-07-10' and L_DATE <= '2019-07-16' ").first().getAs("num")
      groupCount += ((id, tcount))
    }


    //    val groupRDD = spark.sparkContext.parallelize(groupCount).map(x => {
    //      Row(x._1.toString, x._2.toString, x._3.toString)
    //    })

    val schemaString = "id,group_number,group_active_count"

    val schema =
      StructType(
        schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    //    val groupRow = spark.createDataFrame(groupRDD, schema)


    /* val group_final = group_sql_rdd.map(rdd => {
       val id = rdd._1
       val sql = rdd._2
       val tcount = spark.sql(s"select count(DISTINCT UUID) as num from tmp.WECHAT_USER_INFO_LATEST30D  where and ${sql} ")
       //val lseverd = spark.sql(s"select count(DISTINCT uuid) as num from dw.WECHAT_USER_INFO_LATEST30D where uuid in ( SELECT distinct uuid FROM dw.WECHAT_USER_INFO_LATEST30D WHERE ${rdd._2} ) and L_DATE >= ${startDate} and L_DATE <= ${endDate} ").first().getAs("num")
       Row(id, tcount)
     })*/


    //    val groupDf = spark.createDataFrame(groupRow, schema)
    //    groupDf.show()
    //    groupDf.write.format("csv").mode("overwrite").save("/tmp/mh/group")
    //    groupDf.select("id", "group_number").write.mode("overwrite").saveAsTable("tmp.analysis_user_group_count")
    //    spark.close()
    //  }


    //    def getday(be: Int): String = {
    //      var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    //      var cal: Calendar = Calendar.getInstance()
    //      cal.add(Calendar.DATE, be)
    //      var yesterday = dateFormat.format(cal.getTime())
    //      yesterday
    //    }


  }
}
