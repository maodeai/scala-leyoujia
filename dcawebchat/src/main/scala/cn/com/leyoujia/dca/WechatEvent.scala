package cn.com.leyoujia.dca

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.lib.CombineTextInputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable;

object WechatEvent {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apapche.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apapche.kafka").setLevel(Level.WARN)
    Logger.getLogger("org.apapche.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apapche.flume").setLevel(Level.WARN)
    Logger.getLogger("org.apapche.zookeeper").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .appName("free-wechat-event")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    //val path=args(0)
    process(spark)


    spark.table("")


    val basic = spark.table("tmp.FCT_USER_BASIC_INFO")
    basic.repartition(1).write.format("csv").save("/tmp/mh/basic2")
    basic.write.format("csv").save("/tmp/mh/basic")


    val bhv = spark.table("tmp.FCT_USER_BEHAVIOR_INFO")
    bhv.repartition(1).write.format("csv").save("/tmp/mh/bhv        where l_date>='2019-07-05'\"")


    val info = spark.sql("select * from dw.WECHAT_USER_INFO_LATEST30D where l_date>='2019-07-11'")
    //val info=spark.table("tmp.WECHAT_USER_INFO_LATEST30D")
    info.repartition(1).write.format("csv").mode("overwrite").save("/tmp/mh/info")


    val json = spark.sql("select id,allocatedmb,allocatedvcores from  tmp.yarn_apps_resources_info_test where allocatedmb is not null and allocatedvcores is not null")
    json.repartition(1).write.mode("Overwrite").format("csv").save("/tmp/mh/yarn/yarn_job_csv")


    val cpv = spark.sparkContext.textFile("/tmp/mh/wechat_cpv_t1d_tim_alldim.csv")
    //    val cpv2 = spark.sparkContext.textFile("/tmp/mh/wechat_cpv_t1d_tim_alldim2.csv")


    val cpvrdd = cpv.map(x => {
      val length = x.split("\t").toList.length
      (x, length)
    })

      cpvrdd.filter(x => {
        x._1.equals(' ')
      }).count()
    cpvrdd.filter(x => {
      x._2.equals(13)
    }).repartition(1).saveAsTextFile("/tmp/mh/cpvrddtesstwsd")


    cpvrdd.collect().max


    val uid = spark.sql("select uid,terminal,insert_day  from tmp.dim_pc_wap_uid where l_date='2019-08-10'")
    uid.repartition(200).write.saveAsTable("tmp.dim_pc_wap_uid_tmp")


    val uuid = spark.sql("select uuid,terminal,insert_day from tmp.dim_pc_wap_uuid where l_date='2019-08-11'")
    uuid.repartition(1).write.saveAsTable("tmp.dim_pc_wap_uuid_tmp")


    val basc = spark.sql("select *  from dw.FCT_USER_BASIC_INFO where l_date>='2019-07-09'")
    basc.repartition(1).write.format("csv").save("/tmp/mh/basic")


    val rebin = spark.table("dw.cs_pv_pc_page_1d")
    rebin.repartition(1).write.format("csv").save("/tmp/mh/rebin")

    val cirinfo = spark.table("tmp.FCT_USER_BEHAVIOR_INFO_MID")

    val rebin_df = spark.sql("select index_id,collect_list(indexId) reb_indexlist  from tmp.index_detail4_recbin group by index_id")
    val cirinfo_df = spark.sql("select uuid,collect_list(INDEX_ID) cir_indexStr from tmp.FCT_USER_BEHAVIOR_INFO_MID where INDEX_ID is not null group by uuid")


    rebin_df.rdd.map(x => {
      val index_id = x.get(0)
      val reb_indexlist = x.getAs[mutable.WrappedArray[String]]("reb_indexlist")
      (index_id, reb_indexlist)
    }).first()

    /*
        val page = spark.read.textFile("file:/tmp/mh/zn/spark/")
        val field = page.map(line => line.split("\t"))
        val iPhone = field.filter(s => s.contains("iPhone"))
        iPhone.count()
        val sample = iPhone.sample(false, 0.5)

        val peopleDF = spark.read.format("json").load("file:/tmp/mh/zn/resources/people.json")
        peopleDF.write.mode("overwrite").saveAsTable("tmp.people_bucketed")*/
    spark.stop()
  }

  //获取hdfs上数据
  def loadSmallFile(sc: SparkContext, path: String, part: Int): RDD[String] = {
    sc.hadoopFile[LongWritable, Text, CombineTextInputFormat](path, part).map(line => line._2.toString)
  }


  /**
    * 主程序
    *
    * @param context
    * @return
    */
  def process(spark: SparkSession): Unit = {

    val preFixPath = "/data/flume/wechat/free-wechat-event/" + getNowDate("yyyy-MM-dd") + "/" + getHourPath + "/free-wechat-event.*"
    val table = "tmp.free_wechat_event_log"
    val fileRdd = loadSmallFile(spark.sparkContext, preFixPath, 30)
    //构建df表结构
    val schemaString = "request_time,ip,http_logsource,http_uuid,http_aid,http_version,http_phoneos,http_phonemodel,http_network,http_carries,http_longitude,http_latitude,bd,osv,uid,eid,obj,scs,osl,dpi,bat,sid,srcid,l_date"

    val schema =
      StructType(
        schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    //解析
    val page = fileRdd.filter(line => line.split("\\|\\|").length == 15).map(line => {
      val fieldArray = line.split("\\|\\|")
      val timeComplex = fieldArray(0)
      val request_time = if (timeComplex.contains("\u0000")) timeComplex.split("\u0000")(timeComplex.split("\u0000").length - 1)
      else timeComplex
      val ip = fieldArray(1)
      val http_logsource = fieldArray(2)
      val http_uuid = fieldArray(3)
      val http_aid = fieldArray(4)
      val http_version = fieldArray(5)
      val http_phoneos = fieldArray(6)
      val http_phonemodel = fieldArray(7)
      val http_network = fieldArray(8)
      val http_carries = fieldArray(9)
      val http_longitude = fieldArray(10)
      val http_latitude = fieldArray(11)
      val sid = fieldArray(12)
      val srcid = fieldArray(13)
      val map = util.ArrToMap(fieldArray(14))
      val bd = map.get("bd")
      val osv = map.get("osv")
      val uid = map.get("uid")
      val eid = map.get("eId")
      val obj = map.get("obj")
      val scs = map.get("scs")
      val osl = map.get("osl")
      val dpi = map.get("dpi")
      val bat = map.get("bat")
      Row(request_time, ip, http_logsource, http_uuid, http_aid, http_version, http_phoneos, http_phonemodel, http_network, http_carries, http_longitude, http_latitude, bd, osv, uid, eid, obj, scs, osl, dpi, bat, sid, srcid, getNowDate("yyyy-MM-dd"))
    })
    val pageDF = spark.createDataFrame(page, schema)
    pageDF.createTempView("event_tmp")
    val sql = s"insert into table  ${table} PARTITION(l_date) select request_time, ip, http_logsource, http_uuid, http_aid, http_version, http_phoneos, http_phonemodel, http_network, http_carries, http_longitude, http_latitude, bd, osv, uid, eid, obj, scs, osl, dpi, bat, sid, srcid,l_date from event_tmp "
    spark.sql(sql)
  }


  /**
    * 获取时间
    *
    * @param pattern
    * @return
    */
  def getNowDate(pattern: String): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat(pattern)
    var hehe = dateFormat.format(now)
    hehe
  }


  /**
    * 得到小时路径
    *
    * @return
    */
  def getHourPath(): String = {
    val hour = getNowDate("HH")
    val mintues = getNowDate("mm")
    val mi = mintues.toInt
    val minP = mintues match {
      case mintues if 0 <= mi && mi < 5 => "55"
      case mintues if 5 <= mi && mi < 10 => "00"
      case mintues if 10 <= mi && mi < 15 => "05"
      case mintues if 15 <= mi && mi < 20 => "10"
      case mintues if 20 <= mi && mi < 25 => "15"
      case mintues if 25 <= mi && mi < 30 => "20"
      case mintues if 30 <= mi && mi < 35 => "25"
      case mintues if 35 <= mi && mi < 40 => "30"
      case mintues if 40 <= mi && mi < 45 => "35"
      case mintues if 45 <= mi && mi < 50 => "40"
      case mintues if 50 <= mi && mi < 55 => "45"
      case mintues if 55 <= mi => "50"
      case _ => ""

    }
    hour + "-" + minP
  }


}
