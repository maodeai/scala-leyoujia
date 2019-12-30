package cn.com.leyoujia.dca

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.lib.CombineTextInputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object WechatPage {
  def main(args: Array[String]): Unit = {
    Logger.getLogger( "org.apapche.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apapche.kafka").setLevel(Level.WARN)
    Logger.getLogger("org.apapche.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apapche.flume").setLevel(Level.WARN)
    Logger.getLogger("org.apapche.zookeeper").setLevel(Level.WARN)

    /* val conf = new SparkConf().setMaster("local[4]").setAppName("WechatPage")
       .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     val sc = new SparkContext(conf)*/
    val spark = SparkSession
      .builder()
      .appName("wechat_Page")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    //val path=args(0)
    process(spark)
    spark.stop()
  }

  //获取hdfs上数据
  def loadSmallFile(sc: SparkContext, path: String, part: Int): RDD[String] = {
    sc.hadoopFile[LongWritable, Text, CombineTextInputFormat](path, part).map(line => line._2.toString)
  }

  /*  /**
      * 构建streamingContext
      *
      * @param conf
      * @param checkPath
      * @return
      */
    def createContext(conf: SparkConf, checkPath: String): StreamingContext = {
      val ssc = new StreamingContext(conf, Seconds(300))
      ssc.checkpoint(checkPath)
      process(ssc)
      ssc
    }*/

  /**
    * 主程序
    *
    * @param context
    * @return
    */
  def process(spark: SparkSession): Unit = {

    val preFixPath = "/data/flume/wechat/wechat-page/" + getNowDate("yyyy-MM-dd") + "/" + getHourPath + "/wechat-page.*"
    //val textFile = "file:///G:Data/wechat/*"
    //println("拉取的时间段" + path)tmp.wechat_page_log
    val table = "tmp.wechat_page_log"
    val fileRdd = loadSmallFile(spark.sparkContext, preFixPath, 30)
    //val  wetchat=ssc.textFileStream()
    //构建df表结构
    val schemaString = "it,ip,logsource,uuid,aid,ssid,ver,ost,model,bi_nt,bi_np,bi_lng,bi_lat,channel,mac,imei,idfa,imsi,pid,st,bd,osv,uid,et,scs,osl,dpi,bat,sid,srcid,l_date"
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StructType, StructField, StringType};
    val schema =
      StructType(
        schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    //解析
    val page = fileRdd.filter(line => line.split("\\|\\|").length == 21).map(line => {
      val fieldArray = line.split("\\|\\|")
      val itComplex = fieldArray(0)
      val it = if (itComplex.contains("\u0000")) itComplex.split("\u0000")(itComplex.split("\u0000").length - 1)
      else itComplex
      val ip = fieldArray(1)
      val logsource = fieldArray(2)
      val uuid = fieldArray(3)
      val aid = fieldArray(4)
      val ssid = fieldArray(5)
      val ver = fieldArray(6)
      val ost = fieldArray(7)
      val model = fieldArray(8)
      val bi_nt = fieldArray(9)
      val bi_np = fieldArray(10)
      val bi_lng = fieldArray(11)
      val bi_lat = fieldArray(12)
      val channel = fieldArray(13)
      val mac = fieldArray(14)
      val imei = fieldArray(15)
      val idfa = fieldArray(16)
      val imsi = fieldArray(17)
      val sid = fieldArray(18)
      val srcid = fieldArray(19)
      val pid = fieldArray(20).split("&st=")(0).replace("pId=", "")
      val map = util.ArrToMap(fieldArray(20))
      val st = map.get("st")
      val bd = map.get("bd")
      val osv = map.get("osv")
      val uid = map.get("uid")
      val et = map.get("et")
      val scs = map.get("scs")
      val osl = map.get("osl")
      val dpi = map.get("dpi")
      val bat = map.get("bat")
      Row(it, ip, logsource, uuid, aid, ssid, ver, ost, model, bi_nt, bi_np, bi_lng, bi_lat, channel, mac, imei, idfa, imsi, pid, st, bd, osv, uid, et, scs, osl, dpi, bat, sid, srcid,getNowDate("yyyy-MM-dd"))
    })
    val pageDF = spark.createDataFrame(page, schema)
    //pageDF.write.mode(SaveMode.Append).partitionBy("l_date").insertInto("tmp.wechat_page_log")
    //pageDF.foreach(x => println(x))
    //pageDF.show(30)
    //pageDF.write.mode("append").insertInto("tmp.wechat_page_log")
    pageDF.createTempView("page_tmp")
    val sql = s"insert into table  ${table} PARTITION(l_date) select it, ip, logsource, uuid, aid, ssid, ver, ost, model, bi_nt, bi_np, bi_lng, bi_lat, channel, mac, imei, idfa, imsi, pid, st, bd, osv, uid, et, scs, osl, dpi, bat, sid, srcid,l_date from page_tmp "
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
