package cn.com.leyoujia.dca

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object HouseMatchCluster {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apapche.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apapche.kafka").setLevel(Level.WARN)
    Logger.getLogger("org.apapche.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apapche.flume").setLevel(Level.WARN)
    Logger.getLogger("org.apapche.zookeeper").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .appName("HouseMatchClient")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    //val path=args(0)
    process(spark)

    val fdf = spark.table("tmp.client_intention_info_final")
    fdf.selectExpr("groupby(cid)").show()


    spark.stop()

  }


  /*
  * @title
  * @Describe
  * @Param:
  * @return:
  * @Author: mh
  * @Date: 2019/12/19
  */
  def process(spark: SparkSession): Unit = {

    val table1 = spark.table("tmp.client_intention_match_info")
    val table2 = spark.table("tmp.house_match_info") //为房客互动圈的新发布的数据，数据量比较少

    table1.na.fill(0.0).na.fill(' ').repartition(1).write.mode("overwrite").format("json").save("/tmp/mh/client")
    table1.filter(x => (x.getAs[String]("area_code").split("-").length == 3)).filter(x => (x.getAs[String]("area_code").split("-")(2).equals("012953"))).show()
    table1.filter(x => (x.getAs[String]("area_code").split("-").length == 3)).filter(x => (x.getAs[String]("area_code").split("-")(1).equals("000060")) && x.getAs[Double]("price_start") <= 197 && x.getAs[Double]("price_end") >= 197).show()

    table1.na.fill(0.0).na.fill(' ').repartition(1).write.mode("overwrite").format("json").save("/tmp/mh/client")
    table1.filter(x => (x.getAs[String]("area_code").split("-").length == 3)).filter(x => (x.getAs[String]("area_code").split("-")(2).equals("001368"))).show()
    table1.filter(x => (x.getAs[String]("area_code").split("-").length == 3)).filter(x => (x.getAs[String]("area_code").split("-")(1).equals("001197")) && x.getAs[Double]("price_start") <= 280 && x.getAs[Double]("price_end") >= 280).show()


    val table11 = spark.read.json("F:\\房客互动圈\\dev\\data\\client.json").filter(x => (x.getAs[String]("area_code").split("-").length == 3)).na.fill(' ')
      .filter(x => (x.getAs[String]("house_type") != null))

    val table22 = spark.read.json("F:\\房客互动圈\\dev\\data\\house.json")
    table2.filter(x => x.getAs[Int]("fh_id") == 6114625).show()

    table2.filter(x => x.getAs[Int]("fh_id") == 81739917).show()
    table2.filter(x => x.getAs[Int]("fh_id") == 2218720).show()


    //客户需求信息
    var clientInfo = table1.rdd.map(x => {
      val cid = x.getAs[String]("cid")
      val seeTime = x.getAs[String]("see_time")
      val objective = x.getAs[String]("objective")
      val area_code = x.getAs[String]("area_code")
      val area_name = x.getAs[String]("area_name")
      val priceStart = x.getAs[String]("price_start")
      val priceEnd = x.getAs[String]("price_end")
      val houseType = x.getAs[String]("house_type")
      val houseAreaStart = x.getAs[String]("house_area_start")
      val houseAreaEnd = x.getAs[String]("house_area_end")
      val purposeSchool = x.getAs[String]("purpose_school_id")
      val purposeSchoolId = x.getAs[String]("purpose_school_id")
      val purposeSchoolName = x.getAs[String]("purpose_school_name")
      val updateTime = x.getAs[String]("update_time")
      val insertWorkerId = x.getAs[String]("insert_worker_id")
      ClietInfo(cid, seeTime, objective, area_code, area_name, priceStart, priceEnd, houseType, houseAreaStart, houseAreaEnd, purposeSchool, purposeSchoolId, purposeSchoolName, updateTime, insertWorkerId)
    }).collect()


    //房客互动圈信息
    val table2RDD = table2.rdd.map(x => {
      val shareId = x.getAs[String]("share_id")
      val fh_id = x.getAs[Int]("fh_id")
      val workerId = x.getAs[String]("worker_Id")
      val city = x.getAs[String]("city")
      val area = x.getAs[String]("area")
      val place = x.getAs[String]("place")
      val room = x.getAs[String]("room")
      val price = x.getAs[String]("price")
      HouseInfo(shareId, fh_id, workerId, city, area, place, room, price)
    }).collect()


    //得到客户匹配的次数，并转换成map
    val matchInfoCount = spark.table("tmp.match_client_count").rdd.map(x => {
      val cid = x.getAs[String]("cid")
      val count = x.getAs[Int]("count")
      (cid, count)
    }).collect().toMap

    //    val matchInfoMap = new HashMap[String, Int]
    //      val initMatcheCount = new HashMap[String, Int]()
    //      initMatcheCount.map(x => ClientMatchCount(x._1, x._2)).toSeq.toDF().repartition(1).write.mode("overwrite").saveAsTable("tmp.house_match_client_count")
    var mathInfoArray = new ArrayBuffer[MatchInfo]

    //第一层循环，为房源
    for (house <- table2RDD) {
      //第二层循环，为客户需求意向
      for (client <- clientInfo) {
        //客户意向code解析
        val areaCodeArray = client.area_code.split("-")
        val clientPlace = areaCodeArray.last
        val clientArea = areaCodeArray(1)
        val clientCity = areaCodeArray(0)

        //匹配规则，房源价格在客户需求价格之间，并且需求户数包含房源室数
        if (house.price <= client.priceEnd && house.price >= client.priceStart && house.room <= client.houseType) {
          //客户需求片区不为0，房源片区和客户需求片区匹配
          if (clientPlace != 0 && house.place == clientPlace) {
            mathInfoArray += (MatchInfo(house.shareId, house.fh_id, house.workerId,
              client.cid,
              client.seeTime,
              client.objective,
              client.area_code,
              client.area_name,
              client.priceStart,
              client.priceEnd,
              client.houseType,
              client.houseAreaStart,
              client.houseAreaEnd,
              client.purposeSchool,
              client.purposeSchoolId,
              client.purposeSchoolName,
              client.updateTime,
              client.insertWorkerId))

            //            val matchCount = matchInfoCount.getOrElse("cid", 0)
            //            //当次数大于3时，过滤掉包含匹配上的客户id
            //            if (matchCount > 3) {
            //              clientInfo = clientInfo.filter(x => {
            //                x.cid != client.cid
            //              })
            //            } else {
            //              mathInfoArray += (MatchInfo(house.shareId, house.fh_id, house.workerId,
            //                client.cid,
            //                client.seeTime,
            //                client.objective,
            //                client.area_code,
            //                client.area_name,
            //                client.priceStart,
            //                client.priceEnd,
            //                client.houseType,
            //                client.houseAreaStart,
            //                client.houseAreaEnd,
            //                client.purposeSchool,
            //                client.purposeSchoolId,
            //                client.purposeSchoolName,
            //                client.updateTime,
            //                client.insertWorkerId))
            //            }
            //            //对客户id的匹配次数进行更新
            //            val newc = matchCount + 1
            //            matchInfoCount.updated(client.cid, newc)
            //客户需求城区不为0，房源城区和客户需求城区匹配
          } else if (clientArea != 0 && house.area == clientArea) {
            mathInfoArray += (MatchInfo(house.shareId, house.fh_id, house.workerId,
              client.cid,
              client.seeTime,
              client.objective,
              client.area_code,
              client.area_name,
              client.priceStart,
              client.priceEnd,
              client.houseType,
              client.houseAreaStart,
              client.houseAreaEnd,
              client.purposeSchool,
              client.purposeSchoolId,
              client.purposeSchoolName,
              client.updateTime,
              client.insertWorkerId))

            //            val matchCount = matchInfoCount.getOrElse("cid", 0)
            //            //当次数大于3时，过滤掉包含匹配上的客户id
            //            if (matchCount > 3) {
            //              clientInfo = clientInfo.filter(x => {
            //                x.cid != client.cid
            //              })
            //            } else {
            //              mathInfoArray += (MatchInfo(house.shareId, house.fh_id, house.workerId,
            //                client.cid,
            //                client.seeTime,
            //                client.objective,
            //                client.area_code,
            //                client.area_name,
            //                client.priceStart,
            //                client.priceEnd,
            //                client.houseType,
            //                client.houseAreaStart,
            //                client.houseAreaEnd,
            //                client.purposeSchool,
            //                client.purposeSchoolId,
            //                client.purposeSchoolName,
            //                client.updateTime,
            //                client.insertWorkerId))
            //            }

          }
        }

      }
      //对匹配上的客户需求信息进行过滤，取最近带看的客户为此次客户
      import spark.implicits._
      mathInfoArray.toDF().createOrReplaceTempView("match_client_info")
      val filterSql = "select shareId,fh_id,workerId,cid, seeTime, objective, area_code, area_name, priceStart, priceEnd, houseType, houseAreaStart, houseAreaEnd, purposeSchool, purposeSchoolId, purposeSchoolName, updateTime, insertWorkerId from (select shareId,fh_id,workerId,cid, seeTime, objective, area_code, area_name, priceStart, priceEnd, houseType, houseAreaStart, houseAreaEnd, purposeSchool,purposeSchoolId, purposeSchoolName, updateTime, insertWorkerId,row_number() over(partition by fh_id order by seeTime desc)rank from  match_client_info)s where s.rank=1"
      val finalMatchClientInfo = spark.sql(filterSql)


      val finalCid = finalMatchClientInfo.select("cid").first().getAs[String]("cid")
      val matchCount = matchInfoCount.getOrElse("cid", 0)
      //当次数大于3时，过滤掉循环层的包含此客户的需求
      if (matchCount > 3) {
        clientInfo = clientInfo.filter(x => {
          x.cid != finalCid
        })
      }

      //对客户id的匹配次数进行更新
      val newc = matchCount + 1
      matchInfoCount.updated(finalCid, newc)


      //得到这个房源最终的客户匹配信息及对应的需求及录入经纪人
      finalMatchClientInfo.createOrReplaceTempView("match_info")
      val sql = "SELECT shareId,fh_id, workerId,insertWorkerId ,cid,objective,area_code,area_name,priceStart,priceEnd,houseType,houseAreaStart, houseAreaEnd,purposeSchool, purposeSchoolId,purposeSchoolName  FROM (SELECT shareId,   fh_id,        workerId,        cid,         seeTime,         objective,         area_code,area_name,priceStart,priceEnd,houseType,houseAreaStart,houseAreaEnd, purposeSchool, purposeSchoolId,purposeSchoolName,updateTime,insertWorkerId,       row_number() over(partition by fh_id,cid   ORDER BY  updateTime desc)rank   FROM match_info)s WHERE s.rank=1"
      spark.sql(sql).write.mode("overwrite").saveAsTable("tmp.hsl_share_match_tmp")
    }
    //将客户匹配次数持久化到hive层
    import spark.implicits._
    Seq(matchInfoCount.toArray.map(x => ClientMatchCount(x._1, x._2))).toDF().write.mode("overwrite").saveAsTable("tmp.match_client_count")

  }


  case class ClietInfo(cid: String, seeTime: String, objective: String, area_code: String, area_name: String, priceStart: String, priceEnd: String, houseType: String, houseAreaStart: String, houseAreaEnd: String, purposeSchool: String, purposeSchoolId: String, purposeSchoolName: String, updateTime: String, insertWorkerId: String) extends Serializable

  //  case class houseInfo(shareId: String, workerNo: String, rsType: String, fhId: String, entrustId: String, city: String, area: String, place: String, houseType: String, price: String) extends Serializable

  case class HouseInfo(shareId: String, fh_id: Int, workerId: String, city: String, area: String, place: String, room: String, price: String) extends Serializable


  case class MatchInfo(shareId: String, fh_id: Int, workerId: String, cid: String, seeTime: String, objective: String, area_code: String, area_name: String, priceStart: String, priceEnd: String, houseType: String, houseAreaStart: String, houseAreaEnd: String, purposeSchool: String, purposeSchoolId: String, purposeSchoolName: String, updateTime: String, insertWorkerId: String) extends Serializable


  case class ClientMatchCount(cid: String, count: Int) extends Serializable

}




