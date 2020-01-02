package cn.com.leyoujia.dca

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

object HouseMatchClient {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.kafka").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.flume").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("HouseMatchClient")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()


    process(spark)
    spark.stop()
  }


  /**
    * @title
    * @Describe
    * @Params:
    * @return:
    * @Author: mh
    * @Date: 2019/12/20
    */

  def process(spark: SparkSession): Unit = {

    val table1 = spark.table("pdata.client_intention_match_info") //客户需求信息
    val table2 = spark.table("pdata.house_match_info") //为房客互动圈的新发布的数据，数据量比较少
    //客户需求信息
    val table1RDD = table1.rdd.map(x => {
      val cid = x.getAs[String]("cid")
      val seeTime = x.getAs[String]("see_time")
      val require_type = x.getAs[Int]("require_type")
      val objective = x.getAs[String]("objective")
      val area_code = x.getAs[String]("area_code")
      val area_name = x.getAs[String]("area_name")
      val priceStart = x.getAs[Double]("price_start")
      val priceEnd = x.getAs[Double]("price_end")
      val houseType = x.getAs[String]("house_type")
      val houseAreaStart = x.getAs[Double]("house_area_start")
      val houseAreaEnd = x.getAs[Double]("house_area_end")
      val purposeSchool = x.getAs[Int]("purpose_school")
      val purposeSchoolId = x.getAs[String]("purpose_school_id")
      val purposeSchoolName = x.getAs[String]("purpose_school_name")
      val updateTime = x.getAs[String]("update_time")
      val insertWorkerId = x.getAs[String]("insert_worker_id")
      ClietInfo(cid, seeTime, require_type, objective, area_code, area_name, priceStart, priceEnd, houseType, houseAreaStart, houseAreaEnd, purposeSchool, purposeSchoolId, purposeSchoolName, updateTime, insertWorkerId)
    })

    //table1RDD.persist(StorageLevel.MEMORY_ONLY_SER)
    var clientInfo = table1RDD.filter(x => (x.area_code.split("-").length == 3)).filter(x => (x.houseType != null)).collect()
    var clientInfoFilter = Array[ClietInfo]()

    //房客互动圈信息
    val table2RDD = table2.rdd.map(x => {
      val cityCode = x.getAs[String]("city_code")
      val shareId = x.getAs[Int]("share_id")
      val fh_id = x.getAs[Int]("fh_id")
      val share_worker_id = x.getAs[String]("share_worker_id")
      val city = x.getAs[String]("city")
      val area = x.getAs[String]("area")
      val place = x.getAs[String]("place")
      val room = x.getAs[Int]("room")
      val price = x.getAs[Double]("price")
      val rs_type = x.getAs[Int]("rs_type")
      HouseInfo(cityCode, shareId, fh_id, share_worker_id, city, area, place, room, price, rs_type)
    })


    //table2RDD.persist(StorageLevel.MEMORY_ONLY_SER)
    val houseInfo = table2RDD.collect()

    //判断日期是否重新开始，如果现在的日期和表格中存在日期不一致，更新持久化的日期，并初始化客户次数
    val newDay = util.getNowDate("yyyy-MM-dd")
    val tableDay = spark.table("pdata.house_match_client_day").rdd.map(d => d.getAs[String]("p_date")).collect()
    var matchInfoCount: Map[String, Int] = Map()
    if (tableDay.isEmpty || newDay != tableDay.last) {
      matchInfoCount = HashMap()
      //持久化新日期至hive表
      import spark.implicits._
      Seq(newDay).map(ClientMatchDay(_)).toDF.write.mode("overwrite").saveAsTable("pdata.house_match_client_day")
    } else {
      //得到客户匹配的次数，并转换成map
      matchInfoCount = spark.table("pdata.house_match_client_count").rdd.map(x => {
        val cid = x.getAs[String]("cid")
        val count = x.getAs[Int]("count")
        (cid, count)
      }).collect().toMap
    }


    //对相应的客户数进项判断，过滤掉客户匹配数超过3次的客户
    for ((cid, count) <- matchInfoCount) {
      if (count > 3) {
        clientInfo = clientInfo.filter(x => {
          x.cid != cid
        })
      }
    }


    val matchInfo = new ArrayBuffer[MatchFinal]
    //第一层循环，为房源
    for (house <- houseInfo) {
      val houseMachInfoArray = new ArrayBuffer[MatchInfo]



      //第二层循环，为客户需求意向
      for (client <- clientInfo) {
        //客户意向code解析
        val areaCodeArray = client.area_code.split("-")
        val clientPlace = areaCodeArray.last
        val clentHouseTypeList = client.houseType.split(",").toList
        val clientHouseType = client.require_type match {
          case 1 => 1
          case 2 => 2
          case 4 => 2
          case _ => 3
        }


        //匹配规则，房源价格在客户需求价格之间，并且需求户数包含房源室数，客户需求片区等于房源片区,//客户需求片区不为0，房源片区和客户需求片区匹配
        if (house.price <= client.priceEnd && house.price >= client.priceStart && clentHouseTypeList.contains(house.room.toString) && clientPlace != 0 && house.place == clientPlace && clientHouseType == house.rs_type) {
          houseMachInfoArray += (MatchInfo(house.cityCode, house.shareId, house.fh_id, house.share_worker_id,
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
        }
      }

      //过滤掉片区已经匹配上的客户
      for (cid <- houseMachInfoArray.map(_.cid)) {
        clientInfoFilter = clientInfo.filter(x => {
          x.cid != cid
        })
      }


      //判断通过房源片区与客户需求片区匹配上的客户数是否超过3个，若不满足3个客户需求，则扩大至行政区匹配, //客户需求城区不为0，房源城区和客户需求城区匹配
      if (houseMachInfoArray.map(_.cid).distinct.length < 3) {
        for (client <- clientInfoFilter) {
          //客户意向code解析
          val areaCodeArray = client.area_code.split("-")
          val clientArea = areaCodeArray(1)
          val clentHouseTypeList = client.houseType.split(",").toList
          val clientHouseType = client.require_type match {
            case 1 => 1
            case 2 => 2
            case 4 => 2
            case _ => 3
          }

          if (house.price <= client.priceEnd && house.price >= client.priceStart && clentHouseTypeList.contains(house.room.toString) && clientArea != 0 && house.area == clientArea && clientHouseType == house.rs_type) {
            houseMachInfoArray += (MatchInfo(house.cityCode, house.shareId, house.fh_id, house.share_worker_id,
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
          }
        }
      }






      //对匹配上的客户需求信息进行过滤，取最近带看的客户为此次客户
      import spark.implicits._
      val houseMachInfoDF = houseMachInfoArray.toDF()
      //houseMachInfoDF.show(100)
      houseMachInfoDF.createOrReplaceTempView("match_client_info")
      val fsql = "select cityCode, shareId,fh_id,share_worker_id,cid, seeTime, objective, area_code, area_name, priceStart, priceEnd,houseType, houseAreaStart, houseAreaEnd, purposeSchool, purposeSchoolId, purposeSchoolName, updateTime,insertWorkerId from match_client_info where cid in (select cid from (select s.cid,row_number() over(order by s.ms desc)r from (select max(seeTime) ms,cid from match_client_info group by cid) s ) f  where f.r<=3) "
      //val fsql2 = "select a.shareId,a.fh_id,a.share_worker_id,a.cid, a.seeTime, a.objective, a.area_code, a.area_name, a.priceStart, a.priceEnd, a.houseType, a.houseAreaStart, a.houseAreaEnd, a.purposeSchool, a.purposeSchoolId, a.purposeSchoolName, a.updateTime, a.insertWorkerId from match_client_info a join (select cid from (select s.cid,row_number() over(order by s.ms desc)r from (select max(seeTime) ms,cid from match_client_info group by cid) s  ) f  where f.r<=3) b on a.cid=b.cid "
      val recentlyMatchClient = spark.sql(fsql)


      //对每个客户的匹配数进行更新
      val cidList = recentlyMatchClient.map(x => x.getAs[String]("cid")).collect()
      //cidList.foreach(x => println("cid matche list cid :" + x))
      if (cidList.length > 0)
        for (cid <- cidList) {
          val cidcount = matchInfoCount.getOrElse(cid, 0)
          val newCidCount = cidcount + 1
          matchInfoCount = matchInfoCount.updated(cid, newCidCount)
          //过滤掉本次匹配后客户匹配数超过3次的客户需求
          if (newCidCount > 3) clientInfo = clientInfo.filter(x => {
            x.cid != cid
          })
        }


      if (recentlyMatchClient.count() > 0) {
        //得到这个房源最终的客户匹配信息及对应的需求及录入经纪人
        recentlyMatchClient.createOrReplaceTempView("match_info")
        val sql = "SELECT cityCode, shareId,fh_id, share_worker_id,insertWorkerId ,cid,objective,area_code,area_name,priceStart,priceEnd,houseType,houseAreaStart, houseAreaEnd,purposeSchool, purposeSchoolId,purposeSchoolName  FROM (SELECT cityCode,shareId,fh_id,share_worker_id,cid,seeTime,objective,area_code,area_name,priceStart,priceEnd,houseType,houseAreaStart,houseAreaEnd, purposeSchool, purposeSchoolId,purposeSchoolName,updateTime,insertWorkerId,row_number() over(partition by fh_id,cid   ORDER BY  updateTime desc)rank   FROM match_info)s WHERE s.rank=1"
        val matchInfofinal = spark.sql(sql).as[MatchFinal].collect()
        matchInfo ++= ((matchInfofinal))
      }

    }


    import spark.implicits._
    //    System.out.println("将最终匹配结果持久化到hive层")
    //    matchInfo.toDF().show()
    matchInfo.toDF().repartition(1).write.mode("overwrite").saveAsTable("pdata.hsl_share_match_tmp")


    //    System.out.println("将客户匹配次数持久化到hive层")
    //    matchInfoCount.toArray.map(x => ClientMatchCount(x._1, x._2)).toSeq.toDF().show()
    matchInfoCount.toArray.map(x => ClientMatchCount(x._1, x._2)).toSeq.toDF().repartition(1).write.mode("overwrite").saveAsTable("pdata.house_match_client_count")

  }


  case class ClietInfo(cid: String, seeTime: String, require_type: Int, objective: String, area_code: String, area_name: String, priceStart: Double, priceEnd: Double, houseType: String, houseAreaStart: Double, houseAreaEnd: Double, purposeSchool: Int, purposeSchoolId: String, purposeSchoolName: String, updateTime: String, insertWorkerId: String) extends Serializable

  case class HouseInfo(cityCode: String, shareId: Int, fh_id: Int, share_worker_id: String, city: String, area: String, place: String, room: Int, price: Double, rs_type: Int) extends Serializable

  case class MatchInfo(cityCode: String, shareId: Int, fh_id: Int, share_worker_id: String, cid: String, seeTime: String, objective: String, area_code: String, area_name: String, priceStart: Double, priceEnd: Double, houseType: String, houseAreaStart: Double, houseAreaEnd: Double, purposeSchool: Int, purposeSchoolId: String, purposeSchoolName: String, updateTime: String, insertWorkerId: String) extends Serializable

  case class MatchFinal(cityCode: String, shareId: Int, fh_id: Int, share_worker_id: String, insertWorkerId: String, cid: String, objective: String, area_code: String, area_name: String, priceStart: Double, priceEnd: Double, houseType: String, houseAreaStart: Double, houseAreaEnd: Double, purposeSchool: Int, purposeSchoolId: String, purposeSchoolName: String) extends Serializable

  case class ClientMatchCount(cid: String, count: Int) extends Serializable

  case class ClientMatchDay(p_date: String) extends Serializable

}

