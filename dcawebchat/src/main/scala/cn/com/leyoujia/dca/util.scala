package cn.com.leyoujia.dca

import java.text.SimpleDateFormat
import java.util.{Date, HashMap, Map}

object util {
  def ArrToMap(complexStr: String): Map[String, String] = {
    var key = ""
    var value = ""
    val map = new HashMap[String, String]
    val arr = complexStr.split("\\&")
    var i = 0
    while (i < arr.length) { //判断字符串中有没有点等于号
      if (arr(i).length > 0 && arr(i) != null && (arr(i) ne "")) {
        val a = arr(i) + "%"
        if (arr(i).indexOf("=") >= 0) {
          key = a.substring(0, a.indexOf("=")).trim
          value = a.substring(a.indexOf("=") + 1, a.indexOf("%")).trim
        }
        map.put(key, value)
      }

      {
        i += 1
        i - 1
      }
    }
    map
  }


  /**
    * 获取时间
    *
    * @param pattern
    * @return
    */
  def getNowDate(pattern: String): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat(pattern)
    val hehe = dateFormat.format(now)
    hehe
  }

}
