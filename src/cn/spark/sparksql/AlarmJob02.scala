package com.qzt360.alarm

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object AlarmJob02 {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("AlarmJob")
    
    val sc = new SparkContext(conf)

    val sql = new SQLContext(sc)
    
    val db_url = "192.168.0.45"
    
    val url = "jdbc:postgresql://" + db_url + ":5432/beap?user=postgres&password=123456&loginTimeout=5";//"jdbc:mysql://www.iteblog.com:3306/iteblog?user=iteblog&password=iteblog"
    
    val date = sql.load("jdbc", Map(
      "url" -> url,
      "driver" -> "org.postgresql.Driver",
      "dbtable" -> "iteblog"))
      
     date.registerTempTable("iteblog")
     sql.sql("select * from iteblog").foreach(println)
     
      /*val alarmInfo = new ArrayBuffer[String]()
      
       val conn = DBBridge.openBridge()
       
       val stmt = DBBridge.getStatement(conn)
        
       val sql = "select * from tbl_alarm_cmd"
      
       val rs = DBBridge.execSELECT(sql,stmt,conn)
      
      
      while (rs.next()) {
  
    	  //println(rs.getString("cmd_name"))
        
        alarmInfo+=rs.getString("cmd_name")
      
      }
      
       sc.parallelize(alarmInfo, 1).saveAsTextFile("/user/alarmtemp/info/"+Calendar.getInstance.getTimeInMillis)
      */
    
  }
  
}