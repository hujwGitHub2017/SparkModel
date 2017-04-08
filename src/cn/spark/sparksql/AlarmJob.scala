package com.qzt360.alarm

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Properties
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.ArrayBuffer

object AlarmJob {
  /**
   * 
   */
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("AlarmJob")
    
    val sc = new SparkContext(conf)
    
    val file = sc.textFile("")
    
    val data01 = file.map { x => (x.split("\t")(0),x.split("\t")(2))}.groupByKey()
    
    val data02 = data01.map(f =>{
      
     val num = f._2.filter { x => x.contains(1002) }.toList.distinct.size
     val numQQ = f._2.filter { x => x.contains(1005) }.toList.distinct.size
      
      //val num = f._2.toList.distinct.size
      
      (f._1,"10002"+num)
      
    })
    
   val fin =  data02.mapPartitions(f => {
      
       val alarmFinallyInfo = ArrayBuffer[String]()
      
      while (f.hasNext) {
        
        val i = f.next()
        
        val n = i._1+"\t"+i._2
        
        alarmFinallyInfo+=n
      }
       
       
      alarmFinallyInfo.iterator
      
    })
    
    fin.saveAsTextFile("")
    
    val sql = new SQLContext(sc)
    
    val lowerBound = 1
    val upperBound = 100000
    val numPartitions = 5
    val db_url = "192.168.10.20"
    val url = "jdbc:postgresql://" + db_url + ":5432/beap?user=postgres&password=123456&loginTimeout=5";//"jdbc:mysql://www.iteblog.com:3306/iteblog?user=iteblog&password=iteblog"
    val prop = new Properties()
    
    val df = sql.read.jdbc(url, "tbl_alarm_cmd", prop)
    
    println(df.count())
    
    
  }
  
}