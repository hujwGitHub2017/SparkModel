package cn.spark.study

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object shell {
  
    def main(args: Array[String]): Unit = {
      
      val conf = new SparkConf()
              .setAppName("")
              
     val sc = new SparkContext(conf)
      
    val data =  sc.textFile("/user/wlan/nat/history/2016060401*", 100)
    
    data.flatMap { x => ??? }
    
     data.filter ( f => f.startsWith("1") ).count()
     
     data.filter ( x => !x.split("|")(3).startsWith("null")).count()
     
     
     data.filter ( f => f.startsWith("1") ).collect()
     
     data.filter ( f => f.startsWith("1") ).take(2)
     
    data.filter ( f => f.contains("112.9.245.63") ).count()
    
     data.filter ( f => f.contains("112.9.245.63")&&f.contains("7373")  ).repartition(1).saveAsTextFile("/user/wlan/20160604_01.log")
      
    }
  
}