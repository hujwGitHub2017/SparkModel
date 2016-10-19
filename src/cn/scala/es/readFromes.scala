package cn.scala.es

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration

object readFromes {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("readFromEs")
    
    val sc =  new SparkContext(conf)
    
    conf.set("es.resource", "radio/artists");       
    conf.set("es.query", "?q=me*"); 
    
    sc.hadoopConfiguration.set("es.resource", "radio/artists")
    
    

    
    
  }
  
  
}