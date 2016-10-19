package cn.spark.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object HdfsFileStreaming {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
    
    val context = new SparkContext(conf)
    
    val sc = new StreamingContext(context,Seconds(10))
    
    val dataStream = sc.textFileStream("hdfs://spark01:9000/user/data")
    
    val wordStream = dataStream.flatMap { l => l.split(" ") }
    
    val pairStream = wordStream.map { w => (w,1) }
    
   /* pairStream.reduce((k,v) => {
      
      (k._1,(k._2+v._2))
      
    })
    */
    
    val vaStream = pairStream.reduceByKey(_+_)
    
    
    vaStream.print()
    
  }
  
}