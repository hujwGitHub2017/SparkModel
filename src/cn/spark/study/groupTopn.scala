package cn.spark.study

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import scala.util.control._
object groupTopn {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("groupTopn").setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val data = sc.textFile("D:\\data\\testData\\sorrt.txt", 1)
    
    val dataMap = data.map { x => (x.split(" ")(0),(x.split(" ")(1)).toInt) }
    
    val dataGroup = dataMap.groupByKey()
    
    val loop = new Breaks
    
   val context =  dataGroup.map(f => {
      
      val top3 = new Array[Int](3)
      
      val scoreValue = f._2.toBuffer
      
      top3(0) = scoreValue.max
      scoreValue.-=(scoreValue.max)
      
      top3(1) = scoreValue.max
      scoreValue.-=(scoreValue.max)
      
      top3(2) = scoreValue.max
      scoreValue.-=(scoreValue.max)
      
      (f._1,top3)
      
    })
    
    
    context.foreach(f =>{
      
      for(i <- f._2){
        
        println("className :"+f._1+" score : "+i)
      }
      
    })
    
  }
  
}