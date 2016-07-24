package cn.spark.study

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BroadCastVariable {
  
  /**
   * 共享变量
   * 
   */
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
              .setAppName("BroadCastVariable")
              .setMaster("local")
              
    val sc = new SparkContext(conf)
    
    val  dataList = Array(1,2,3,4,5,6)
    
    val dataRdd = sc.parallelize(dataList, 3)
    
    val factor = 2
    
    val factorBroadCast = sc.broadcast(factor)
    
    val dataMap = dataRdd.map { x => (x,x*factorBroadCast.value) }
    
    
    dataMap.foreach( f => {
      
      println("data : "+f._1+" *="+f._2)
      
    })

    
  }
  
}