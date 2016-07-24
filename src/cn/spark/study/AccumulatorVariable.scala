package cn.spark.study

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ariable {
  
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
          .setAppName("AccumulatorVariable")
          .setMaster("local")
    
    val sc = new SparkContext(conf)
    
    
    val dataList = Array(1,2,3,4,5,6)
    
    val dataRdd = sc.parallelize(dataList, 1)
    
    val count = sc.accumulator(0)
    
//    count.setValue(2)
    
//    val initvalue = 5L  
//    
//    val count = sc.accumulator(initvalue)(MyAccumulator)  
    
    dataRdd.foreach { x => {
          
      count+=x
      
      
      } 
    }
    
    println("count = "+count.value)
    
    count.setValue(100)
    
    
      dataRdd.foreach { x => {
          
      count+=x
      
      } 
    }
    
     println("count = "+count.value)
  
  }
}