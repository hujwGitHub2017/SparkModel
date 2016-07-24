package cn.spark.study

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ActionOperation {
  
  def main(args: Array[String]): Unit = {
    
//    Collect()
    
    Take()
    
  }
  
  
  def Collect():Unit = {
    
    
    val conf = new SparkConf()
              .setMaster("local")
              .setAppName("ActionOperation")
              
    val sc = new SparkContext(conf)
    
    val dataList = Array(1,2,3,4,5)
    
     val dataRdd = sc.parallelize(dataList, 2)
     
     val dataRdd2 = dataRdd.map { f => f*2 }
    
    dataRdd2.foreach { x => println(x) }
    
    
    val localData = dataRdd2.collect()
    
    for( i <- localData){
      
      println(i)
      
    }
    
  }
  
  def Take():Unit={
    
    val conf =  new SparkConf()
                .setAppName("ActionOperation")
                .setMaster("local")
                
                
    val sc = new SparkContext(conf)
    
    val dataList = Array(7,1,2,4,5,5,6)
    
    
    val dataRdd = sc.parallelize(dataList, 2)
    
    val sortRdd = dataRdd.sortBy(f => f ,true, 1)
    
    val takeData = sortRdd.take(3)
    
    for( i <- takeData){
      
     println(i)
    }
    
    
  }
}