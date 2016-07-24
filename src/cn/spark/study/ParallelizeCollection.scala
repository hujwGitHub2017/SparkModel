package cn.spark.study

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ParallelizeCollection {
  
  def main(args: Array[String]): Unit = {
    
    val numList =  Array(1,2,3,4,5,6,7,8)
    
    val conf = new SparkConf()
              .setAppName("ParallelizeCollection")
              .setMaster("local")
              
    val sc = new SparkContext(conf)
    
    val numRdd = sc.parallelize(numList, 2)
    
    
    val total  = numRdd.reduce(_+_)
    
    
    println("计数 大小： "+total)
    
    
  }
}