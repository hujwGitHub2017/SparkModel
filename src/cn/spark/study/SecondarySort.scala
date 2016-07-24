package cn.spark.study

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SecondarySort {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("SecondarySort").setMaster("local")
    
    val sc = new SparkContext(conf)
    
    
    val data = sc.textFile("D:\\data\\testData\\sorrt.txt", 2)
    
    
    val keydata = data.map { str => (new SecondarySortKey(str.split(" ")(0),str.split(" ")(1)),str) }
    
    
    val sortdata = keydata.sortByKey(true, 1)
    
    val context = sortdata.map(f => f._2)
    
    context.foreach(f => {

      println(f)
      
    })
    
    
  }
  
}