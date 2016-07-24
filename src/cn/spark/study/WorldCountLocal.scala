package cn.spark.study

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WorldCountLocal {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
       .setMaster("local")
       .setAppName("WorldCountLocal")
    
    val sc = new SparkContext(conf)
    
    val lines = sc.textFile("D:\\data\\study.txt", 5)
    
    val words = lines.flatMap { line => line.split("\t") }.filter { line => line.contains(".") }
    
    val worldvalus = words.map { word => (word,1) }
    
    val counts = worldvalus.reduceByKey(_+_)
    
    counts.foreach(v => println("单词： "+v._1+"  计数： "+v._2))
  }
  
}