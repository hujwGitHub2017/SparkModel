package cn.spark.study

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object wholetextFile {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
              .setAppName("wholetextFile")
              .setMaster("local")
              
    val sc = new SparkContext(conf)
    
    val text = sc.wholeTextFiles("D:\\data\\scala", 3);
    
    val numLIst =  Array("1111","222","333")
    
  /* val name = text.name
   
   println(name)*/
    
    
    val nameList = new scala.collection.mutable.ArrayBuffer[String]()
    
    numLIst.foreach { x => nameList+=x }
    
    text.foreach(f => nameList+=f._1)
    
    
     val fileContext =  text.map{ f =>
       
       println(f._1)
       
        f._2
        
      }
   
   
    
//    text.foreach(f => println("filename : "+f._1 +" contxt: "+f._2))
    
    println("=================================size:"+nameList.length)
    
    nameList.foreach { x => println("names : "+ x) }
    
    fileContext.foreach { x => println(x) }
    
  }
}