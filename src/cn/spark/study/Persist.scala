package cn.spark.study

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File


object Persist {
  
  val hadoopConf = new Configuration()
  
  def main(args: Array[String]): Unit = {
    
    val fs = FileSystem.newInstance(hadoopConf)
    
//    fs.delete(f, recursive)
    
    val conf = new SparkConf()
             .setMaster("local")
             .setAppName("Persist")
             
    val sc = new SparkContext(conf)
    
    val file = sc.textFile("D:\\data\\testData\\study.txt", 2)
    
    
    println(file.count())
    
    new File("D:\\data\\testData\\study.txt").delete()
    
    for(i <- 1 to 3){
      
      
      Thread.sleep(2000)
      
     /* if (i == 2) {
        
       fs.deleteOnExit(new Path("D:\\data\\testData\\study.txt"))
        
      }*/
      
      println("wait-----")
    }
    
       println(file.count())
     
    fs.close()
  }
  
}