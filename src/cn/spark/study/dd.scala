package cn.spark.study

import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object dd {
  
  def main(args: Array[String]): Unit = {
    
    
    val conf = new SparkConf().setAppName("ddd").setMaster("local")
    
     val sc = new SparkContext(conf)
    
    val data = sc.textFile("/user/wlan/dpilog_07_21/20160715153000/",1)
    
     data.filter { x => !x.startsWith("null") && x.split("\\t").length > 10 }.count()
    
    data.filter { x => !x.startsWith("null") && x.split("\\t").length > 10 }.repartition(1).saveAsTextFile("/user/wlan/hdata2")
    
    
        /*val conf = new SparkConf().setAppName("ddd").setMaster("local")
        
        val sc = new SparkContext(conf)
        
        
        var i = 2
        
        val dataList = Array(1,2,3,4,5,6,7)
        
        
        val dataRdd = sc.parallelize(dataList, 3)
        
       dataRdd.foreach { x => {
             
             i = i+x   
             
             println("x =" +x)
         } 
       
           println("i = "+i)
       }
        
        dataRdd.foreachPartition { x => 
            
            x.foreach { x => 
                  
              i = i+x
              
              println("x =" +x)
              
            }
            
            println("i = "+i)
          
        
        }
        
        
        
        println("data==="+i);*/
        
    
    val testData = new Array[Int](3)
    
      if (testData == null) {
        
        println(" no bady")
        
      }
    
  }
  
}