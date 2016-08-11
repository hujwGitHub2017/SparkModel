package cn.spark.Case

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.rdd.HadoopRDD
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.FileSplit

object MultipleTextOutput {
  
  def main(args: Array[String]): Unit = {
  
    val conf = new SparkConf().setMaster("local").setAppName("MultipTextOutPut")
    
    val sc = new SparkContext(conf)
    
//    val file = sc.textFile("D:\\data\\study.txt", 1)
    
    val fileRdd = sc.newAPIHadoopFile[Text,Text,cn.spark.Case.myInputFormat]("D:\\data\\study.txt")
    
   /* fileRdd.foreachPartition(f => {
      
      f.foreach(line => {
        
        println(" key : "+line._1)
        
        println(" value : "+ line._2)
        
        println("-------------------------------------")
        
      })
      
    })  */

    
      
      /**
       * 产生的数据：
       * 
       * -------------------------------------
         key : study.txt
         value : 2	create	10.147.3.62	183.198.168.26	55293	56316	2016-05-19 00:47:54	0
        -------------------------------------
         key : study.txt
         value : 2	close	10.147.48.99	183.198.168.3	50173	51196	2016-05-19 00:47:55	200
        -------------------------------------
         key : study.txt
         value : 2	close	10.147.118.16	183.198.168.20	33789	34812	2016-05-19 00:47:55	322
        -------------------------------------
         key : study.txt
         value : 2	close	10.147.27.71	183.198.168.25	23549	24572	2016-05-19 00:47:55	250
        -------------------------------------
         key : study.txt
         value : 2	close	10.171.63.71	183.197.48.167	57341	58364	2016-05-19 00:47:55	2
        -------------------------------------
       *  
       */
      
      /**
       * 指定文件 名 写出
       * 
       */
      
    /*  val hadoopRdd = fileRdd.asInstanceOf[HadoopRDD[Text,Text]] 
    
      val fileAndLine =  hadoopRdd.mapPartitionsWithInputSplit((inputsplit:InputSplit,iterrator:Iterator[(Text,Text)]) => {
        
        var filesplit = inputsplit.asInstanceOf[FileSplit]
        
        
        iterrator.map(line =>(filesplit.getPath.getName,line._1+"--"+line._2))
        
      })*/
    
    
    val fileAndLine = fileRdd.mapPartitionsWithSplit((num:Int,iterrator:Iterator[(Text,Text)])=>{
      
      iterrator.map(line =>(line._1+"",line._1+" "+line._2) )
    })
      
    
    /**
     * 
     * 多 目录输出，这种方式重写的方法是 比较多的。。
     * 
     */
    
      fileAndLine.saveAsHadoopFile("D:\\data\\testData01\\", classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat01])      
      
      
//    fileAndLine.saveAsHadoopFile("D:\\data\\testData01\\", classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat02])
    
  }
}