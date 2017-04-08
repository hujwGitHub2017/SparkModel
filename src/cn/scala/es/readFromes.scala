package cn.scala.es

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.MapWritable
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import java.util.Set

object readFromes {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("readFromEs")
    
    val sc =  new SparkContext(conf)
    
//    conf.set("es.resource", "radio/artists");       
//    conf.set("es.query", "?q=me*"); 
// conf.set("es.nodes", "127.0.0.1")    
    
    val confs = new Configuration()
    
    confs.set("es.nodes", "192.168.36.26")
    
    confs.set("es.port", "9200")
    
    confs.set("es.resource", "wjlog_20170405")
    
    //confs.set("es.query", "?q=192.168*")
    

    val esRdd = sc.newAPIHadoopRDD(confs,classOf[EsInputFormat[Text,MapWritable]],classOf[Text],classOf[MapWritable])
    
//    esRdd.foreach(f=>{
//      
//      println(f._1)
//      
//    })
    
   
    
    esRdd.foreach(f =>{
      
      println(f._1+" values: ")
      
      /*val keySet = f._2.keySet().iterator()
      
      while (keySet.hasNext()) {
        
        println("valus : "+keySet.next().getClass)
        
      }*/
      
      println(" smac  key "+ f._2.containsKey(new Text("smac")))
      
      println(" smac value : "+f._2.get( new Text("smac")))
      
      println("id key "+f._2.containsKey(new Text("_id")))
      
    })
    
    
    
    val count = esRdd.count()
    println("count == "+count)
  }
  
  
}