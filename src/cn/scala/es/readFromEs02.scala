package cn.scala.es

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text

object readFromEs02 {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("readFromEs")
    val sc = new SparkContext(conf)
    //创建esRDD 
    val conf1 = new Configuration()
     conf1.set("es.resource", "id_/id_type")
      //es.nodes
     conf1.set("es.nodes", "192.168.10.16")
     conf1.set("es.output.json", "true")
    val esRdd = sc.newAPIHadoopRDD(conf1, classOf[MyEsInputFormat[Text,Text]], classOf[Text], classOf[Text])
    
    val strEsRdd = esRdd.map(f =>(f._1.toString().split(",")(1),f._1.toString().split(",")(0)+" : "+f._2.toString()))
    val typeCount = strEsRdd.groupByKey().map(f => (f._1,f._2.size))
    
    
    typeCount.repartition(1).foreach(f => {
       println("type: "+f._1+" -- count-=  "+f._2) 
    })
    
  }
  
  
}