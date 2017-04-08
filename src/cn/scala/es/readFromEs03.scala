package cn.scala.es

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapred.JobConf
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.apache.hadoop.io.MapWritable;

/**
 * 
 * 这个
 */
// git remote add origin https://github.com/hujwGitHub2017/SparkModel.git
object readFromEs03 {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("readFromEs")
     val sc = new SparkContext(conf)
     val jobConf = new JobConf(sc.hadoopConfiguration)
     jobConf.set(ConfigurationOptions.ES_RESOURCE_READ,args(0)) //helloes/demo  index/type
     jobConf.set(ConfigurationOptions.ES_NODES,args(1))
     val dataRdd = sc.hadoopRDD(jobConf,classOf[EsInputFormat[Object,MapWritable]],classOf[Object],classOf[MapWritable])
     val mapWriteRdd = dataRdd.map{case (key,value)=>value}
     
    
  }
 
}