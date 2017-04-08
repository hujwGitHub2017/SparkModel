package cn.scala.es

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapred.JobConf
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.apache.hadoop.mapred.FileOutputCommitter
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.fs.Path

object savaAsData2Es {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("readFromEs")
    val sc = new SparkContext(conf)
    val dataRdd = sc.parallelize(Array(("A",2),("A",1),("B",6),("B",3),("B",7)), 2)
    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.set("mapred.output.format.class","org.elasticsearch.hadoop.mr.EsOutputFormat")
    jobConf.set(ConfigurationOptions.ES_RESOURCE_READ,args(0)) //helloes/demo  index/type
    jobConf.set(ConfigurationOptions.ES_NODES,args(1))
    jobConf.setOutputCommitter(classOf[FileOutputCommitter])
    FileOutputFormat.setOutputPath(jobConf,new Path("-"))
    dataRdd.saveAsHadoopDataset(jobConf)
  }  
}