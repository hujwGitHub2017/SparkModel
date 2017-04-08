package cn.spark.hbase

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes

object ReadDataFromHbase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HbaseModel")
    val sc  = new SparkContext(conf)
    val tableName = ""
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","slave1,slave2,slave3")
    //设置zookeeper连接端口，默认2181  
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181") 
    conf.set(TableInputFormat.INPUT_TABLE, "tablename")
    
    /** 加入scan 过滤器 
     *  
    conf.set(TableInputFormat.SCAN, ScanToString)
    * 
    */
    
    
     // 如果表不存在则创建表  
    val admin = new HBaseAdmin(hbaseConf)  
    if (!admin.isTableAvailable(tableName)) {  
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))  
      admin.createTable(tableDesc)  
    }
    //读取数据并转化成rdd  
    val hBaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],  
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],  
      classOf[org.apache.hadoop.hbase.client.Result])  
  
    val count = hBaseRDD.count()  
    println(count)  
    hBaseRDD.foreach{case (_,result) =>{  
      //获取行键  
      val key = Bytes.toString(result.getRow)  
      //通过列族和列名获取列  
      val name = Bytes.toString(result.getValue("cf".getBytes,"name".getBytes))  
      val age = Bytes.toInt(result.getValue("cf".getBytes,"age".getBytes))  
      println("Row key:"+key+" Name:"+name+" Age:"+age)  
    }}  
  
    sc.stop()  
    admin.close()  
  }
}