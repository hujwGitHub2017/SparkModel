package cn.scala.jdbc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.sql.DriverManager
import java.sql.ResultSet
import org.apache.spark.rdd.JdbcRDD

/**
 * 
 * 该模版定义的是从数据库中查询到数据并转换成Rdd
 */
object ReadDataFromDB {
  def main(args: Array[String]): Unit = {
    val conf  = new SparkConf().setAppName("jdbc")
    val sc = new SparkContext(conf)
    val dataRdd = new JdbcRDD(sc,createConnection,"",1,3,2,extracValues)
    print(dataRdd.collect().toList)
  }
  
  def createConnection()={
    val db_url = "192.168.36.43"
    val db_name = "beap"
    // mysql : com.mysql.jdbc.Driver
    Class.forName("org.postgresql.Driver").newInstance();
    DriverManager.getConnection("jdbc:postgresql://"+db_url+":5432/"+db_name+"?user="+"postgres"+"&password="+"123456"+"&loginTimeout=20&allowEncodingChanges=true")
  }
  
  def extracValues(r:ResultSet)={
    (r.getString(1),r.getString(2))
  }
}