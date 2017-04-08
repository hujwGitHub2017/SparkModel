package cn.scala.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer

object combineByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local")
    val sc = new SparkContext()
    val data = Array(("a",1),("a",2),("a",3),("b",4),("b",5))
    val dataRdd = sc.parallelize(data, 1)
    val dataCombine = dataRdd.combineByKey(v =>{val nums = new ArrayBuffer[Int];nums+=v;nums}, (c:ArrayBuffer[Int],v) => {c+=v;c}, (c1:ArrayBuffer[Int],c2:ArrayBuffer[Int])=>{c1.++=(c2)})
    dataCombine.foreach(f =>{
      println("key "+f._1)
      println("value "+f._2)
    })
  }
}