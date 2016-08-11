package cn.spark.Case

import org.apache.hadoop.mapred.TextOutputFormat

class RDDMultipleTextOutputFormat01 extends MyMultipleTextOutputFormat[Any,Any]{
  
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String):String={
    
    key.asInstanceOf[String]
    
  }
  
  
}