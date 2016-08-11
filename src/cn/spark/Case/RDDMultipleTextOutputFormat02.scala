package cn.spark.Case

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class RDDMultipleTextOutputFormat02 extends MultipleTextOutputFormat[Any,Any] {
  
  
     override def generateFileNameForKeyValue(key: Any, value: Any, name: String):String={
    
       println("name : "+name)  
       
       key.asInstanceOf[String]
    
  }
  
}