package cn.spark.study

object Test {
  def main(args: Array[String]): Unit = {
    
    val aa = Array[Int](1,2,3)
    
    val d =  aa.toBuffer
    
    
    for(i <- d){
      
      println(i)
    }
    
    println("==================================================")
    
   d.-=(1)
   
   
   for(i <- d){
      
      println(i)
    }
    
  }
  
}