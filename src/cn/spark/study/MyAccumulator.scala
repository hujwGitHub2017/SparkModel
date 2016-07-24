package cn.spark.study

import org.apache.spark.AccumulatorParam



object MyAccumulator extends AccumulatorParam[Long]{
  
    def zero(initialValue: Long): Long = {
        
       return 100l;
    }
   
    def addInPlace(v1: Long, v2: Long): Long = {
      
      println("v1 = "+v1+" v2 = "+v2)
       
       return v1+v2
      
    }
        
    
    /*override def addAccumulator(v1:Long, v2:Long):Long = {

      println(v1+","+v2);
      
       if (v2%2 == 0) {
        
        return v1+v2
        
      }
      
        return v1;
    }*/
   
}