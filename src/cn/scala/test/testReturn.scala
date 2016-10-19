package cn.scala.test

object testReturn {
  
  
  def main(args: Array[String]): Unit = {
    
    
    println(getHashCode("943207084,110"))
    
  }
  
  
   /**
   * 获取hash后的值 作为key的开始
   */
	def getHashCode(key:String):String={
	  
	  var code = ""
	  var nHashCode: Int = key.hashCode();
    if (nHashCode < 0) {
      nHashCode = nHashCode * (-1);
    }
    code = (nHashCode % 8).toString();
    if (code.length() == 1) {
      code = "0" + code;
    }
	  code
	}
  
  def getReturn(n:Int):Int = {
    
    var rnum = 0;
    
    if (n == 1) {
      
      rnum = 1
      
     return rnum
      
    }else if(n == 3){
      
      rnum = 3
      
    return  rnum
      
//      rnum
     
    }else {
      
      
      rnum = 5
      
      return  rnum
    }
    
    rnum = 0
    
    
    rnum
    
  }
  
}