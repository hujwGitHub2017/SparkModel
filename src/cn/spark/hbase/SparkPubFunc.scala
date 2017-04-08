package cn.spark.hbase

import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.FileSystem
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.Path

object SparkPubFunc {
  
  
   def getFilePathList(parentPath:String,fs:FileSystem,len:String): (ArrayBuffer[String],ArrayBuffer[String]) = {
      
      var fileList = new ArrayBuffer[String]()
      var otheFiles = new ArrayBuffer[String]()
      
      try {
        
        val fPath = new Path(parentPath)
        
        if (fs.exists(fPath)) {
          
          val files = fs.listStatus(fPath)
          
          for(f <- files){
            
            if (fileList.length > len.trim().toInt) {
              
              return (fileList,otheFiles)
              
            }
            
            
            
            val strDayTime:String = getTime()
            
            val name:String = f.getPath().getName()
            
            val  isWlanFile:Boolean = (name.startsWith("wlan_")) && name.contains(strDayTime);
			      val  isAuthlogFile:Boolean = (name.startsWith("authlog_")) && (name.contains(strDayTime));
			      val  isTmacFile:Boolean = (name.startsWith("tmac_")) && name.contains(strDayTime);
			      val  isNetidfoFile:Boolean = name.startsWith("netidinfo_")  &&  name.contains(strDayTime)
			      val  isGuestFile:Boolean = name.startsWith("guestinfo_") && name.contains(strDayTime)
			      
			      
            
            if (isWlanFile || isAuthlogFile || isTmacFile || isNetidfoFile || isGuestFile) {
              
               fileList+=f.getPath().toString()
              
            }else {
              
              otheFiles+=f.getPath().toString()
              
            }
            
            
          }
          
        }
        
      } catch {
        case t: Exception => t.printStackTrace() 
      }
      
      (fileList,otheFiles)
      
    }
  
   def getTime():String = {
      
      var DayTime:String = ""
      
      // 每天统计当天的数据；前一天的yyyyMMdd
		 val time = Calendar.getInstance();
		 val strDayTime:String = time.get(Calendar.YEAR) + "" + (if(time.get(Calendar.MONTH)+1 > 9)  "" else "0") + (time.get(Calendar.MONTH) + 1) + ( if(time.get(Calendar.DAY_OF_MONTH)>9) "" else "0") + time.get(Calendar.DAY_OF_MONTH);
		 val sdf:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
		 DayTime = sdf.parse(strDayTime).getTime().toString().substring(0,10)
		 DayTime
    }
}