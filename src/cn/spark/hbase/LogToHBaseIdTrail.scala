package cn.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import scala.collection.mutable.ArrayBuffer
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.TableName
import com.qzt360.auditLog.NetIdManager
import com.qzt360.util.PubFunc
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Durability
import com.qzt360.util.PubIdentityMessage
import com.qzt360.auditLog.GuestManager
import com.qzt360.auditLog.WLanManager
import scala.collection.mutable.HashMap
import com.qzt360.util.NewIdentityMessageToOld
import com.qzt360.auditLog.AuthlogManager
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat


/**
 * 身份日志生成身份轨迹
 */
object LogToHBaseIdTrail {
  
  
  def main(args: Array[String]): Unit = {
    
    var lengths:String = "1000"
    var hdfsPath:String = ""
    
    if (args.length > 1) {
        
    	   lengths = args(0)
    	   hdfsPath = args(1)
    }else {
        
        hdfsPath = args(0)
    }
    
    val hashNetId = new HashMap[String,String]()
    
    val conf:Configuration =  new Configuration()
    
      conf.set("fs.hdfs.impl.disable.cache", "true")
      conf.set("dfs.client.slow.io.warning.threshold.ms", "100000")
      conf.set("mapreduce.input.lineinputformat.linespermap", "10000")
    var fs:FileSystem = null   
    
    try {
      
      fs = FileSystem.newInstance(conf)
      
    } catch {
      case t: Exception => {
       
        t.printStackTrace() 
        
      }
    }
    
    var files = SparkPubFunc.getFilePathList(hdfsPath,fs,lengths)
    fs.close()
    
    val hbaseFile = files._1
    val confs = new SparkConf()
    val sc= new SparkContext(confs)
    
    
    if (hbaseFile.length > 0) {
      
      
      
//       val fileRdd = sc.newAPIHadoopFile[LongWritable,Text,TextInputFormat](hbaseFile.mkString(","))
      
       val fileRdd = sc.newAPIHadoopFile[LongWritable,Text,NLineInputFormat](hbaseFile.mkString(","))

        
       val hadoopRdd = fileRdd.asInstanceOf[NewHadoopRDD[LongWritable,Text]];
 
        
       val hadoopRdd2 = hadoopRdd.mapPartitionsWithInputSplit((inputSplit: InputSplit, iterator: Iterator[(LongWritable, Text)]) => {
          
              val file = inputSplit.asInstanceOf[FileSplit]
              iterator.map(x => { file.getPath.getName + ">>" + x._2 })
              
        })
        
        
       hadoopRdd2.foreachPartition ( x => {
         
         val nim = new NetIdManager()
         val gm = new GuestManager()
         val wm = new WLanManager()
         val am = new AuthlogManager()
         
         val hbaseConf = HBaseConfiguration.create()
         
         hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
         hbaseConf.set("hbase.zookeeper.quorum","cmserver,namenode01,datanode01")
         
         val tbl_id_trail = new HTable(hbaseConf,TableName.valueOf("tbl_id_trail_wj"))
         
         tbl_id_trail.setAutoFlush(false,false)
         
         tbl_id_trail.setWriteBufferSize(6*1024*1024)
         
         x.foreach ( info => {
           
           var strRowKey = ""
           
           val nameAndinfo = info.split(">>")
           
           if (nameAndinfo.length == 2 && nameAndinfo(0).startsWith("netidinfo_")) {
             
              if (nim.isLegalData(nameAndinfo(1))) {
                
                strRowKey = nim.getStrNetId()+ ","+ nim.getStrNetIdType()+ ","+ PubFunc.IntTime2FormatStr(nim.getStrFirstTime(),"yyyy-MM-dd HH:mm:ss") +"," + "01_"+ nim.getStrQztId()
                
                var put = new Put(Bytes.toBytes(PubFunc.getHashCode(strRowKey)+strRowKey))
                      	   
                put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
                      	  
                put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                      	  
                tbl_id_trail.put(put)
                
                // mac信息
						  	strRowKey = nim.getStrMac()+ ","+ PubIdentityMessage.SERVER_ID_CARD_TYPE_10031+ ","+ PubFunc.IntTime2FormatStr(
											nim.getStrFirstTime(),"yyyy-MM-dd HH:mm:ss") + "," + "01_"+ nim.getStrQztId()
											
								put = new Put(Bytes.toBytes(PubFunc.getHashCode(strRowKey)+strRowKey))
                      	   
                put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
                      	  
                put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                      	  
                tbl_id_trail.put(put)
                
                if (!"".equals(nim.getStrCert())) {
                  
                  try {
                    
                    var nCertType = nim.getStrCertType().trim().toInt
                    
                    if (nCertType == PubIdentityMessage.SERVER_ID_CARD_TYPE_10027 || nCertType == PubIdentityMessage.SERVER_ID_CARD_TYPE_10028) {
                      
                      if (PubFunc.isMobilePhone(nim.getStrCert())) { // 手机号
                        
                        strRowKey = nim.getStrCert()+ ","+ PubIdentityMessage.SERVER_ID_CARD_TYPE_10027+ ","+ PubFunc.IntTime2FormatStr(
															nim.getStrFirstTime(),
															"yyyy-MM-dd HH:mm:ss")+ "," + "01_"+ nim.getStrQztId()
															
															put = new Put(Bytes.toBytes(PubFunc.getHashCode(strRowKey)+strRowKey))
                      	   
                              put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
                                    	  
                              put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                                    	  
                              tbl_id_trail.put(put)
                        
                      }
                      
                    }else if (nCertType == PubIdentityMessage.SERVER_ID_CARD_TYPE_CHN_10003) { // 身份证
                      
                      if (PubFunc.isIDCard(nim.getStrCert())) {
                        
                        
                        strRowKey = nim.getStrCert()+ ","+ nCertType+ ","+ PubFunc.IntTime2FormatStr(
															nim.getStrFirstTime(),
															"yyyy-MM-dd HH:mm:ss") + "," + "01_"+ nim.getStrQztId()
															
                              put = new Put(Bytes.toBytes(PubFunc.getHashCode(strRowKey)+strRowKey))
                      	   
                              put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
                                    	  
                              put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                                    	  
                              tbl_id_trail.put(put)
                        
                      }
                        
                    }
                    
                  } catch {
                    case t: Throwable => t.printStackTrace() 
                  }
                  
                }

                
              }
             
           }else if (nameAndinfo.length == 2 && nameAndinfo(0).startsWith("guestinfo_")) {
             
             if (gm.isLegal(nameAndinfo(1))) {
               
                var nTime = gm.getNLoginTime()
  							if (nTime <= 0) {
  								nTime = gm.getNLogoutTime()
  							}
							
								 strRowKey = gm.getStrIdCard()+ ","+ gm.getnIdCardType()+ ","+ PubFunc.IntTime2FormatStr(
								           "" + nTime,
											     "yyyy-MM-dd HH:mm:ss") + "," + "01_"+ gm.getNCustomerId()
											     
								var  put = new Put(Bytes.toBytes(PubFunc.getHashCode(strRowKey)+strRowKey))
                      	   
                put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
                      	  
                put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                      	  
                tbl_id_trail.put(put)			     
											     
							
             }
             
           }else if (nameAndinfo.length ==2 && (nameAndinfo(0).startsWith("wlan_")||nameAndinfo(0).startsWith("tmac_"))) {
             
             var strComSNNew:String = ""
             
             
             if (nameAndinfo(0).startsWith("wlan_") && nameAndinfo(0).split("_")(1).length() > 3) {
               
               if (wm.isLegal(nameAndinfo(1))) {
                 
                  strRowKey = wm.getStrMac()+ ","+ PubIdentityMessage.SERVER_ID_CARD_TYPE_10031+ ","+ PubFunc.IntTime2FormatStr("" + wm.getnTime(),
											"yyyy-MM-dd HH:mm:ss") + "," + "01_"+ wm.getnCustomerId()
											
									val  put = new Put(Bytes.toBytes(PubFunc.getHashCode(strRowKey)+strRowKey))
                      	   
                  put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
                        	  
                  put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                        	  
                  tbl_id_trail.put(put)			     		
                 
               }
               
             }else {
               
               if (nameAndinfo(0).startsWith("wlan_")) {
                 
                 strComSNNew = nameAndinfo(0).split("_")(1)
                 
               }else {
                 strComSNNew = "01"
               }
               
               if (wm.isLegalWlan(nameAndinfo(1))) {// 终端mac扫描数据 wlan_xx_xxxxxxxx
                 
                 val strHashNetKey = wm.getStrMac()+ ","+ PubIdentityMessage.SERVER_ID_CARD_TYPE_10031+ ","+ wm.getnTime()/300 + "," + strComSNNew + "_"+ wm.getStrCustomerId()
                 
                 if (!hashNetId.contains(strHashNetKey)) {
                   
                       hashNetId.put(strHashNetKey, "")
                   
                        strRowKey = wm.getStrMac()+ ","+ PubIdentityMessage.SERVER_ID_CARD_TYPE_10031+ ","+ PubFunc.IntTime2FormatStr("" + wm.getnTime(),
                    		   "yyyy-MM-dd HH:mm:ss") + "," + strComSNNew + "_"+ wm.getStrCustomerId()
                    		   
                    	 val  put = new Put(Bytes.toBytes(PubFunc.getHashCode(strRowKey)+strRowKey))
                       
                       put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
                       
                       put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                       
                       tbl_id_trail.put(put)	
                 }
                 
                 if (!PubFunc.isNull(wm.getStrIdType()) && !PubFunc.isNull(wm.getStrIdCode())) {// 虚拟身份
                   
                   val   strHashNetKey = wm.getStrMac()+ ","+ PubIdentityMessage.SERVER_ID_CARD_TYPE_10031+ ","+ wm.getnTime()/300 + "," + strComSNNew + "_"+ wm.getStrCustomerId()
                   
                   if (!hashNetId.contains(strHashNetKey)) {
                     
                     hashNetId.put(strHashNetKey, "")
                     
                      strRowKey = wm.getStrIdCode()+ ","+ NewIdentityMessageToOld.changeIdentityMessage(wm.getStrIdType())+ ","+ PubFunc.IntTime2FormatStr("" + wm.getnTime(),
													"yyyy-MM-dd HH:mm:ss") + "," + strComSNNew + "_"++ wm.getStrCustomerId()
													
													
											val  put = new Put(Bytes.toBytes(PubFunc.getHashCode(strRowKey)+strRowKey))
                       
                      put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
                       
                      put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                       
                      tbl_id_trail.put(put)		
                     
                   }
                   
                   
                 }
                 
               }
               
               
               
             }
             
           }else if (nameAndinfo.length == 2 && nameAndinfo(0).startsWith("authlog_")) {
             
              val aStrFileSplit = nameAndinfo(0).split("_")
              
              var strComSNNew:String = ""
              
    				  if(aStrFileSplit.length > 3) //wifi卡口数据
    				  {
    					  strComSNNew = aStrFileSplit(1)
    				  }else{
    					  strComSNNew = "01"
    				  }
             
             if (am.isLegal(nameAndinfo(1))) {
               
               	val ltime = if(am.getlLoginTime()==0) am.getlLogoutTime() else am.getlLoginTime()
               	
               	if (ltime > 0) {
               	  
               	  	strRowKey = am.getStrTMac()+ ","+ PubIdentityMessage.SERVER_ID_CARD_TYPE_10031+ ","+ PubFunc.IntTime2FormatStr("" + ltime,
												"yyyy-MM-dd HH:mm:ss") + "," + strComSNNew + "_"+ am.getStrCustomerId()
												
										val  put = new Put(Bytes.toBytes(PubFunc.getHashCode(strRowKey)+strRowKey))
                       
                    put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
                       
                    put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                       
                    tbl_id_trail.put(put)		
                    
                    
  								val  strAuthType = am.getStrAuthType()
  								val strAuthCode = am.getStrAuthCode()
  								
  								if (!PubFunc.isNull(strAuthType) && !PubFunc.isNull(strAuthCode)) {
  								  
                     	val strOldType = ""+NewIdentityMessageToOld.changeIdentityMessage(strAuthType)
                     	if (!"-1".equalsIgnoreCase(strOldType)) {
                     	  
                     	 strRowKey = strAuthCode+ ","+ strOldType+ ","+ PubFunc.IntTime2FormatStr("" + ltime,
														"yyyy-MM-dd HH:mm:ss") + "," + strComSNNew + "_"+ am.getStrCustomerId()
														
												val  put = new Put(Bytes.toBytes(PubFunc.getHashCode(strRowKey)+strRowKey))
                       
                        put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
                           
                        put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                           
                        tbl_id_trail.put(put)		
											 			
                     	  
                     	}

  								  
  								}
										
               	}

               
             }
             
           }  
           
           
         })         
         
          tbl_id_trail.flushCommits() //批量入数据
         
       }) 
      
    }
    
    try {
          
        conf.set("dfs.support.append", "true")
        conf.set("dfs.client.slow.io.warning.threshold.ms", "100000");
        fs = FileSystem.newInstance(conf) //(conf)//get(conf)
        
        if (files._1.length > 0) {
          
        	moviFile(files._1, fs)
        }
        
        if (files._2.length > 0) {
          
        	moviFile(files._2, fs)
        }
        
         
        fs.close() 
          
        } catch {
          case es: Exception => {
            
            println("FS move ")
            es.printStackTrace()
          } 
        }
      
//       sc.stop()
    
  }
  
  def moviFile(fileSource:ArrayBuffer[String],fileSystem:FileSystem):Unit={
      
      var filename = "";
      
       for (files <- fileSource) {
        try{
           
        	filename = files.substring(files.lastIndexOf("/")+1, files.length()) //原始文件名称
        	
        	if (!fileSystem.exists(new Path("/user/wjpt/"+PubFunc.getFilePath(filename)))) {
        	  
        	    fileSystem.mkdirs(new Path("/user/wjpt/"+PubFunc.getFilePath(filename)))
        	  
        	  
        	}
          //先改下文件名
          fileSystem.rename(new Path(files), new Path("/user/wjpt/"+PubFunc.getFilePath(filename)+"/"+filename))
              
          }catch{
            case es:Exception=>{
              println("FileMoveFaild----"+es.getMessage+"---filename=="+filename)
            }
         }
      }
    }
  
  
}