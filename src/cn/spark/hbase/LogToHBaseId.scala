package cn.spark.hbase

import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.util.Calendar
import java.text.SimpleDateFormat
import com.qzt360.util.PubFunc
import scala.collection.mutable.HashMap
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.TableName
import com.qzt360.auditLog.NetIdManager
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Durability
import com.qzt360.util.PubIdentityMessage
import com.qzt360.auditLog.GuestManager
import com.qzt360.auditLog.WLanManager
import com.qzt360.util.NewIdentityMessageToOld
import com.qzt360.util.NewIdentityMessageToOld
import com.qzt360.auditLog.AuthlogManager
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

/**
 * 身份日志生成身份信息
 */
object LogToHBaseId {
  
  def main(args: Array[String]): Unit = {
    
      val 	hashNetId		= new HashMap[String,String]()
      val   hashMac = new HashMap[String,String]()
      val   hashMobilePhone = new HashMap[String,String]()
      val   hashCert = new HashMap[String,String]()
      
      var lengths:String = "1000";
      
      var hdfsPath:String = ""
      
      if (args.length > 1) {
        
    	   lengths = args(0)
    	   hdfsPath = args(1)
      }else {
        
        hdfsPath = args(0)
      }
      
      val conf:Configuration = new Configuration()
      
          conf.set("fs.hdfs.impl.disable.cache", "true")
          conf.set("dfs.client.slow.io.warning.threshold.ms", "100000")
          //conf.set("mapreduce.input.lineinputformat.linespermap", "10000") // 
      var fs:FileSystem = null; 
      
      try {
        
    	  fs = FileSystem.newInstance(conf)
        
      } catch {
        
        case t: Exception => {
          
          t.printStackTrace()
        } 
          
      }
      
      var files = SparkPubFunc.getFilePathList(hdfsPath, fs, lengths)
      val hbaseFile = files._1
      val confs = new SparkConf()
      fs.close()
      val sc = new SparkContext(confs)
      
      if (hbaseFile.length > 0) {
        
        val fileRdd = sc.newAPIHadoopFile[LongWritable,Text,TextInputFormat](hbaseFile.mkString(","))
        
        val hadoopRdd = fileRdd.asInstanceOf[NewHadoopRDD[LongWritable,Text]];
         
        
        val hadoopRdd2 = hadoopRdd.mapPartitionsWithInputSplit((inputSplit: InputSplit, iterator: Iterator[(LongWritable, Text)]) => {
          
              val file = inputSplit.asInstanceOf[FileSplit]
              iterator.map(x => { file.getPath.getName + ">>" + x._2 })
              
        })
        
        hadoopRdd2.foreachPartition ( x => {
          
          val hbaseconf =  HBaseConfiguration.create()
        
          hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
          hbaseconf.set("hbase.zookeeper.quorum","cmserver,namenode01,datanode01")
          
          val tbl_id = new HTable(hbaseconf,TableName.valueOf("tbl_id_wj"))
          
          tbl_id.setAutoFlush(false, false)//取消自动刷数据入库
          
          tbl_id.setWriteBufferSize(6*1024*1024)
          
          val netIdManager = new NetIdManager()    
          
          val gm = new GuestManager()
          
          val wm = new WLanManager()
          
          val am = new AuthlogManager()
          
          x.foreach ( info  => {
            
        	  var strRowKey = ""
        	  
             val nameAndinfo = info.split(">>")
                 if (nameAndinfo.length==2 && nameAndinfo(0).startsWith("netidinfo_")) {
                 
                       if (netIdManager.isLegalData(nameAndinfo(1))) {
                         
                            
                            // 虚拟身份
                           if (!hashNetId.contains(netIdManager.getStrNetId() + "," + netIdManager.getStrNetIdType())) {
                      
                            hashNetId.put(netIdManager.getStrNetId() + "," + netIdManager.getStrNetIdType(), "")
                          
                      	    strRowKey = PubFunc.getHashCode(netIdManager.getStrNetId() + "," + netIdManager.getStrNetIdType())+netIdManager.getStrNetId() + "," + netIdManager.getStrNetIdType();
                      	  
                      	    val put = new Put(Bytes.toBytes(strRowKey))
                      	   
                      	    put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
                      	  
                      	    put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                      	  
                      	    tbl_id.put(put)
                          
                         }
                         // mac信息  
                         if (!hashMac.contains(netIdManager.getStrMac()+","+PubIdentityMessage.SERVER_ID_CARD_TYPE_10031)) {
                							 
                             hashMac.put(netIdManager.getStrMac()+","+PubIdentityMessage.SERVER_ID_CARD_TYPE_10031, "")
              							 strRowKey = PubFunc.getHashCode(netIdManager.getStrMac() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10031)+netIdManager.getStrMac() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10031
              							 val  put = new Put(Bytes.toBytes(strRowKey));
              							 put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
              							 put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
              					     tbl_id.put(put)
                           
                         }
                         
                         if (!"".equals(netIdManager.getStrCert())) {
                           
                           try {
                             
                             val  nCertType = netIdManager.getStrCertType().trim().toInt
                             // 手机号
                             if (nCertType == PubIdentityMessage.SERVER_ID_CARD_TYPE_10027 || nCertType == PubIdentityMessage.SERVER_ID_CARD_TYPE_10028) {
                               
                               if (PubFunc.isMobilePhone(netIdManager.getStrCert())) {
                                 
                                 if (!hashMobilePhone.contains(netIdManager.getStrCert() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10027)) {
                                   
                                    hashMobilePhone.put(netIdManager.getStrCert() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10027, "")
              											strRowKey = PubFunc.getHashCode(netIdManager.getStrCert() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10027)+netIdManager.getStrCert() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10027
              											val put = new Put(Bytes.toBytes(strRowKey));
              											put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
              							        put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                          	        tbl_id.put(put)
                                 }
                                 
                               }
                               // 身份证
                             }else if (nCertType == PubIdentityMessage.SERVER_ID_CARD_TYPE_CHN_10003) {
                               
                               if (PubFunc.isIDCard(netIdManager.getStrCert())) {
                                 
                                 if (!hashCert.contains(netIdManager.getStrCert() + "," + nCertType)) {
                                   
                                   hashCert.put(netIdManager.getStrCert() + "," + nCertType, "")
              										 strRowKey = PubFunc.getHashCode(netIdManager.getStrCert() + "," + nCertType)+netIdManager.getStrCert() + "," + nCertType
              										 val put = new Put(Bytes.toBytes(strRowKey))
              										 put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
              							       put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                          	       tbl_id.put(put)
                                   
                                 }
                                 
                               }
                               
                             }
                             
                           } catch {
                             case t: Exception => t.printStackTrace() 
                           }
                           
                         }
        						
                     }
    						
                 }else if (nameAndinfo.length==2 && nameAndinfo(0).startsWith("guestinfo_")) {
                   
                   if (gm.isLegal(nameAndinfo(1))) {
                     
                     if (!hashCert.contains(gm.getStrIdCard() + "," + gm.getnIdCardType())) {
                       
                       hashCert.put(gm.getStrIdCard() + "," + gm.getnIdCardType(), "")
                       
                       strRowKey = PubFunc.getHashCode(gm.getStrIdCard() + "," + gm.getnIdCardType())+gm.getStrIdCard() + "," + gm.getnIdCardType()
                       
                       val put = new Put(Bytes.toBytes(strRowKey));
                       
                       put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
  							       put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
              	       tbl_id.put(put)
                       
                     }
                     
                   }
                   
                   
                 }else if (nameAndinfo.length==2 && (nameAndinfo(0).startsWith("wlan_")||nameAndinfo(0).startsWith("tmac_"))) {
                   
                   val strInputFileName = nameAndinfo(0)
                   
                   if (strInputFileName.startsWith("wlan_") && strInputFileName.split("_")(1).length() > 3) { //百米数据
                     
                     if (wm.isLegal(nameAndinfo(1))) {
                       
                       if (!hashMac.contains(wm.getStrMac() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10031)) {
                         
                         hashMac.put(wm.getStrMac() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10031, "")
                         
                         strRowKey = PubFunc.getHashCode(wm.getStrMac() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10031)+wm.getStrMac() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10031
                         
                         val put = new Put(Bytes.toBytes(strRowKey));
                         put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
    							       put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                	       tbl_id.put(put)
                         
                       }
                       
                     }
                     
                   }else { // 其他厂家数据（145卡口数据）
                     
                     if (wm.isLegalWlan(nameAndinfo(1))) {//终端mac扫描数据 wlan_xx_xxxxxxxx_
                       
                       if (!hashMac.contains(wm.getStrMac() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10031)) {
                         
                          hashMac.put(wm.getStrMac() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10031, "")
                         
                          strRowKey = PubFunc.getHashCode(wm.getStrMac() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10031) + wm.getStrMac() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10031
                          
                          val put = new Put(Bytes.toBytes(strRowKey))
                          
                         put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
    							       put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                	       tbl_id.put(put)                      
                	     }
                       
                       
                       if (!PubFunc.isNull(wm.getStrIdType()) && !PubFunc.isNull(wm.getStrIdCode())) {
                         
                         if (!hashNetId.contains(NewIdentityMessageToOld.changeIdentityMessage(wm.getStrIdType()) + "," + wm.getStrIdCode())) {
                           
                            hashNetId.put(NewIdentityMessageToOld.changeIdentityMessage(wm.getStrIdType()) + "," + wm.getStrIdCode(), "")
                            
                            strRowKey = PubFunc.getHashCode(NewIdentityMessageToOld.changeIdentityMessage(wm.getStrIdType()) + "," + wm.getStrIdCode())+NewIdentityMessageToOld.changeIdentityMessage(wm.getStrIdType()) + "," + wm.getStrIdCode()
                            
                            val put = new Put(Bytes.toBytes(strRowKey))
                            
                            put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
        							      put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                    	      tbl_id.put(put) 
                            
                           
                         }
                         
                       }
                       
                     }
                   }
                   
                 }else if (nameAndinfo.length==2 && nameAndinfo(0).startsWith("authlog_")) { //authlog_
                   
                   if (am.isLegal(nameAndinfo(1))) {// 终端上下线数据 authlog_xx_xxxxxxxx
                     
                       if (!hashMac.contains(am.getStrTMac() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10031)) {
                         
                          hashMac.put(am.getStrTMac() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10031, "")
                          
                          strRowKey = PubFunc.getHashCode(am.getStrTMac() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10031)+am.getStrTMac() + "," + PubIdentityMessage.SERVER_ID_CARD_TYPE_10031
                          
                          val put = new Put(Bytes.toBytes(strRowKey));
                          
                          put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
        							    put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                    	    tbl_id.put(put)
                          
                       }
                       
                       //
          						val strAuthType = am.getStrAuthType();
          						val strAuthCode = am.getStrAuthCode();
          						// 手机号
          						if (!PubFunc.isNull(strAuthType) && !PubFunc.isNull(strAuthCode)) {
          						  
          						  val strOldType = ""+NewIdentityMessageToOld.changeIdentityMessage(strAuthType)
          						  
          						  if (!"-1".equalsIgnoreCase(strOldType)) {
          						    
          						    if (!hashMobilePhone.contains(strAuthCode + "," + strOldType)){
          						      
          						      hashMobilePhone.put(strAuthCode + "," + strOldType, "")
          						      
          						      strRowKey = PubFunc.getHashCode(strAuthCode + "," + strOldType)+strAuthCode + "," + strOldType
          						      
          						      val put = new Put(Bytes.toBytes(strRowKey));
                            put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes(""), Bytes.toBytes(""))
          							    put.setDurability(Durability.SKIP_WAL)//放弃写WAL日志，从而提高数据写入的性能
                      	    tbl_id.put(put)
          						    }
          						    
          						    
          						  }
          						  
          						}
                     
                   }
                   
                 }
            
          })
          
           tbl_id.flushCommits() //批量入数据
          
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
      
       sc.stop()
    
  }
  
  
     def moviFile(fileSource:ArrayBuffer[String],fileSystem:FileSystem):Unit={
      
      var filename = "";
      
       for (files <- fileSource) {
        try{
           
        	filename = files.substring(files.lastIndexOf("/")+1, files.length()) //原始文件名称
        	
        	if (!fileSystem.exists(new Path("/user/hujw/hbase_idTrail"))) {
        	  
        	    fileSystem.mkdirs(new Path("/user/hujw/hbase_idTrail"))
        	  
        	  
        	}
          //移动文件
          fileSystem.rename(new Path(files), new Path("/user/hujw/hbase_idTrail/"+filename))
              
          }catch{
            case es:Exception=>{
              println("FileMoveFaild----"+es.getMessage+"---filename=="+filename)
            }
         }
      }
    }
}