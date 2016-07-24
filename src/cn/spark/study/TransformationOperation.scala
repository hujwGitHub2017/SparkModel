package cn.spark.study

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object TransformationOperation {
  
    def main(args: Array[String]): Unit = {
      
//      JoinOperation02()
      
      CountBykeyOperation()
      
    }
    
    def JoinOperation02():Unit={
      
      val conf = new SparkConf()
            .setAppName("TransformationOperation")
            .setMaster("local")
            
      val sc = new SparkContext(conf)
      
      val file01Rdd = sc.textFile("D:\\data\\hb\\filename.txt", 1)
      
      val logRdd = sc.textFile("D:\\data\\hb\\Log_Dpi.txt.2016-07-01", 1)
      
      val filename = file01Rdd.map { x => (x.split(" ")((x.split(" ").length)-1),1) }
      
      val logs = logRdd.map { x => (x.split("\\]\\[")(2),2) }
      
      val success = filename.join(logs)
      
      println("file count"+filename.count())
      println("logs count"+logs.count())
      
      println("success count"+success.distinct().count())
      
//      success.saveAsTextFile("D:\\data\\hb\\count")
      
    /*  logs.foreach { x =>{
        
       
        println(x)
         
      } }*/
			
			
			
			
			
      
    }
    
    def JoinOperation():Unit={
      
      val conf = new SparkConf()
            .setAppName("TransformationOperation")
            .setMaster("local")
            
      val sc = new SparkContext(conf)
      
      val nameList = Array(Tuple2(1,"Tom"),
				Tuple2(2, "Jon"),
				Tuple2(2, "Jon2"),
				Tuple2(3, "alin"),
				Tuple2(4, "yiming"))
      
      val scoreList = Array(Tuple2(1, 20),
			  Tuple2(2, 40),
				Tuple2(2, 44),
				Tuple2(3, 80),
				Tuple2(4, 100))
				
			val nameRdd = sc.parallelize(nameList, 1)	
			val scoreRdd = sc.parallelize(scoreList, 1)
			
			
			val nameScore = nameRdd.join(scoreRdd).sortByKey(true, 2)
			
			
			nameScore.foreach(v => {
			  
			  println("ID :"+v._1)
			  println("name : "+v._2._1)
			  println("score : "+v._2._2)
			  
			})
      
    }
    
    def CogropOperation():Unit={
      
      val conf = new SparkConf()
            .setAppName("TransformationOperation")
            .setMaster("local")
            
      val sc = new SparkContext(conf)
      
      val nameList = Array(Tuple2(1,"Tom"),
				Tuple2(2, "Jon"),
				Tuple2(2, "Jon2"),
				Tuple2(3, "alin"),
				Tuple2(4, "yiming"))
      
      val scoreList = Array(Tuple2(1, 20),
			  Tuple2(2, 40),
				Tuple2(2, 44),
				Tuple2(3, 80),
				Tuple2(4, 100))
				
			val nameRdd = sc.parallelize(nameList, 1)	
			val scoreRdd = sc.parallelize(scoreList, 1)
			
			
			val nameScore = nameRdd.cogroup(scoreRdd).sortByKey(true, 1)
			
			
			nameScore.foreach(v => {
			  
			  println("ID :"+v._1)
			  println("name : "+v._2._1)
			  println("score : "+v._2._2)
			  
			})
      
    }
    
    def CountBykeyOperation(){
      
      val conf  = new SparkConf()
              .setAppName("CountBykeyOperation")
              .setMaster("local")
              
      val sc = new SparkContext(conf)
      
      
      val dataList = Array(Tuple2("class01", 90),
                				Tuple2("class01", 50),
                				Tuple2("class02", 44),
                				Tuple2("class02", 99),
                				Tuple2("class03", 90),
                				Tuple2("class01", 90))
     val dataRdd = sc.parallelize(dataList, 1)
     
     val valueMap = dataRdd.countByKey()
     
     for (v <- valueMap){
       
       println("classname : "+v._1)
       
       println("count: "+v._2)
     }
     
    }  
}