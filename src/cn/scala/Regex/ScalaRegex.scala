package cn.scala.Regex

import scala.util.matching.Regex

object ScalaRegex {
  
  def main(args: Array[String]): Unit = {
    
    //例一、这个字符串匹配
    println("51".matches("""\d+"""))
    
    //例二、查询是否包含复合正则的模式
    val num = """\d+""".r.findAllIn("asd3424df223sd")
    
    num.foreach { x => println(x) }
    
    println("""\d+""".r.findAllIn("asd3424dfsd").length)
    
    //例子三返回第一个匹配正则的字符串
    val regex01 = """\d+""".r
    val str01 = "asd34asd45"
    println(regex01.findFirstIn(str01).get)
    
    //例子四迭代所有匹配到的复合模式的字符串
    regex01.findAllMatchIn(str01).foreach(println)
    
    //例子五返回所有正则匹配作为一个List
    println(regex01.findAllMatchIn(str01).toList)
    
    //例子六使用正则查询和替换
    var letters="""[a-zA-Z]+""".r    
    var str2="foo123bar"
    println(letters.replaceAllIn(str2,"spark"))//spark123spark
    
    //例子七使用正则查询和替换使用一个函数
    println(letters.replaceAllIn(str2,m=>m.toString().toUpperCase()))//FOO 123 BAR 456
    
    
    //?????????????????????????????????????????????????????????????????????????????????
   /* //例子八使用正则查询替换字符
    var exp="""##(\d+)##""".r    
    var str8="foo##123##bar"
    //group 0 返回所有捕获，其他1...n分别返回第一个捕获，或第二个捕获，至第n个捕获
    println(exp.replaceAllIn(str8,m=>(m.group(0)).toString))//foo##123##bar
    println(exp.replaceAllIn(str8,m=>(m.group(0).toInt*2).toString()))//foo246bar
*/   
    
     //例子九使用正则提取值进入一个变量里面
    val pattern="""(\d{4})-([0-9]{2})""".r    
    val myString="2016-02"
    val pattern(year,month)=myString
    println(year)//2016
    println(month)//02
    
    //例子十在case match匹配中使用 正则
    val dataNoDay="2016-08"
    val dateWithDay="2016-08-20"

    val yearAndMonth = """(\d{4})-([01][0-9])""".r    
    val yearMonthAndDay = """(\d{4})-([01][0-9])-([012][0-9])""".r

    dataNoDay match{   
   
       case yearAndMonth(year,month) => println("no day provided")        
       case yearMonthAndDay(year,month,day) => println("day provided: it is $day")
   }    //day provided: it is 20
   
    
    //例子十一正则匹配忽略大小写

    val caseSensitivePattern = """foo\d+"""
    println("Foo123".matches(caseSensitivePattern))//false


    val caseInsensitivePattern = """(?i)foo\d+"""
    println("Foo123".matches(caseInsensitivePattern))//true

    //注意使用正则字符串三个双引号，不需要转义
    
    process2("pig:2.0")
    
  }
  
  def process2(input:String) = {  
    val MatchStock = """^(.+):(\d+\.?\d*)""".r  
    input match {  
      case MatchStock(stock, price) => printf("stock=%s,price=%f\n", stock,price.toFloat)   
      case _ => println("invalid input " + input)  
    }  
}     
  
  def process(input:String) = {  
       val MatchStock = """(\d+)""".r  
       input match {  
            case MatchStock(stock) => printf("stock=%s,price=%f\n", stock,2.0)   
            case _ => println("invalid input " + input)  
        }  
  }     
  
}