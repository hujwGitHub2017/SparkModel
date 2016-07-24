package cn.spark.study

class SecondarySortKey(val first:String,val second:String) extends Ordered[SecondarySortKey] with Serializable{
  
    def compare(s: SecondarySortKey): Int={
      
      if (this.first.hashCode() - s.first.hashCode() != 0) {
        
        return this.first.hashCode() - s.first.hashCode();
        
      }else {
        
        this.second.hashCode() - s.second.hashCode();
      }
      
    }
  
}