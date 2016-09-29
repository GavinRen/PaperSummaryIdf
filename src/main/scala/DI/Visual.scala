package DI

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by 任贵福 on 2016/8/30.
  */
object Visual {
    def basicVisual(df:DataFrame,x:String,y:List[String],method:String):List[List[Any]]={
      val xmax=df.agg(Map(x->"max")).head().get(0).toString.toDouble
      val xmin=df.agg(Map(x->"min")).head().get(0).toString.toDouble
      def normalize(xStr:Any)={
        val x=xStr.toString.toDouble
        val xx=Math.round((x-xmin)/(xmax-xmin)*50).toInt
        xx
      }
      import org.apache.spark.sql.functions._
      val normalizeFunc=udf(normalize _)
      val map =y.map(ele=>Map(ele->method)).reduce(_++_)
      val res=df.select(x,y:_*).groupBy(x).agg(map)
      if (res.count()<=50){
        val columns=res.columns
        return res.rdd.map(row=>List(row.get(0).toString)++List(columns.tail.map(name=>row.get(row.fieldIndex(name)).toString.toDouble):_*)).collect().toList
      }else{
        val res1=df.select(x,y:_*).withColumn("secID",normalizeFunc(col(x))).groupBy("secID").agg(map)
        val columns1=res1.columns
        res1.rdd.map(row=>List(row.get(0).toString)++List(columns1.tail.map(name=>row.get(row.fieldIndex(name)).toString.toDouble):_*)).collect().toList
      }
    }

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TestVisual").getOrCreate()

  }
}
