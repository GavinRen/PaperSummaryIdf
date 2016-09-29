package weibo

import com.hankcs.hanlp.HanLP
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
/**
  * Created by 任贵福 on 2016/8/30.
  */
object WeiboClassify {
//  对微博内容进行分词
  def segmentDF(dataFrame: DataFrame, text: String, newfields: String): DataFrame = {
    def segment(line: String): Array[String] = {
      if (line==null){
        return null
      }
      val wordSegment = HanLP.segment(line).asScala.map(term => term.word).toArray
      wordSegment
    }
    val segementFunc = udf(segment _)
    dataFrame.withColumn(newfields, segementFunc(col(text)))
  }
//  对微博内容进行分词后的结果提取TF-IDF值

  case class WeiboTrainingSample(weiboID:String,weibocontent:String,isLable:String,weiboLable:Double)
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("WeiboClassify").getOrCreate()
    val df = spark.read.csv("/home/gavin/data/weibo/Content/")
    val list=List("msgID","userID","userName","nickName","forwardMsgID","msgContent","msgURL","from","pictureURL","audioURL","videoURL","location","forwardNum","commentNum","likeNum","timeStamp")
    val weibo=df.filter(row=>row.get(0).toString.!=("消息ID")||row!= null).filter(row=>row.length==16).toDF(list:_*)
    weibo.printSchema()
    weibo.show(5)
    weibo.repartition(1).write.json("/home/gavin/data/weibo/weibojson")
    weibo.select("msgContent").show()
    weibo.select("msgContent").filter(row=>row(0).!=("")).rdd.repartition(1).saveAsTextFile("/home/gavin/data/weibo/msgcontent")
    val weiboSegment=segmentDF(weibo,"msgContent","segWords").filter(row=> !row.isNullAt(16))
    val hashingTF=new HashingTF().setInputCol("segWords").setOutputCol("wordTF").setNumFeatures(10000)
    val weiboTF=hashingTF.transform(weiboSegment)
    val idf=new IDF().setInputCol("wordTF").setOutputCol("features")
    val idfModel=idf.fit(weiboTF)
    val weiboTFIDF=idfModel.transform(weiboTF)
    import spark.implicits._

    val weiboRdd=spark.sparkContext.textFile("/home/gavin/data/weibo_classify/")

    val weiboTrainingRdd=weiboRdd.map(_.split("\t")).filter(row=>row.length==4).map(row=>WeiboTrainingSample(row(0),row(1),row(2),row(3).trim().toDouble))
    val weiboTraining=weiboTrainingRdd.toDF()
    val weiboTrainingSegement=segmentDF(weiboTraining,"weibocontent","segwords")
    val weibohashingTF=new HashingTF().setInputCol("segwords").setOutputCol("features").setNumFeatures(10000)
    val weiboTrainingTF=weibohashingTF.transform(weiboTrainingSegement)
    val nativeBayes=new NaiveBayes().setFeaturesCol("features").setLabelCol("weiboLable").fit(weiboTrainingTF)
    val weiboClassifyLable=nativeBayes.transform(weiboTFIDF)
    weiboClassifyLable.printSchema()
    weiboClassifyLable.select("msgID","msgContent","prediction")show(5)
    weiboClassifyLable.select("msgID","msgContent","prediction").repartition(1).write.csv("/home/gavin/data/weiboLable")
    spark.stop()
  }
}