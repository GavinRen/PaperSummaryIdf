package papersummary

import com.hankcs.hanlp.HanLP
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.util.List

import scala.collection.JavaConverters._
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by 任贵福 on 2016/8/22.
  */
object Idf {
  def segmentDF(dataFrame: DataFrame, text: String, newfields: String): DataFrame = {
    def segment(line: String): String = {
      val wordSegment = HanLP.segment(line).asScala.map(term => term.word).toArray.mkString("\t")
      wordSegment
    }
    val segementFunc = udf(segment _)
    dataFrame.withColumn(newfields, segementFunc(col(text)))
  }
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("PaperSummaryIdf").getOrCreate()
    val csvDF = spark.read.option("sep", "|").csv("/home/renguifu/data/170wpaper_summary_utf-8.csv")
    val paperSummaryDF = csvDF.select("_c0", "_c1", "_c2", "_c4").toDF("ID", "Category", "Date", "PaperSummary").filter(row=>row.get(3)!=null)
    val paperSummarySegmentDF = segmentDF(paperSummaryDF, "PaperSummary", "PaperSummarySegment")
    paperSummarySegmentDF.printSchema()
    paperSummarySegmentDF.show(10)
    paperSummarySegmentDF.write.csv("/home/renguifu/data/PaperSummarySegment")
    spark.stop()
  }

}
