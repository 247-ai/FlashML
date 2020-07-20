package com.tfs.flashml.core.metrics

import java.text.DecimalFormat

import com.tfs.flashml.core.DirectoryCreator
import com.tfs.flashml.util.ConfigValues.DataSetType
import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigValues, FlashMLConfig}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Class to compute custom metrics on a dataframe containing web journey data. The metrics computed are: probability
 * threshold,
 * time-on-page (TOP) threshold, hotlead and hotlead rate, capture percentage and capture rate etc.
 *
 * @since 1/31/17
 */
object WebCustomMetricsEvaluator
{

  private val log = LoggerFactory.getLogger(getClass)

  private val posProbUDF = udf((a: DenseVector) => a(1))
  private val primaryKey = FlashMLConfig.getStringArray(FlashMLConstants.PRIMARY_KEY)
  private val responseVariable = ConfigValues.getIndexedResponseColumn
  private val pageVariable = FlashMLConfig.getString(FlashMLConstants.PAGE_VARIABLE)
  private val customMetricsType = FlashMLConfig.getString(FlashMLConstants.CUSTOM_METRICS_TYPE)
  private val topVariable = FlashMLConfig.getString(FlashMLConstants.TOP_VARIABLE)
  private val basePath = DirectoryCreator.getBasePath

  case class CustomMetrics(dataset: String, pageNo: Int, accuracy: Double, bestF2: Double, probThreshold: Double,
                           topThreshold: Double, hotLead: Long, totalVisitor: Long, hotleadRate: Double,
                           capturedPositiveClass: Long, totalPositiveClass: Long, captureRate: Double)

  private def getF2Score(df: DataFrame): (Double, Double, Double) =
  {
    val ss = SparkSession.builder().getOrCreate()
    import ss.implicits._
    val responseVariable = ConfigValues.getIndexedResponseColumn
    val predictDF = df.withColumn("positive_probability", posProbUDF(col("probability"))).select("positive_probability", responseVariable)
    val predictionAndLabelsRdd = predictDF.as[(Double, Double)].rdd
    val metrics = new BinaryClassificationMetrics(predictionAndLabelsRdd)
    val beta = 2
    val fScore = metrics.fMeasureByThreshold(beta).toDF("thresh", "F2")
    val precision = metrics.precisionByThreshold().toDF("thresh", "Precision")
    val recall = metrics.recallByThreshold().toDF("thresh", "Recall")
    val fprScore = fScore.join(recall, "thresh").join(precision, "thresh")
    val bestF2 = fprScore.orderBy($"F2".desc).limit(1).take(1)
    val positiveClassRatio = predictionAndLabelsRdd.filter(x => x._2 == 1).count().toDouble / predictionAndLabelsRdd.count().toDouble
    val recallAtBestF2 = bestF2(0).apply(2).asInstanceOf[Double]
    val precisionAtBestF2 = bestF2(0).apply(3).asInstanceOf[Double]
    //    log.info("Threshold: " + bestF2(0).apply(0).asInstanceOf[Double])
    //    log.info("Accuracy: " + (positiveClassRatio * (2 * recallAtBestF2 - recallAtBestF2 / precisionAtBestF2)
    //    + (1 - positiveClassRatio)))
    //    log.info("BestF2score: " + bestF2(0).apply(1).asInstanceOf[Double])
    //    log.info("")
    (bestF2(0).apply(0).asInstanceOf[Double],
      positiveClassRatio * (2 * recallAtBestF2 - recallAtBestF2 / precisionAtBestF2) + (1 -
        positiveClassRatio),
      bestF2(0).apply(1).asInstanceOf[Double]
    )
  }

  def customMetricsSimulation(dfArray: Array[DataFrame], step: String): Unit =
  {

    val ss = SparkSession.builder().getOrCreate()

    /**
     * Getting the thresholds specified in the config
     */
    val (configProbThresholds, configTopThresholds, fileName) =
      (FlashMLConfig.getDoubleArray(FlashMLConstants.CUSTOM_THRESHOLDS), FlashMLConfig.getIntArray
      (FlashMLConstants.TOP_THRESHOLDS), "custom")
    val colNames = (primaryKey ++ Array(pageVariable, responseVariable, "positive_probability", "visitors",
      "isHotLead")).distinct
    val nPages = FlashMLConfig.getInt(FlashMLConstants.N_PAGES)

    /**
     * This is the threshold that is used in deciding hotlead visitors.
     * We should not have it as empty, thus we have a condition check and update the object accordingly
     */
    val topThresholds = 1.to(nPages).map(i =>
    {
      if (configTopThresholds.isEmpty || customMetricsType.equals(FlashMLConstants.PROB_ONLY_CUSTOM_METRICS)) 0
      else
        configTopThresholds(i - 1).toDouble
    })

    MetricsEvaluator.csvMetrics ++= "Custom Metrics \n"
    val pageValues = dfArray(0).select(pageVariable).distinct.orderBy(asc(pageVariable)).take(nPages).map(x => x
      .getInt(0))

    val probThresholds = 1.to(nPages).map(i =>
    {
      if (configProbThresholds.isEmpty) getF2Score(dfArray(0).filter(pageVariable + " = " + pageValues(i - 1)))
      else
        (configProbThresholds(i - 1).toDouble, 0, 0)
    })

    /**
     * UDF function which decides whether a particular visitor is hotlead or not
     * It takes three input: pageno,probability and top; this function will check these values with user provided
     * values in config
     */
    val hotleadCheck = udf((index: Int, prob: Double, top: Double) =>
    {
      val page = if (index > nPages) nPages - 1
      else index - 1
      if (prob >= probThresholds(page)._1 && top >= topThresholds(page)) 1
      else 0
    })

    /**
     * The customMetrics metrics object will hold the page level metrics whihc is computed without joins.
     */
    val customMetrics = dfArray
      .zipWithIndex
      .flatMap
      { case (df, i) =>
        log.info("pageCount\t Accuracy \t BestF2\t probThreshold\t topThreshold\t hotlead\t " +
          "totalVisitor\t hotleadRate\t capturedPositiveClass\t totalPositiveClass\t captureRate")

        val predictDF = df.withColumn("positive_probability", posProbUDF(col("probability")))
          .withColumn("visitors", concat(primaryKey.map(col): _*))
          .withColumn("isHotLead", hotleadCheck(col(pageVariable), col("positive_probability"), if
          (!customMetricsType.equals(FlashMLConstants.PROB_ONLY_CUSTOM_METRICS)) col(topVariable)
          else lit(0)))
          .select(colNames.map(col): _*)
          .cache()

        val totalVisitors = predictDF.select("visitors").distinct().count()
        val totalPositiveClass = predictDF
          .filter(responseVariable + "=1")
          .select("visitors")
          .distinct()
          .count()

        var customMetricsDF = predictDF
          .filter("isHotLead=1")
          .groupBy("visitors")
          .agg(col("visitors"), min(col(pageVariable)) as "hotleadPage", max(col(responseVariable))
            as "truePositive")

        predictDF.unpersist()

        var hotlead = customMetricsDF
          .groupBy("hotleadPage")
          .agg(count("visitors") as "hotLeadcount", sum("truePositive") as "capturedPositiveClass")
        var hotLeadCount = ArrayBuffer.fill(nPages)(0.toDouble)
        var capturedPositiveClass = ArrayBuffer.fill(nPages)(0.toDouble)
        var curPageNo = 0

        /**
         * The score till npages will be printed as such but after that it gets accumulated to the nth
         * page score
         */
        hotlead
          .sort(asc("hotLeadPage"))
          .collect
          .foreach(r =>
          {
            curPageNo = r.get(0).toString.toInt
            if (curPageNo <= nPages)
            {
              hotLeadCount(curPageNo - 1) = r.get(1).toString.toDouble
              capturedPositiveClass(curPageNo - 1) = r.get(2).toString.toDouble
            }
            else
            {
              hotLeadCount(nPages - 1) = hotLeadCount(nPages - 1) + r.get(1).toString.toDouble
              capturedPositiveClass(nPages - 1) = capturedPositiveClass(nPages - 1) + r.get(2)
                .toString
                .toDouble
            }
          })
        val decimalFormat = new DecimalFormat("#.####")
        val dataSet = DataSetType(i).toString

        0.until(nPages).map
        { i =>
          log.info((i + 1) + "\t\t" +
            decimalFormat.format(probThresholds(i)._2) + "\t\t" +
            decimalFormat.format(probThresholds(i)._3) + "\t\t" +
            decimalFormat.format(probThresholds(i)._1) + "\t\t" +
            decimalFormat.format(topThresholds(i)) + "\t\t" +
            hotLeadCount(i) + "\t\t" +
            totalVisitors + "\t\t\t\t" +
            decimalFormat.format(hotLeadCount(i) / totalVisitors.toDouble) + "\t\t\t" +
            capturedPositiveClass(i) + "\t\t\t\t\t" +
            totalPositiveClass + "\t\t\t\t\t" +
            decimalFormat.format(capturedPositiveClass(i) / totalPositiveClass.toDouble))
          mutable.LinkedHashMap("dataset" -> dataSet, "pageNo" -> (i + 1), "accuracy" -> probThresholds(i)._2, "bestF2" -> probThresholds(i)._3, "probThreshold" -> probThresholds(i)._1, "topThreshold" -> topThresholds(i), "hotLead" -> hotLeadCount(i).toInt, "totalVisitor" -> totalVisitors, "hotleadRate" -> hotLeadCount(i) / totalVisitors.toDouble, "capturedPositiveClass" -> capturedPositiveClass(i).toInt, "totalPositiveClass" -> totalPositiveClass, "captureRate" -> capturedPositiveClass(i) / totalPositiveClass.toDouble)
        }
      }
    MetricsEvaluator.csvMetrics ++= customMetrics(0).keys.toArray.mkString(",") + "\n" + customMetrics.foldLeft("")((acc, x) => acc + x.values.toArray.mkString(",") + "\n")
    MetricsEvaluator.csvMetrics ++= "\n\n"
    MetricsEvaluator.metricsMap += (s"CustomMetrics" -> customMetrics)
  }
}
