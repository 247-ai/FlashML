
package com.tfs.flashml.core.metrics

import java.text.DecimalFormat

import com.tfs.flashml.core.{DirectoryCreator, ModelTrainingEngine}
import com.tfs.flashml.util.ConfigUtils.DataSetType
import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigUtils, FlashMLConfig}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Class to calculate the standard metrics for a trained model for binary and multi-class classification.
  *
  * @since 12/26/16
  */

object StandardMetricsEvaluator
{

    private val log = LoggerFactory.getLogger(getClass)
    val columnsNames =
        {
            if (ConfigUtils.isUplift)
                ConfigUtils.primaryKeyColumns ++ Array(ConfigUtils.getIndexedResponseColumn, "modelPrediction",
                    "modelProbability", ConfigUtils.responseColumn)
            else
                ConfigUtils.primaryKeyColumns ++ Array(ConfigUtils.getIndexedResponseColumn,
                    if (ConfigUtils.isProbabilityColGenerated) "probability" else "",
                    "prediction", ConfigUtils.responseColumn)
        }
        .filter(_.nonEmpty)
        .distinct

    private val posProbUDF = udf((a: DenseVector) => a(1))
    private val basePath = DirectoryCreator.getBasePath

    case class stdMetricsSingleIntent(dataset: String, AUROC: Double, f2Score: Double, accuracy: Double)

    case class stdMetricsMultipleIntent(dataset: String, accuracy: Double, weightedPrecision: Double,
                                        weightedRecall: Double, weightedfScore: Double, weightedTruePostive: Double,
                                        weightedFalsePositive: Double)

    case class stdMetricsSingleIntentPageLevel(dataset: String, pageNo: Int, AUROC: Double, f2Score: Double,
                                               accuracy: Double)

    case class stdMetricsMultipleIntentPageLevel(dataset: String, pageNo: Int, accuracy: Double,
                                                 weightedPrecision: Double, weightedRecall: Double,
                                                 weightedfScore: Double, weightedTruePostive: Double,
                                                 weightedFalsePositive: Double)

    def generateMetrics(predictionArray: Array[DataFrame]): Unit =
    {
        log.info("\nStandard Metrics")
        val ss = SparkSession.builder().getOrCreate()
        import ss.implicits._

        val decimalFormat = new DecimalFormat("#.####")

        // Only required columns for Std Metrics computation are selected, and cached
        // The DFs are unpersisted just before exiting current function
        val filteredPredArray = predictionArray
                .map(_.select(columnsNames.map(col): _*))

        filteredPredArray
                .map(_.cache())
        MetricsEvaluator.csvMetrics ++= "Standard Metrics \n"

        if (ConfigUtils.isSingleIntent)
        {

            if (ConfigUtils.isPageLevelModel)
            {
                val standardSinglePageLevel = new ListBuffer[mutable.LinkedHashMap[String, Any]]()
                for (x <- filteredPredArray.indices)
                {
                    val pageNumber = x % ConfigUtils.numPages + 1
                    val metrics = if (ConfigUtils.isUplift)
                    {

                        new BinaryClassificationMetrics(filteredPredArray(x)
                                .withColumn("positive_probability", posProbUDF(col("modelProbability")))
                                .select("positive_probability", ConfigUtils.getIndexedResponseColumn)
                                .as[(Double, Double)].rdd)
                    }
                    else
                    {
                        new BinaryClassificationMetrics(filteredPredArray(x)
                                .withColumn("positive_probability", posProbUDF(col("probability")))
                                .select("positive_probability", ConfigUtils.getIndexedResponseColumn)
                                .as[(Double, Double)].rdd)
                    }
                    val myMetrics = if (ConfigUtils.isUplift)
                    {
                        new MulticlassMetrics(filteredPredArray(x)
                                .select("modelPrediction", ConfigUtils.getIndexedResponseColumn)
                                .as[(Double, Double)]
                                .rdd)
                    }
                    else
                    {
                        new MulticlassMetrics(filteredPredArray(x)
                                .select("prediction", ConfigUtils.getIndexedResponseColumn)
                                .as[(Double, Double)]
                                .rdd)
                    }
                    val beta = 2
                    val fScore = metrics.fMeasureByThreshold(beta).toDF("thresh", "F2")

                    val maxfScore = fScore
                            .agg(max(fScore.col("F2")))
                            .head()
                            .getDouble(0)

                    val dfType = if (x < ConfigUtils.numPages) "Train"
                    else "Test"

                    standardSinglePageLevel += mutable.LinkedHashMap(
                        "dataset" -> dfType,
                        "pageNo" -> pageNumber,
                        "AUROC" -> metrics.areaUnderROC(),
                        "f2Score" -> maxfScore,
                        "accuracy" -> myMetrics.accuracy)

                    log.info(s"Page$pageNumber AUROC: ${decimalFormat.format(metrics.areaUnderROC())} " +
                            s"F2Score: ${decimalFormat.format(maxfScore)}")

                }
                MetricsEvaluator.csvMetrics ++= standardSinglePageLevel.toList(0).keys.toArray.mkString(",") + "\n" + standardSinglePageLevel.toList.foldLeft("")((acc,x) => acc + x.values.toArray.mkString(",")+"\n")
                //Storing Metrics as JSON
                MetricsEvaluator.metricsMap += ("StandardMetrics" -> standardSinglePageLevel.toList)
            }
            else
            {
                val standardSingle = new ListBuffer[mutable.LinkedHashMap[String, Any]]()
                for (x <- filteredPredArray.indices)
                {

                    val metrics = if (ConfigUtils.isUplift)
                    {

                        new BinaryClassificationMetrics(filteredPredArray(x)
                                .withColumn("positive_probability", posProbUDF(col("modelProbability")))
                                .select("positive_probability", ConfigUtils.getIndexedResponseColumn)
                                .as[(Double, Double)]
                                .rdd)
                    }
                    else
                    {
                        new BinaryClassificationMetrics(filteredPredArray(x)
                                .withColumn("positive_probability", posProbUDF(col("probability")))
                                .select("positive_probability", ConfigUtils.getIndexedResponseColumn)
                                .as[(Double, Double)].rdd)
                    }
                    val myMetrics = if (ConfigUtils.isUplift)
                    {
                        new MulticlassMetrics(filteredPredArray(x)
                                .select("modelPrediction", ConfigUtils.getIndexedResponseColumn)
                                .as[(Double, Double)].rdd)
                    }
                    else
                    {
                        new MulticlassMetrics(filteredPredArray(x)
                                .select("prediction", ConfigUtils.getIndexedResponseColumn)
                                .as[(Double, Double)].rdd)
                    }

                    val beta = 2
                    val fScore = metrics.fMeasureByThreshold(beta).toDF("thresh", "F2")
                    val maxfScore = fScore.agg(max(fScore.col("F2"))).head().getDouble(0)

                    val dataset = DataSetType(x)

                    standardSingle += mutable.LinkedHashMap(
                        "dataset" -> dataset.toString,
                        "AUROC" -> metrics.areaUnderROC(),
                        "f2Score" -> maxfScore,
                        "accuracy" -> myMetrics.accuracy
                    )
                    log.info(s"AUROC: ${decimalFormat.format(metrics.areaUnderROC())} F2Score: ${decimalFormat.format(maxfScore)}")
                }
                MetricsEvaluator.csvMetrics ++= standardSingle.toList(0).keys.toArray.mkString(",") + "\n" + standardSingle.toList.foldLeft("")((acc,x) => acc + x.values.toArray.mkString(",")+"\n")
                MetricsEvaluator.metricsMap += ("StandardMetrics" -> standardSingle.toList)
            }
        }
        else if (ConfigUtils.isMultiIntent)
        {
            //log.info("\nStandard Metrics")
            if (ConfigUtils.isPageLevelModel)
            {
                val standardMultiplePageLevel = new ListBuffer[mutable.LinkedHashMap[String, Any]]()

                //Loading the labels across all the pages from the StringIndexerModel
                val responseVariableLabels: Array[Array[String]] = ModelTrainingEngine.loadPipelineArray.map(_.stages(0).asInstanceOf[StringIndexerModel].labels)

                //Holder for Map between Index and Label across all pages
                val responseVariableMapArray = new Array[Map[Double, String]](ConfigUtils.numPages)
                for (x <- filteredPredArray.indices)
                {
                    val pageNumber = x % ConfigUtils.numPages + 1

                    //Populate the Holder if not already done
                    if(!responseVariableMapArray.isDefinedAt(x % ConfigUtils.numPages)) {
                      responseVariableMapArray(x % ConfigUtils.numPages) = ( (for(i <- responseVariableLabels(x % ConfigUtils.numPages).indices) yield i.toDouble) zip responseVariableLabels(x % ConfigUtils.numPages)).toMap
                    }

                    if (!ConfigUtils.isUplift)
                    {

                        val metrics = new MulticlassMetrics(filteredPredArray(x)
                                .select("prediction", ConfigUtils.getIndexedResponseColumn)
                                .as[(Double, Double)].rdd)

                        log.info(s"Page$pageNumber Accuracy: ${decimalFormat.format(metrics.accuracy)} Weighted " +
                                s"precision: ${decimalFormat.format(metrics.weightedPrecision)} " +
                                s"Weighted recall: ${decimalFormat.format(metrics.weightedRecall)} Weighted True " +
                                s"Positive Rate: ${decimalFormat.format(metrics.weightedTruePositiveRate)} " +
                                s"Weighte False Positive Rate: ${
                                    decimalFormat.format(metrics
                                            .weightedFalsePositiveRate)
                                }")

                        val dfType = if (x < ConfigUtils.numPages) "Train"
                        else "Test"

                        standardMultiplePageLevel += mutable.LinkedHashMap(
                            "dataset" -> dfType,
                            "pageNo" -> pageNumber,
                            "accuracy" -> metrics.accuracy,
                            "weightedPrecision" -> metrics.weightedPrecision,
                            "weightedRecall" -> metrics.weightedRecall,
                            "weightedfScore" -> metrics.weightedFMeasure,
                            "weightedTruePostive" -> metrics.weightedTruePositiveRate,
                            "weightedFalsePositive" -> metrics.weightedFalsePositiveRate
                        )

                        val predictionCount = filteredPredArray(x)
                                .groupBy(ConfigUtils.getIndexedResponseColumn)
                                .agg(sum(col(ConfigUtils.getIndexedResponseColumn)))
                                .as[(Double, Double)]
                                .map(x => (x._1, x._2))
                                .collect
                                .toMap

                        val labels = metrics.labels
                        val path = DirectoryCreator.getConfusionMetricsPath()

                        val confusionPath = new Path(path, s"page$pageNumber$dfType")
                        val metricsArray = ArrayBuffer[String]()
                        metricsArray += "Intent\t\tTrue Positive\t\tFalse Positive\t\tPrecision\t\tRecall"

                        for (i <- labels)
                        {
                            metricsArray += responseVariableMapArray(x % ConfigUtils.numPages) + "\t\t" + (metrics.truePositiveRate(i) *
                                    predictionCount(i)).toInt.toString + "\t\t" + (metrics.falsePositiveRate(i) *
                                    predictionCount(i)).toInt.toString + "\t\t" + metrics.precision(i).toInt.toString + "\t\t" + metrics.recall(i).toInt.toString
                        }

                        SparkSession
                                .builder()
                                .getOrCreate()
                                .sparkContext
                                .parallelize(metricsArray.toSeq, 1)
                                .toDF()
                                .write
                                .mode(SaveMode.Overwrite)
                                .text(FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + confusionPath
                                        .toString)
                        log.info(s"Confusion metrics saved  [${FlashMLConfig.getString(FlashMLConstants
                                .NAME_NODE_URI)}/${confusionPath.toString}]")
                    }
                }
                MetricsEvaluator.csvMetrics ++= standardMultiplePageLevel.toList(0).keys.toArray.mkString(",") + "\n" + standardMultiplePageLevel.toList.foldLeft("")((acc,x) => acc + x.values.toArray.mkString(",")+"\n")
                MetricsEvaluator.metricsMap += ("StandardMetrics" -> standardMultiplePageLevel.toList)
            }
            else
            {
                // Non Page Level Model
                val standardMultiple = new ListBuffer[mutable.LinkedHashMap[String, Any]]()

                val responseVariableLabels = ModelTrainingEngine.loadPipelineModel(0).stages(0).asInstanceOf[StringIndexerModel].labels
                val responseVariableMap = ((for(i <- responseVariableLabels.indices) yield i.toDouble) zip responseVariableLabels).toMap

                for (x <- filteredPredArray.indices)
                {
                    if (!ConfigUtils.isUplift)
                    {
                        // Obtain the multi-class metrics for this dataset.
                        // Note: available from the underlying RDD object.
                        val metrics = new MulticlassMetrics(filteredPredArray(x)
                                .select("prediction", ConfigUtils.getIndexedResponseColumn)
                                .as[(Double, Double)].rdd)

                        val dataSetType = DataSetType(x)

                        standardMultiple += mutable.LinkedHashMap(
                            "dataset" -> dataSetType.toString,
                            "accuracy" -> metrics.accuracy,
                            "weightedPrecision" -> metrics.weightedPrecision,
                            "weightedRecall" -> metrics.weightedRecall,
                            "weightedfScore" -> metrics.weightedFMeasure,
                            "weightedTruePostive" -> metrics.weightedTruePositiveRate,
                            "weightedFalsePositive" -> metrics.weightedFalsePositiveRate
                        )

                        // Calculate the intent level confusion metrics
                        val predictionCount = filteredPredArray(x)
                                .groupBy(ConfigUtils.getIndexedResponseColumn)
                                .agg(count(col(ConfigUtils.getIndexedResponseColumn)))
                                .as[(Double, Double)]
                                .map(x => (x._1, x._2))
                                .collect
                                .toMap

                        val labels = metrics.labels
                        val path = DirectoryCreator.getConfusionMetricsPath()

                        val confusionPath = new Path(path, s"noPage$dataSetType")
                        val metricsArray = ArrayBuffer[String]()
                        metricsArray += "Intent\t\tTrue Positive\t\tFalse Positive\t\tPrecision\t\tRecall"

                        for (i <- labels)
                        {
                            metricsArray += responseVariableMap(i) + "\t\t" + (metrics.truePositiveRate(i) *
                                    predictionCount(i)).toInt.toString + "\t\t" + (metrics.falsePositiveRate(i) *
                                    predictionCount(i)).toInt.toString + "\t\t" + metrics.precision(i).toString +
                                    "\t\t" + metrics.recall(i).toString
                        }

                        // We do not want to print the class-level confusion metrics on screen, since that can be a
                        // very long list.
                        // Write the metrics to a file
                        ss
                                .sparkContext
                                .parallelize(metricsArray.toSeq, 1)
                                .toDF()
                                .write
                                .mode(SaveMode.Overwrite)
                                .text(FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + confusionPath
                                        .toString)

                        // Model performance Stats logged
                        log.info(s"${dataSetType}:\n\tAccuracy: ${metrics.accuracy}\tWeighted " +
                                s"precision: ${decimalFormat.format(metrics.weightedPrecision)}\tWeighted " +
                                s"recall: ${decimalFormat.format(metrics.weightedRecall)}\tF-Measure: ${decimalFormat
                                        .format(metrics.weightedFMeasure)}\tWeighted True Positive " +
                                s"Rate: ${decimalFormat.format(metrics.weightedTruePositiveRate)}\tWeighted False " +
                                s"Positive Rate: ${decimalFormat.format(metrics.weightedFalsePositiveRate)}")

                        // Intent Level Stats (confusion metrics) have been saved at Proj Directory on HDFS
                        // Logging the saved location
                        log.info(s"Confusion metrics stored at [${FlashMLConfig.getString(FlashMLConstants
                                .NAME_NODE_URI)}/${confusionPath.toString}]")

                    }
                    else throw new UnsupportedOperationException("Computation of metrics for multi-intent uplift " +
                            "models is not supported.")
                }
                MetricsEvaluator.csvMetrics ++= standardMultiple.toList(0).keys.toArray.mkString(",") + "\n" + standardMultiple.toList.foldLeft("")((acc,x) => acc + x.values.toArray.mkString(",")+"\n")
                MetricsEvaluator.metricsMap += ("StandardMetrics" -> standardMultiple.toList)
            }
        }
        MetricsEvaluator.csvMetrics ++= "\n\n"
        filteredPredArray
                .map(_.unpersist())
    }
}
