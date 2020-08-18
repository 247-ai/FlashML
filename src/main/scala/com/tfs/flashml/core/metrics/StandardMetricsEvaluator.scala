package com.tfs.flashml.core.metrics

import java.text.DecimalFormat

import com.tfs.flashml.core.DirectoryCreator
import com.tfs.flashml.core.modeltraining.ModelTrainingEngine
import com.tfs.flashml.util.ConfigValues.DataSetType
import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigValues, FlashMLConfig}
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
    private val columnsNames =
    {
        if (ConfigValues.isUplift)
            ConfigValues.primaryKeyColumns ++ Array(ConfigValues.getIndexedResponseColumn, "modelPrediction",
                "modelProbability", ConfigValues.responseColumn)
        else
            ConfigValues.primaryKeyColumns ++ Array(ConfigValues.getIndexedResponseColumn,
                if (ConfigValues.isProbabilityColGenerated) "probability" else "",
                "prediction", ConfigValues.responseColumn)
    }
    .filter(_.nonEmpty)
    .distinct

    private val posProbUDF = udf((a: DenseVector) => a(1))

    case class StdMetricsSingleIntent(dataset: String, AUROC: Double, f2Score: Double, accuracy: Double)

    case class StdMetricsMultipleIntent(dataset: String, accuracy: Double, weightedPrecision: Double,
                                        weightedRecall: Double, weightedfScore: Double, weightedTruePostive: Double,
                                        weightedFalsePositive: Double)

    case class StdMetricsSingleIntentPageLevel(dataset: String, pageNo: Int, AUROC: Double, f2Score: Double,
                                               accuracy: Double)

    case class StdMetricsMultipleIntentPageLevel(dataset: String, pageNo: Int, accuracy: Double,
                                                 weightedPrecision: Double, weightedRecall: Double,
                                                 weightedfScore: Double, weightedTruePostive: Double,
                                                 weightedFalsePositive: Double)

    def generateMetrics(predictionArray: Array[DataFrame]): Unit =
    {
        log.info("Standard Metrics:")
        val ss = SparkSession.builder().getOrCreate()
        import ss.implicits._

        val decimalFormat = new DecimalFormat("#.####")

        // Only required columns for Std Metrics computation are selected, and cached
        // The DFs are unpersisted just before exiting current function
        val filteredPredArray = predictionArray.map(_.select(columnsNames.map(col): _*))

        filteredPredArray.map(_.cache())
        MetricsEvaluator.csvMetrics ++= "Standard Metrics\n"

        if (ConfigValues.isSingleIntent)
        {
            if (ConfigValues.isPageLevelModel)
            {
                val standardSinglePageLevel = new ListBuffer[mutable.LinkedHashMap[String, Any]]()
                for (x <- filteredPredArray.indices)
                {
                    val pageNumber = x % ConfigValues.numPages + 1
                    val metrics = if (ConfigValues.isUplift)
                    {

                        new BinaryClassificationMetrics(filteredPredArray(x)
                                .withColumn("positive_probability", posProbUDF(col("modelProbability")))
                                .select("positive_probability", ConfigValues.getIndexedResponseColumn)
                                .as[(Double, Double)].rdd)
                    }
                    else
                    {
                        new BinaryClassificationMetrics(filteredPredArray(x)
                                .withColumn("positive_probability", posProbUDF(col("probability")))
                                .select("positive_probability", ConfigValues.getIndexedResponseColumn)
                                .as[(Double, Double)].rdd)
                    }
                    val mcMetrics = if (ConfigValues.isUplift)
                    {
                        new MulticlassMetrics(filteredPredArray(x)
                                .select("modelPrediction", ConfigValues.getIndexedResponseColumn)
                                .as[(Double, Double)]
                                .rdd)
                    }
                    else
                    {
                        new MulticlassMetrics(filteredPredArray(x)
                                .select("prediction", ConfigValues.getIndexedResponseColumn)
                                .as[(Double, Double)]
                                .rdd)
                    }
                    val beta = 2
                    val fScore = metrics.fMeasureByThreshold(beta).toDF("thresh", "F2")

                    val maxfScore = fScore
                            .agg(max(fScore.col("F2")))
                            .head()
                            .getDouble(0)

                    val dfType = if (x < ConfigValues.numPages) "Train" else "Test"

                    standardSinglePageLevel += mutable.LinkedHashMap(
                        "dataset" -> dfType,
                        "pageNo" -> pageNumber,
                        "AUROC" -> metrics.areaUnderROC(),
                        "f2Score" -> maxfScore,
                        "accuracy" -> mcMetrics.accuracy)

                    log.info(s"Page$pageNumber AUROC: ${decimalFormat.format(metrics.areaUnderROC())} " +
                            s"F2Score: ${decimalFormat.format(maxfScore)}")

                }
                MetricsEvaluator.csvMetrics ++= standardSinglePageLevel.toList(0).keys.toArray.mkString(",") + "\n" + standardSinglePageLevel.toList.foldLeft("")((acc, x) => acc + x.values.toArray.mkString(",") + "\n")
                // Storing Metrics as JSON
                MetricsEvaluator.metricsMap += ("StandardMetrics" -> standardSinglePageLevel.toList)
            }
            else
            {
                // Non Page Level Model
                val standardSingle = new ListBuffer[mutable.LinkedHashMap[String, Any]]()
                for (x <- filteredPredArray.indices)
                {
                    // For calculating the precision, recall and a few other metrics, we will use the inbuilt MulticlassMetrics class.
                    // Similar to what we have in CrossValidatorCustom.
                    val mcMetrics = if (ConfigValues.isUplift)
                    {
                        new MulticlassMetrics(filteredPredArray(x)
                                .select("modelPrediction", ConfigValues.getIndexedResponseColumn)
                                .as[(Double, Double)].rdd)
                    }
                    else
                    {
                        new MulticlassMetrics(filteredPredArray(x)
                                .select("prediction", ConfigValues.getIndexedResponseColumn)
                                .as[(Double, Double)].rdd)
                    }

                    // Also calculate the F2 score
                    val beta = 2

                    val dataSetType = DataSetType(x)

                    standardSingle += mutable.LinkedHashMap(
                        "dataset" -> dataSetType.toString,
                        "accuracy" -> mcMetrics.accuracy,
                        "precision" -> mcMetrics.weightedPrecision,
                        "recall" -> mcMetrics.weightedRecall,
                        "F1Score" -> mcMetrics.weightedFMeasure,
                        "F2Score" -> mcMetrics.weightedFMeasure(beta),
                    )
                    // Log model performance stats
                    log.info(s"\n$dataSetType:\n\t" +
                            s"Accuracy: ${decimalFormat.format(mcMetrics.accuracy)}, " +
                            s"Precision: ${decimalFormat.format(mcMetrics.weightedPrecision)}, " +
                            s"Recall: ${decimalFormat.format(mcMetrics.weightedRecall)}, " +
                            s"F1 Score: ${decimalFormat.format(mcMetrics.weightedFMeasure)}, " +
                            s"F2 Score: ${decimalFormat.format(mcMetrics.weightedFMeasure(beta))}")
                }
                MetricsEvaluator.csvMetrics ++= standardSingle.toList(0).keys.toArray.mkString(",") + "\n" + standardSingle.toList.foldLeft("")((acc, x) => acc + x.values.toArray.mkString(",") + "\n")
                MetricsEvaluator.metricsMap += ("StandardMetrics" -> standardSingle.toList)
            }
        }
        else if (ConfigValues.isMultiIntent)
        {
            if (ConfigValues.isPageLevelModel)
            {
                val standardMultiplePageLevel = new ListBuffer[mutable.LinkedHashMap[String, Any]]()

                //Loading the labels across all the pages from the StringIndexerModel
                val responseVariableLabels: Array[Array[String]] = ModelTrainingEngine.loadPipelineArray.map(_.stages(0).asInstanceOf[StringIndexerModel].labels)

                //Holder for Map between Index and Label across all pages
                val responseVariableMapArray = new Array[Map[Double, String]](ConfigValues.numPages)
                for (x <- filteredPredArray.indices)
                {
                    val pageNumber = x % ConfigValues.numPages + 1

                    // Populate the Holder if not already done
                    if (!responseVariableMapArray.isDefinedAt(x % ConfigValues.numPages))
                    {
                        responseVariableMapArray(x % ConfigValues.numPages) = ((for (i <- responseVariableLabels(x % ConfigValues.numPages).indices) yield i.toDouble) zip responseVariableLabels(x % ConfigValues.numPages)).toMap
                    }

                    if (!ConfigValues.isUplift)
                    {
                        val metrics = new MulticlassMetrics(filteredPredArray(x)
                                .select("prediction", ConfigValues.getIndexedResponseColumn)
                                .as[(Double, Double)].rdd)

                        log.info(s"Page$pageNumber Accuracy: ${decimalFormat.format(metrics.accuracy)} Weighted " +
                                s"precision: ${decimalFormat.format(metrics.weightedPrecision)} " +
                                s"Weighted recall: ${decimalFormat.format(metrics.weightedRecall)} Weighted True " +
                                s"Positive Rate: ${decimalFormat.format(metrics.weightedTruePositiveRate)} " +
                                s"Weighte False Positive Rate: ${decimalFormat.format(metrics.weightedFalsePositiveRate)}")

                        val dfType = if (x < ConfigValues.numPages) "Train" else "Test"

                        standardMultiplePageLevel += mutable.LinkedHashMap(
                            "dataset" -> dfType,
                            "pageNo" -> pageNumber,
                            "accuracy" -> metrics.accuracy,
                            "weightedPrecision" -> metrics.weightedPrecision,
                            "weightedRecall" -> metrics.weightedRecall,
                            "weightedF1Score" -> metrics.weightedFMeasure,
                            "weightedTruePostive" -> metrics.weightedTruePositiveRate,
                            "weightedFalsePositive" -> metrics.weightedFalsePositiveRate
                        )

                        val predictionCount = filteredPredArray(x)
                                .groupBy(ConfigValues.getIndexedResponseColumn)
                                .agg(sum(col(ConfigValues.getIndexedResponseColumn)))
                                .as[(Double, Double)]
                                .map(x => (x._1, x._2))
                                .collect
                                .toMap

                        val labels = metrics.labels
                        val path = DirectoryCreator.getConfusionMetricsPath()

                        val confusionPath = new Path(path, s"page$pageNumber$dfType")
                        val metricsArray = ArrayBuffer[String]()
                        metricsArray += "Intent\t\tTrue Positive\t\tFalse Positive\t\tPrecision\t\tRecall"

                        labels.foreach(label =>
                        {
                            metricsArray += responseVariableMapArray(x % ConfigValues.numPages) + "\t\t" +
                                    (metrics.truePositiveRate(label) * predictionCount(label)).toInt.toString + "\t\t" +
                                    (metrics.falsePositiveRate(label) * predictionCount(label)).toInt.toString + "\t\t" +
                                    metrics.precision(label).toInt.toString + "\t\t" +
                                    metrics.recall(label).toInt.toString
                        })

                        // Write out the confusion metrics
                        val pathToSave = s"${FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI)}/${confusionPath.toString}"
                        ss
                            .sparkContext
                            .parallelize(metricsArray.toSeq, 1)
                            .toDF()
                            .write
                            .mode(SaveMode.Overwrite)
                            .text(pathToSave)
                        log.info(s"Confusion metrics saved at: [$pathToSave]")
                    }
                }
                MetricsEvaluator.csvMetrics ++= standardMultiplePageLevel.toList(0).keys.toArray.mkString(",") + "\n" + standardMultiplePageLevel.toList.foldLeft("")((acc, x) => acc + x.values.toArray.mkString(",") + "\n")
                MetricsEvaluator.metricsMap += ("StandardMetrics" -> standardMultiplePageLevel.toList)
            }
            else
            {
                // Non Page Level Model
                val standardMultiple = new ListBuffer[mutable.LinkedHashMap[String, Any]]()

                val responseVariableLabels = ModelTrainingEngine.loadPipelineModel(0).stages(0).asInstanceOf[StringIndexerModel].labels
                val responseVariableMap = ((for (i <- responseVariableLabels.indices) yield i.toDouble) zip responseVariableLabels).toMap

                for (x <- filteredPredArray.indices)
                {
                    if (!ConfigValues.isUplift)
                    {
                        // Obtain the multi-class metrics for this dataset from the underlying RDD object.
                        val metrics = new MulticlassMetrics(filteredPredArray(x)
                                .select("prediction", ConfigValues.getIndexedResponseColumn)
                                .as[(Double, Double)].rdd)

                        val dataSetType = DataSetType(x)

                        // Collect all the metrics data
                        standardMultiple += mutable.LinkedHashMap(
                            "dataset" -> dataSetType.toString,
                            "accuracy" -> metrics.accuracy,
                            "weightedPrecision" -> metrics.weightedPrecision,
                            "weightedRecall" -> metrics.weightedRecall,
                            "weightedF1Score" -> metrics.weightedFMeasure,
                            "weightedTruePostive" -> metrics.weightedTruePositiveRate,
                            "weightedFalsePositive" -> metrics.weightedFalsePositiveRate
                        )

                        // Calculate the intent level confusion metrics
                        val intentLevelMetrics = filteredPredArray(x)
                                .groupBy(ConfigValues.getIndexedResponseColumn)
                                .agg(count(col(ConfigValues.getIndexedResponseColumn)))
                                .as[(Double, Double)]
                                .map(x => (x._1, x._2))
                                .collect
                                .toMap

                        val labels = metrics.labels
                        val path = DirectoryCreator.getConfusionMetricsPath()

                        val confusionPath = new Path(path, s"noPage$dataSetType")
                        val metricsArray = ArrayBuffer[String]()
                        metricsArray += "Intent\t\tTrue Positive\t\tFalse Positive\t\tPrecision\t\tRecall"

                        labels.foreach(label =>
                        {
                            metricsArray += responseVariableMap(label) + "\t\t" +
                                    (metrics.truePositiveRate(label) * intentLevelMetrics(label)).toInt.toString + "\t\t" +
                                    (metrics.falsePositiveRate(label) * intentLevelMetrics(label)).toInt.toString + "\t\t" +
                                    metrics.precision(label).toString + "\t\t" +
                                    metrics.recall(label).toString
                        })

                        // We do not want to print the class-level confusion metrics on screen, since that can be a
                        // very long list. Write the metrics to a file
                        ss
                            .sparkContext
                            .parallelize(metricsArray, 1)
                            .toDF()
                            .write
                            .mode(SaveMode.Overwrite)
                            .text(FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + confusionPath
                                    .toString)

                        // Model performance Stats logged
                        log.info(s"\n$dataSetType:\n\t" +
                                s"Accuracy: ${decimalFormat.format(metrics.accuracy)}, " +
                                s"Precision: ${decimalFormat.format(metrics.weightedPrecision)}, " +
                                s"Recall: ${decimalFormat.format(metrics.weightedRecall)}, " +
                                s"F1 Score: ${decimalFormat.format(metrics.weightedFMeasure)}, " +
                                s"F2 Score: ${decimalFormat.format(metrics.weightedFMeasure(2))}")

                        // Intent Level Stats (confusion metrics) have been saved at Proj Directory on HDFS
                        // Logging the saved location
                        log.info(s"Confusion metrics stored at [${FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI)}/${confusionPath.toString}]")

                    }
                    else throw new UnsupportedOperationException("Computation of metrics for multi-intent uplift models is not supported.")
                }
                MetricsEvaluator.csvMetrics ++= standardMultiple.toList(0).keys.toArray.mkString(",") + "\n" + standardMultiple.toList.foldLeft("")((acc, x) => acc + x.values.toArray.mkString(",") + "\n")
                MetricsEvaluator.metricsMap += ("StandardMetrics" -> standardMultiple.toList)
            }
        }
        MetricsEvaluator.csvMetrics ++= "\n\n"
        filteredPredArray.map(_.unpersist())
    }
}
