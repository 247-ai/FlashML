package org.apache.spark.ml.tuning

import java.text.DecimalFormat
import java.util.{Locale, List => JList}

import com.tfs.flashml.core.DirectoryCreator
import com.tfs.flashml.core.metrics.MetricsEvaluator
import com.tfs.flashml.util.Implicits._
import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigValues, FlashMLConfig}
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.classification.{OneVsRestCustomModel, PlattScalar}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.shared.{HasCollectSubModels, HasParallelism}
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model, Pipeline, PipelineStage}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.util.ThreadUtils
import org.json4s.DefaultFormats

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration.Duration

/**
  * Params for [[CrossValidatorCustom]] and [[CrossValidatorCustomModel]].
  */
private[ml] trait CrossValidatorCustomParams extends ValidatorParams
{
    /**
      * Param for number of folds for cross validation.  Must be &gt;= 2.
      * Default: 3
      *
      * @group param
      */
    val numFolds: IntParam = new IntParam(this, "numFolds",
        "number of folds for cross validation (>= 2)", ParamValidators.gtEq(2))

    /** @group getParam */
    def getNumFolds: Int = $(numFolds)

    setDefault(numFolds -> 3)
}

/**
  * K-fold cross validation performs model selection by splitting the dataset into a set of
  * non-overlapping randomly partitioned folds which are used as separate training and test datasets
  * e.g., with k=3 folds, K-fold cross validation will generate 3 (training, test) dataset pairs,
  * each of which uses 2/3 of the data for training and 1/3 for testing. Each fold is used as the
  * test set exactly once.
  *
  * This custom class is created because of the following changes:
  *   1. We need to print the fold level metrics
  *   2. We need to save the intermediate predictedDF of the best param set
  *   3. Perform Platt scaling for the SVM models inside cv fit
  *   4. If the estimator used is OneVsRestCustom, we directly save OneVsRestCustomModel instead of CrossValidatorModel.
  */
class CrossValidatorCustom(override val uid: String)
        extends Estimator[CrossValidatorCustomModel]
                with CrossValidatorCustomParams with HasParallelism with HasCollectSubModels
                with MLWritable with Logging
{

    private val logger = Logger.getLogger(getClass)
    logger.setLevel(Level.INFO)

    private val algorithm: String = ConfigValues.mlAlgorithm

    case class cvMetricsClass(foldNo: Int, paramMap: String, accuracy: Double, weightedPrecision: Double,
                              weightedRecall: Double)

    case class cvAvgMetricsClass(paramMap: String, accuracy: Double, weightedPrecision: Double, weightedRecall: Double)


    def this() = this(Identifiable.randomUID("cv"))

    /** @group setParam */
    def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

    /** @group setParam */
    def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

    /** @group setParam */
    def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

    /** @group setParam */
    def setNumFolds(value: Int): this.type = set(numFolds, value)

    /** @group setParam */
    def setSeed(value: Long): this.type = set(seed, value)

    /**
      * Set the maximum level of parallelism to evaluate models in parallel.
      * Default is 1 for serial evaluation
      *
      * @group expertSetParam
      */
    def setParallelism(value: Int): this.type = set(parallelism, value)

    /**
      * Whether to collect submodels when fitting. If set, we can get submodels from
      * the returned model.
      *
      * Note: If set this param, when you save the returned model, you can set an option
      * "persistSubModels" to be "true" before saving, in order to save these submodels.
      * You can check documents of
      * {@link org.apache.spark.ml.tuning.CrossValidatorCustomModel.CrossValidatorCustomModelWriter}
      * for more information.
      *
      * @group expertSetParam
      */
    def setCollectSubModels(value: Boolean): this.type = set(collectSubModels, value)

    override def fit(dataset: Dataset[_]): CrossValidatorCustomModel = instrumented
    { instr =>

        logger.info("Starting cross-validation runs.")
        val schema = dataset.schema
        transformSchema(schema, logging = true)
        val sparkSession = dataset.sparkSession
        import sparkSession.implicits._
        val est = $(estimator)
        val eval = $(evaluator)
        val epm = $(estimatorParamMaps)

        val decimalFormat = new DecimalFormat("#.####")

        // Create execution context based on $(parallelism)
        val executionContext = getExecutionContext

        instr.logParams(this, numFolds, seed, parallelism)
        logTuningParams(instr)

        val collectSubModelsParam = $(collectSubModels)

        // colsToSelect - For model building we need only primaryKey,features and indexedResponse columns
        //colsToSave - While saving the prediction DFs to HDFS we need only prediction and probability along with
        // primary key
        val colsToSelect = FlashMLConfig.getStringArray(FlashMLConstants.PRIMARY_KEY) ++ Array(FlashMLConstants.FEATURES, ConfigValues.getIndexedResponseColumn)
        val colsToSave = FlashMLConfig.getStringArray(FlashMLConstants.PRIMARY_KEY) ++ Array("prediction", "probability")

        val subModels: Option[Array[Array[Model[_]]]] = if (collectSubModelsParam)
        {
            Some(Array.fill($(numFolds))(Array.fill[Model[_]](epm.length)(null)))
        }
        else None

        // Compute metrics for each model over each split
        val splits = MLUtils.kFold(dataset.toDF.rdd, $(numFolds), $(seed))
        val cvMetricsArray = ArrayBuffer[mutable.LinkedHashMap[String, Any]]()
        val paramLevelPredictDf = ArrayBuffer[DataFrame]()

        val metrics = splits
                .zipWithIndex
                .map
                { case ((training, validation), splitIndex) =>
                    val trainingDataset = sparkSession.createDataFrame(training, schema).select(colsToSelect.map(col): _*).cache
                    val validationDataset = sparkSession.createDataFrame(validation, schema).select(colsToSelect.map(col): _*).cache

                    // Fit models in a Future for training in parallel
                    val foldMetricFutures = epm.map(paramMap =>
                    {
                        // Note: We use MulticlassMetrics for both binary or multi-class classification models
                        Future[MulticlassMetrics]
                        {
                            logger.info(s"Training CV set ${splitIndex + 1} of ${$(numFolds)} with parameter map: ${paramMap.toSingleLineString}")
                            // Collect the complete pipeline for CV
                            val allStages = ArrayBuffer[PipelineStage]()
                            val estimator = est.asInstanceOf[Estimator[_]]

                            allStages += estimator
                            if (ConfigValues.isPlattScalingReqd)
                                allStages += new PlattScalar()
                                        .setIsMultiIntent(ConfigValues.isMultiIntent)
                                        .setLabelCol(ConfigValues.getIndexedResponseColumn)

                            val modelPipeline = new Pipeline().setStages(allStages.toArray)
                            val model = modelPipeline.fit(trainingDataset, paramMap)
                            val intermediateDf = model.transform(validationDataset, paramMap)
                            paramLevelPredictDf += intermediateDf
                            val combinedMetrics = new MulticlassMetrics(intermediateDf
                                    .select("prediction", ConfigValues.getIndexedResponseColumn)
                                    .as[(Double, Double)]
                                    .rdd)

                            //val metric = eval.evaluate(intermediateDf)
                            logger.info(
                                s"For fold: ${splitIndex + 1} with paramMap: ${paramMap.toSingleLineString}\n" +
                                s" Accuracy: ${decimalFormat.format(combinedMetrics.accuracy)}\n" +
                                s" Weighted Precision: ${decimalFormat.format(combinedMetrics.weightedPrecision)}\n" +
                                s" Weighted Recall: ${decimalFormat.format(combinedMetrics.weightedRecall)}\n" +
                                s" F1 Score: ${decimalFormat.format(combinedMetrics.weightedFMeasure)}"
                            )

                            cvMetricsArray += mutable.LinkedHashMap(
                                "foldNo" -> (splitIndex + 1),
                                "paramMap" -> paramMap.toString(),
                                "accuracy" -> combinedMetrics.accuracy,
                                "weightedPrecision" -> combinedMetrics.weightedPrecision,
                                "weightedRecall" -> combinedMetrics.weightedRecall,
                                "weightedF1" -> combinedMetrics.weightedFMeasure
                            )
                            combinedMetrics
                        }(executionContext)
                    })

                    // Wait for metrics to be calculated
                    val foldMetrics = foldMetricFutures
                            .map(ThreadUtils.awaitResult(_, Duration.Inf))

                    // Unpersist training & validation set once all metrics have been produced
                    trainingDataset.unpersist()
                    validationDataset.unpersist()
                    foldMetrics
                }.transpose

        val avgMetrics = metrics
                .map(v => (
                        v.map(_.accuracy).sum / $(numFolds),
                        v.map(_.weightedPrecision).sum / $(numFolds),
                        v.map(_.weightedRecall).sum / $(numFolds),
                        v.map(_.weightedFMeasure).sum / $(numFolds)
                ))

        logger.info(s"Average cross-validation metrics:")
        val avgCVMetricsArray = avgMetrics
                .zip(epm)
                .map
                { case (avgMetric, paramMap) =>
                    logger.info(s"For paramMap: ${paramMap.toSingleLineString}\n " +
                            s"\tAverage Accuracy: ${decimalFormat.format(avgMetric._1)}\n" +
                            s"\tAverage Weighted Precision: ${decimalFormat.format(avgMetric._2)}\n " +
                            s"\tAverage Weighted Recall: ${decimalFormat.format(avgMetric._3)} \n" +
                            s"\tAverage Weighted F1: ${decimalFormat.format(avgMetric._4)}")
                    mutable.LinkedHashMap(
                        "paramMap" -> paramMap.toSingleLineString,
                        "accuracy" -> avgMetric._1,
                        "weightedPrecision" -> avgMetric._2,
                        "weightedRecall" -> avgMetric._3,
                        "weightedF1" -> avgMetric._4)
                }

        // Copy over the values for storage
        MetricsEvaluator.csvMetrics ++= "Cross Validation Metrics (Fold Level)"
        MetricsEvaluator.csvMetrics ++= cvMetricsArray.toList(0).keys.toArray.mkString(",") + "\n" + cvMetricsArray
                .toList.foldLeft("")((acc, x) => acc + x.values.toArray.mkString(",") + "\n")
        MetricsEvaluator.csvMetrics ++= "\n\n"
        MetricsEvaluator.csvMetrics ++= "Cross Validation Metrics (Average)"
        MetricsEvaluator.csvMetrics ++= avgCVMetricsArray.toList(0).keys.toArray.mkString(",") + "\n" +
                avgCVMetricsArray.toList.foldLeft("")((acc, x) => acc + x.values.toArray.mkString(",") + "\n")
        MetricsEvaluator.csvMetrics ++= "\n\n"
        val cvMetricsBuffer = Map(
            "FoldLevel" -> cvMetricsArray.toArray,
            "Average" -> avgCVMetricsArray)

        MetricsEvaluator.metricsMap += ("CrossValidationMetrics" -> cvMetricsBuffer)
        val cvEvalMetricForFolds = avgMetrics.map(v =>
            ConfigValues.cvEvalMetric match {
                case "accuracy" => v._1
                case "weightedPrecision" => v._2
                case "weightedRecall" => v._3
                case "f1" => v._4
            }
        )

        val (bestMetric, bestIndex) =
            if (eval.isLargerBetter) cvEvalMetricForFolds.zipWithIndex.maxBy(_._1)
            else cvEvalMetricForFolds.zipWithIndex.minBy(_._1)

        logger.info(s"Best set of parameters:\n\t${epm(bestIndex).toSingleLineString}")
        logger.info(s"Best cross-validation metric (${ConfigValues.cvEvalMetric}): $bestMetric.")

        if (FlashMLConfig.getBool(FlashMLConstants.CV_PREDICT_SAVEPOINT))
        {
            val basePath = DirectoryCreator.getBasePath
            val cvDataPath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + basePath.toString + s"/cvData"
            DirectoryCreator.deleteDirectory(new Path(cvDataPath))
            // Save the complete dataframe with predictions for the best model. This can later be used to calculate thresholds.
            // Note: paramLevelPredictDf contains the dataframes in a single dimension array. We need to save only the
            // dataframes that belongs to the best index.
            (1 to $(numFolds)).foldLeft(bestIndex)({ (bestIndexToSave, _) =>
                paramLevelPredictDf(bestIndexToSave)
                        .select(colsToSave.map(col): _*)
                        .write
                        .mode(SaveMode.Append)
                        .save(cvDataPath + s"/predictedData")
                // Update the index to point to the next part
                bestIndexToSave + epm.length
            })
        }
        // Now fit the whole dataset to the best model
        log.info(s"Fitting the best model with params [${epm(bestIndex).toSingleLineString}] to the complete dataset.")
        val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
        copyValues(new CrossValidatorCustomModel(uid, bestModel, cvEvalMetricForFolds)
                .setSubModels(subModels).setParent(this))
    }

    override def transformSchema(schema: StructType): StructType = transformSchemaImpl(schema)

    override def copy(extra: ParamMap): CrossValidatorCustom =
    {
        val copied = defaultCopy(extra).asInstanceOf[CrossValidatorCustom]
        if (copied.isDefined(estimator))
        {
            copied.setEstimator(copied.getEstimator.copy(extra))
        }
        if (copied.isDefined(evaluator))
        {
            copied.setEvaluator(copied.getEvaluator.copy(extra))
        }
        copied
    }

    // Currently, this only works if all [[Param]]s in [[estimatorParamMaps]] are simple types.
    // E.g., this may fail if a [[Param]] is an instance of an [[Estimator]].
    // However, this case should be unusual.
    override def write: MLWriter = new CrossValidatorCustom.CrossValidatorCustomWriter(this)
}

object CrossValidatorCustom extends MLReadable[CrossValidatorCustom]
{
    override def read: MLReader[CrossValidatorCustom] = new CrossValidatorCustomReader

    override def load(path: String): CrossValidatorCustom = super.load(path)

    private[CrossValidatorCustom] class CrossValidatorCustomWriter(instance: CrossValidatorCustom) extends MLWriter
    {

        ValidatorParams.validateParams(instance)

        override protected def saveImpl(path: String): Unit =
            ValidatorParams.saveImpl(path, instance, sc)
    }

    private class CrossValidatorCustomReader extends MLReader[CrossValidatorCustom]
    {

        /** Checked against metadata when loading model */
        private val className = classOf[CrossValidatorCustom].getName

        override def load(path: String): CrossValidatorCustom =
        {
            implicit val format = DefaultFormats

            val (metadata, estimator, evaluator, estimatorParamMaps) =
                ValidatorParams.loadImpl(path, sc, className)

            val cv = new CrossValidatorCustom(metadata.uid)
                    .setEstimator(estimator)
                    .setEvaluator(evaluator)
                    .setEstimatorParamMaps(estimatorParamMaps)

            metadata
                    .getAndSetParams(cv, skipParams = Option(List("estimatorParamMaps")))
            cv
        }
    }

}

/**
  * CrossValidatorCustomModel contains the model with the highest average cross-validation
  * metric across folds and uses this model to transform input data. CrossValidatorCustomModel
  * also tracks the metrics for each param map evaluated.
  *
  * @param bestModel  The best model selected from k-fold cross validation.
  * @param avgMetrics Average cross-validation metrics for each paramMap in
  *                   `CrossValidatorCustom.estimatorParamMaps`, in the corresponding order.
  */

class CrossValidatorCustomModel private[ml](
                                           override val uid: String,
                                           val bestModel: Model[_],
                                           val avgMetrics: Array[Double])
        extends Model[CrossValidatorCustomModel] with CrossValidatorCustomParams with MLWritable
{

    /** A Python-friendly auxiliary constructor. */
    private[ml] def this(uid: String, bestModel: Model[_], avgMetrics: JList[Double]) =
    {
        this(uid, bestModel, avgMetrics.asScala.toArray)
    }

    private var _subModels: Option[Array[Array[Model[_]]]] = None

    private[tuning] def setSubModels(subModels: Option[Array[Array[Model[_]]]])
    : CrossValidatorCustomModel =
    {
        _subModels = subModels
        this
    }

    /**
      * @return submodels represented in two dimension array. The index of outer array is the
      *         fold index, and the index of inner array corresponds to the ordering of
      *         estimatorParamMaps
      * @throws IllegalArgumentException if subModels are not available. To retrieve subModels,
      *                                  make sure to set collectSubModels to true before fitting.
      */

    def subModels: Array[Array[Model[_]]] =
    {
        require(_subModels.isDefined, "subModels not available, To retrieve subModels, make sure " +
                "to set collectSubModels to true before fitting.")
        _subModels.get
    }

    def hasSubModels: Boolean = _subModels.isDefined

    override def transform(dataset: Dataset[_]): DataFrame =
    {
        transformSchema(dataset.schema, logging = true)
        bestModel.transform(dataset)
    }

    override def transformSchema(schema: StructType): StructType =
    {
        bestModel.transformSchema(schema)
    }

    override def copy(extra: ParamMap): CrossValidatorCustomModel =
    {
        val copied = new CrossValidatorCustomModel(
            uid,
            bestModel.copy(extra).asInstanceOf[Model[_]],
            avgMetrics.clone()
        ).setSubModels(CrossValidatorCustomModel.copySubModels(_subModels))
        copyValues(copied, extra).setParent(parent)
    }

    override def write: CrossValidatorCustomModel.CrossValidatorCustomModelWriter =
    {
        new CrossValidatorCustomModel.CrossValidatorCustomModelWriter(this)
    }
}

object CrossValidatorCustomModel extends MLReadable[CrossValidatorCustomModel]
{

    private[CrossValidatorCustomModel] def copySubModels(subModels: Option[Array[Array[Model[_]]]])
    : Option[Array[Array[Model[_]]]] =
    {
        subModels.map(_.map(_.map(_.copy(ParamMap.empty).asInstanceOf[Model[_]])))
    }

    override def read: MLReader[CrossValidatorCustomModel] = new CrossValidatorCustomModelReader


    override def load(path: String): CrossValidatorCustomModel = super.load(path)

    /**
      * Writer for CrossValidatorCustomModel.
      *
      * @param instance CrossValidatorCustomModel instance used to construct the writer
      *
      *                 CrossValidatorCustomModelWriter supports an option "persistSubModels", with possible values
      *                 "true" or "false". If you set the collectSubModels Param before fitting, then you can
      *                 set "persistSubModels" to "true" in order to persist the subModels. By default,
      *                 "persistSubModels" will be "true" when subModels are available and "false" otherwise.
      *                 If subModels are not available, then setting "persistSubModels" to "true" will cause
      *                 an exception.
      */

    final class CrossValidatorCustomModelWriter private[tuning](
                                                                       instance: CrossValidatorCustomModel) extends
            MLWriter
    {

        override protected def saveImpl(path: String): Unit =
        {

            // If the estimator model is OneVsRestCustomModel, we save that instead of CrossValidatorModel, because
            // it throws an error linear_svc not found error while loading the model.
            if (instance.bestModel.isInstanceOf[OneVsRestCustomModel])
            {
                instance.bestModel.asInstanceOf[MLWritable].save(path)
            }
            else
            {
                val persistSubModelsParam = optionMap.getOrElse("persistsubmodels",
                    if (instance.hasSubModels) "true"
                    else "false")

                require(Array("true", "false").contains(persistSubModelsParam.toLowerCase(Locale.ROOT)),
                    s"persistSubModels option value ${persistSubModelsParam} is invalid, the possible " +
                            "values are \"true\" or \"false\"")
                val persistSubModels = persistSubModelsParam.toBoolean

                import org.json4s.JsonDSL._
                val extraMetadata = ("avgMetrics" -> instance.avgMetrics.toSeq) ~
                        ("persistSubModels" -> persistSubModels)
                ValidatorParams.saveImpl(path, instance, sc, Some(extraMetadata))
                val bestModelPath = new Path(path, "bestModel").toString
                instance.bestModel.asInstanceOf[MLWritable].save(bestModelPath)
                if (persistSubModels)
                {
                    require(instance.hasSubModels, "When persisting tuning models, you can only set " +
                            "persistSubModels to true if the tuning was done with collectSubModels set to true. " +
                            "To save the sub-models, try rerunning fitting with collectSubModels set to true.")
                    val subModelsPath = new Path(path, "subModels")
                    for (splitIndex <- 0 until instance.getNumFolds)
                    {
                        val splitPath = new Path(subModelsPath, s"fold${splitIndex.toString}")
                        for (paramIndex <- 0 until instance.getEstimatorParamMaps.length)
                        {
                            val modelPath = new Path(splitPath, paramIndex.toString).toString
                            instance.subModels(splitIndex)(paramIndex).asInstanceOf[MLWritable].save(modelPath)
                        }
                    }
                }
            }
        }
    }

    private class CrossValidatorCustomModelReader extends MLReader[CrossValidatorCustomModel]
    {

        /** Checked against metadata when loading model */
        private val className = classOf[CrossValidatorCustomModel].getName

        override def load(path: String): CrossValidatorCustomModel =
        {
            implicit val format = DefaultFormats

            val (metadata, estimator, evaluator, estimatorParamMaps) = ValidatorParams.loadImpl(path, sc, className)
            val numFolds = (metadata.params \ "numFolds").extract[Int]
            val bestModelPath = new Path(path, "bestModel").toString
            val bestModel = DefaultParamsReader.loadParamsInstance[Model[_]](bestModelPath, sc)
            val avgMetrics = (metadata.metadata \ "avgMetrics").extract[Seq[Double]].toArray
            val persistSubModels = (metadata.metadata \ "persistSubModels")
                    .extractOrElse[Boolean](false)

            val subModels: Option[Array[Array[Model[_]]]] = if (persistSubModels)
            {
                val subModelsPath = new Path(path, "subModels")
                val _subModels = Array.fill(numFolds)(Array.fill[Model[_]](
                    estimatorParamMaps.length)(null))
                for (splitIndex <- 0 until numFolds)
                {
                    val splitPath = new Path(subModelsPath, s"fold${splitIndex.toString}")
                    for (paramIndex <- 0 until estimatorParamMaps.length)
                    {
                        val modelPath = new Path(splitPath, paramIndex.toString).toString
                        _subModels(splitIndex)(paramIndex) =
                                DefaultParamsReader.loadParamsInstance(modelPath, sc)
                    }
                }
                Some(_subModels)
            }
            else None

            val model = new CrossValidatorCustomModel(metadata.uid, bestModel, avgMetrics)
                    .setSubModels(subModels)
            model.set(model.estimator, estimator)
                    .set(model.evaluator, evaluator)
                    .set(model.estimatorParamMaps, estimatorParamMaps)
            metadata.getAndSetParams(model, skipParams = Option(List("estimatorParamMaps")))
            model
        }
    }

}
