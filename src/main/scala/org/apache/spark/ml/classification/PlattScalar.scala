package org.apache.spark.ml.classification

import java.util.UUID

import com.tfs.flashml.util.ConfigValues
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.attribute.BinaryAttribute
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, Metadata, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ThreadUtils
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, JObject, _}

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration

/**
  * Params and Constants for Platt Scalar.
  */
private[ml] trait PlattScalarParams extends Params
  with HasProbabilityCol
  with HasRawPredictionCol
  with HasPredictionCol
  with HasLabelCol
{

    // Constants
    val PLATT_RAW_FEATURE_COLUMN = "platt_relevant_raw_prediction"
    val PLATT_FEATURES_COLUMN = "platt_features"
    val PLATT_SCORE_COLUMN = "platt_raw_prediction"
    val PLATT_PROBABILITY_COLUMN = "probability"
    val PLATT_PREDICTION_COLUMN = "platt_prediction"

    /**
      * Param for whether the model is multi-intent or not.
      */
    final val isMultiIntent: BooleanParam = new BooleanParam(this, "isMultiIntent", "whether the model is multi intent")

    /**
      * Param for whether the model is multi-intent or not.
      */
    def getIsMultiIntent: Boolean = $(isMultiIntent)

    /**
      * Extracts the relevant rawProbability from the raw probability vector and
      * apply vector assembler on the value to get the input features for logistic
      * regression model
      *
      * @param df    input dataframe
      * @param index index of Vector
      * @return Dataframe with new vector
      */
    def buildPlattFeatures(df: Dataset[_], index: Int = 1): DataFrame =
    {

        def rawPredExtractionUdf(idx: Int) = udf((a: Vector) => a(idx))

        val featureDF = df.withColumn(
            PLATT_RAW_FEATURE_COLUMN, rawPredExtractionUdf(index)(col("rawPrediction"))
        )

        // Vectorize the raw probabilities
        val assembler = new VectorAssembler()
                .setInputCols(Array(PLATT_RAW_FEATURE_COLUMN))
                .setOutputCol(PLATT_FEATURES_COLUMN)

        assembler.transform(featureDF)
    }
}

private[ml] object PlattScalarParams
{

    def validateParams(instance: PlattScalarParams): Unit =
    {
        def checkElement(elem: Params, name: String): Unit = elem match
        {
            case stage: MLWritable => // good
            case other =>
                throw new UnsupportedOperationException("Platt Scalar write will fail " +
                        s" because it contains $name which does not implement MLWritable." +
                        s" Non-Writable $name: ${other.uid} of type ${other.getClass}")
        }

        instance match
        {
            case plattScalarModel: PlattScalarModel => plattScalarModel.models.foreach(checkElement(_, "model"))
            case _ => // no need to check PlattScalar here
        }

    }

    def saveImpl(path: String,
                instance: PlattScalarParams,
                sc: SparkContext,
                extraMetadata: Option[JObject] = None): Unit =
    {

        val params = instance.extractParamMap().toSeq
        val jsonParams = render(params
                .map{ case ParamPair(p, v) => p.name -> parse(p.jsonEncode(v)) }
                .toList)

        DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata, Some(jsonParams))
    }

    def loadImpl(path: String,
                sc: SparkContext,
                expectedClassName: String): DefaultParamsReader.Metadata =
    {

        DefaultParamsReader.loadMetadata(path, sc, expectedClassName)
    }

}

/**
  * Model produced by [[PlattScalar]]
  * This stores the model/s which are meant to get the probability for SVM
  *
  * @param uid
  * @param models Array of logistic regression models
  */
final class PlattScalarModel(override val uid: String, val
models: Array[BinaryLogisticRegressionWithDoubleResponseModel])
        extends Model[PlattScalarModel]
                with PlattScalarParams
                with MLWritable
{

    override def transformSchema(schema: StructType): StructType =
    {

    //check whether Label is of numeric type
    //SchemaUtils.checkNumericType(schema, $(labelCol))

        // Append probability column to the data
        SchemaUtils.appendColumn(schema, $(probabilityCol), new VectorUDT)
    }

    override def transform(dataset: Dataset[_]): DataFrame =
    {
        // Check schema
        transformSchema(dataset.schema, logging = true)

        if ($(isMultiIntent))
        {
            // Determine the input columns: these are needed to be passed through
            val origCols = dataset
                    .schema
                    .map(f => col(f.name))

            // Add an accumulator column to store predictions from all the models in OVR.
            // This column will be used to calculate probability column for OVR model
            val accColName = "accumcol_" + UUID.randomUUID().toString
            val initUDF = udf{ () => Map[Int, Double]() }

      val newDataset = dataset
        .withColumn(accColName, initUDF())

            // Persist if underlying dataset is not persistent.
            val handlePersistence = !dataset.isStreaming && dataset.storageLevel == StorageLevel.NONE
            if (handlePersistence)
            {
                newDataset.persist(StorageLevel.MEMORY_AND_DISK)
            }

            // Update the accumulator column with the result of prediction of models
            val aggregatedDataset = models
                    .zipWithIndex
                    .foldLeft[DataFrame](newDataset)
                    {
                        case (df, (model, index)) =>
                            val probabilityCol: String = model.getProbabilityCol
                            val columns = origCols ++ List(col(probabilityCol), col(accColName))
                            val tmpColName = "tmpcol_" + UUID.randomUUID().toString

                            // Build features
                            val plattFeaturesDataset = buildPlattFeatures(df, index)
                            val transformedDataset = model
                                    .transform(plattFeaturesDataset)
                                    .select(columns: _*)

                            // Define an UDF which will extract the prediction value for this model, and
                            // save it in the hashmap in accumulator column.
                            // ToDo: Can this be optimized, by not using UDF?
                            val updateUDF = udf
                            { (predictions: Map[Int, Double], prediction: Vector) =>
                                predictions + ((index, prediction(1)))
                            }

                            val updatedDataset = transformedDataset
                                    .withColumn(tmpColName, updateUDF(col(accColName), col(probabilityCol)))

                            val newColumns = origCols ++ List(col(tmpColName))

                            // Switch out the intermediate column with the accumulator column
                            updatedDataset
                                .select(newColumns: _*)
                                .withColumnRenamed(tmpColName, accColName)
                    }

            if (handlePersistence)
                newDataset.unpersist()

            // UDF to convert Map of Scores to DenseVector for compatibility with Logistic Output
            val mapToVectorUDF = udf
            { predictions: Map[Int, Double] =>
                Vectors.dense(predictions
                        .toSeq
                        .sortBy(_._1)
                        .map(_._2)
                        .toArray[Double])
            }

            // UDF to extract the label as per Platt scores.
            val plattLabelUDF = udf
            { predictionMap: Map[Int, Double] =>
                predictionMap
                        .maxBy(_._2)
                        ._1
                        .toDouble
            }

            // We will need the metadata of the prediction column from LinearSVC, to be applied to the
            // new prediction column from which will be calculated from Platt Scores.
            val origMetadata = aggregatedDataset
                .select("prediction")
                .schema(0)
                .metadata

            // We now obtain labels (predictions) from the Platt Scores.
            // The intent with the max positive probability value is the prediction label.
            // The original label from SVM is renamed to svm_prediction.
            aggregatedDataset
                    .withColumn(PLATT_PROBABILITY_COLUMN, mapToVectorUDF(col(accColName)))
                    .withColumnRenamed("prediction", "svm_prediction")
                    .withColumn("prediction", plattLabelUDF(col(accColName)), origMetadata)
                    .drop(accColName)
        }
        else
        {
            val plattFeaturesDataset = buildPlattFeatures(dataset)
            val outputDf = models(0).transform(plattFeaturesDataset)

            // drop extra columns
            val colsToDrop = Seq(PLATT_FEATURES_COLUMN, PLATT_SCORE_COLUMN, PLATT_PREDICTION_COLUMN, PLATT_RAW_FEATURE_COLUMN)

            outputDf.drop(colsToDrop: _*)
        }
    }

    override def copy(extra: ParamMap): PlattScalarModel =
    {
        val copied = new PlattScalarModel(uid, models.map(_.copy(extra)))
        copyValues(copied, extra).setParent(parent)
    }

    override def write: MLWriter = new PlattScalarModel.PlattScalarModelWriter(this)

}

/**
  * Companion Object for [[PlattScalarModel]]
  * Contains implementations for writing and reading the model
  */
object PlattScalarModel extends MLReadable[PlattScalarModel]
{

    override def read: MLReader[PlattScalarModel] = new PlattScalarModelReader

    override def load(path: String): PlattScalarModel = super.load(path)

    /**
      * [[MLWriter]] instance for [[PlattScalarModel]]
      */
    private[PlattScalarModel] class PlattScalarModelWriter(instance: PlattScalarModel) extends MLWriter
    {

        PlattScalarParams.validateParams(instance)

        override protected def saveImpl(path: String): Unit =
        {
            val extraJson = if (instance.getIsMultiIntent) Some(JsonAST.JObject(List("numClasses" -> JInt(instance
                    .models.length))))
            else None
            PlattScalarParams.saveImpl(path, instance, sc, extraJson)
            instance.models.map(_.asInstanceOf[MLWritable]).zipWithIndex.foreach
            { case (model, idx) =>
                val modelPath = new Path(path, s"binlrModel_$idx").toString
                model.save(modelPath)
            }
        }
    }

    /**
      * [[MLReader]] instance for [[PlattScalarModel]]
      */
    private class PlattScalarModelReader extends MLReader[PlattScalarModel]
    {

        /** Checked against metadata when loading model */
        private val className = classOf[PlattScalarModel].getName

        override def load(path: String): PlattScalarModel =
        {
            implicit val format: DefaultFormats.type = DefaultFormats
            val metadata = PlattScalarParams.loadImpl(path, sc, className)
            val isMultiIntent = (metadata.params \ "isMultiIntent").extract[Boolean]
            val models = if (isMultiIntent)
            {
                val numClasses = (metadata.metadata \ "numClasses").extract[Int]
                Range(0, numClasses).toArray.map
                { idx =>
                    val modelPath = new Path(path, s"binlrModel_$idx").toString
                    DefaultParamsReader.loadParamsInstance[BinaryLogisticRegressionWithDoubleResponseModel](modelPath, sc)
                }
            }
            else
            {
                val modelPath = new Path(path, s"binlrModel_0").toString
                Array(DefaultParamsReader.loadParamsInstance[BinaryLogisticRegressionWithDoubleResponseModel](modelPath, sc))
            }
            val plattScalarModel = new PlattScalarModel(metadata.uid, models)
            metadata.getAndSetParams(plattScalarModel)
            plattScalarModel
        }
    }

}

/**
  * PlattScaler calibrates the score for an SVM classifier to fall into a [0, 1] range and can be used as a
  * probability score.
  * Post SVM transformation, we have the raw predictions - however for scoring, we require probability
  * which lies between 0 and 1. This is done by fitting the raw probability on the prediction column with a logistic
  * regression model. The resultant probabilities are considered as the model probability.
  * <br />
  * The output adds a column titled "probability" in the dataframe
  * <br />
  * More information: <br />
  * [1] Platt, J. C. Probabilistic Outputs for Support Vector Machines and Comparisons to Regularized Likelihood
  * Methods.
  * ADVANCES IN LARGE MARGIN CLASSIFIERS. p61-74. MIT Press. 1999. <br />
  * URL: http://citeseer.ist.psu.edu/viewdoc/summary?doi=10.1.1.41.1639 <br />
  * [2] https://en.wikipedia.org/wiki/Platt_scaling <br />
  *
  * @since 3/9/18
  *
  */
final class PlattScalar(override val uid: String)
        extends Estimator[PlattScalarModel]
                with PlattScalarParams
                with HasParallelism
                with MLWritable
{

    private val logger = Logger.getLogger(getClass)
    logger.setLevel(Level.INFO)

  def this() = this(Identifiable.randomUID("platt_scalar"))

    def setIsMultiIntent(value: Boolean): this.type = set(isMultiIntent, value)

    def setLabelCol(value: String): this.type = set(labelCol, value)

  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)
  setDefault(rawPredictionCol,"rawPrediction")
  setDefault(predictionCol,"prediction")

    def setParallelism(value: Int): this.type = set(parallelism, value)

    override def transformSchema(schema: StructType): StructType =
    {

    //check whether Label is of numeric type
    //SchemaUtils.checkNumericType(schema, $(labelCol))

        //Append probability column to the data
        SchemaUtils.appendColumn(schema, $(probabilityCol), new VectorUDT)
    }

    override def fit(dataset: Dataset[_]): PlattScalarModel = instrumented
    { instr =>
        logger.info("Starting Platt Scaling: Calibration of Model Scores")

        instr.logParams(this, labelCol, isMultiIntent)

        // Extract the raw prediction vector from the dataset
        val rawPredictionDf = dataset.select($(labelCol), "rawPrediction")

        val models = if ($(isMultiIntent))
        {

            // persist if underlying dataset is not persistent.
            val handlePersistence = dataset.storageLevel == StorageLevel.NONE
            if (handlePersistence)
            {
                rawPredictionDf.persist(StorageLevel.MEMORY_AND_DISK)
            }

            // Determine number of classes either from metadata if provided, or via computation.
            // This part of the code is same as in OneVsRest
            val labelSchema = rawPredictionDf.schema($(labelCol))
            val computeNumClasses: () => Int = () =>
            {
                val Row(maxLabelIndex: Double) = rawPredictionDf.agg(max(col($(labelCol)).cast(DoubleType))).head()
                // classes are assumed to be numbered from 0,...,maxLabelIndex
                maxLabelIndex.toInt + 1
            }
            val numClasses = MetadataUtils.getNumClasses(labelSchema).fold(computeNumClasses())(identity)
            instr.logNumClasses(numClasses)

            val executionContext = getExecutionContext

            // Create k columns, one for each binary classifier.
            val modelFutures = Range(0, numClasses).map
            { index =>

                Future
                {
                    // generate new label metadata for the binary problem.
                    val newLabelMeta: Metadata = BinaryAttribute
                            .defaultAttr
                            .withName("label").toMetadata()

                    val labelColName = "response$" + index
                    // Set up the binary response column
                    val classLevelDataset = rawPredictionDf
                            .withColumn(
                                labelColName, when(col($(labelCol)) === index.toDouble, 1.0).otherwise(0.0),
                              newLabelMeta)

                    logger.info(s"Running fit for PlattScaler model for class with index ${index + 1} of $numClasses")
                    // Now we will update the targets of {0,1} for class labels to a Bayes-inspired revised
                    // target, based on the original paper from by Platt [1].
                    // First, calculate the lower and upper target probabilities.
                    // Start with calculating the counts of positive and negative class and get them in a map
                    val classCounts = classLevelDataset
                            .groupBy(labelColName)
                            .agg(count(labelColName))
                            .collect() // Steps before this should be executed in parallel
                            .foldLeft(new mutable.HashMap[Double, Long]())
                            { (a, v) => a += (v(0).asInstanceOf[Double] -> v(1).asInstanceOf[Long]) }
                    val (lowerVal, upperVal) = (1.0d / (classCounts(0.0) + 2), (classCounts(1.0) + 1.0d) / (classCounts(1.0) + 2.0d))

                    // Now update the same column
                    val updatedClassLevelDS = classLevelDataset
                            .withColumn(labelColName, when(col(labelColName) === 0.0, lowerVal).otherwise(upperVal))

                    // Build features
                    val trainingDataset = buildPlattFeatures(updatedClassLevelDS, index)

                    val binLR = new BinaryLogisticRegressionWithDoubleResponse()
                            .setRegParam(0.1)
                            .setFeaturesCol(PLATT_FEATURES_COLUMN)
                            .setRawPredictionCol(PLATT_SCORE_COLUMN)
                            .setProbabilityCol(PLATT_PROBABILITY_COLUMN)
                            .setPredictionCol(PLATT_PREDICTION_COLUMN)
                            .setLabelCol(labelColName)

                    val model = binLR.fit(trainingDataset)
                    model
                }(executionContext)
            }

            val models = modelFutures
                    .map(ThreadUtils.awaitResult(_, Duration.Inf)).toArray[BinaryLogisticRegressionWithDoubleResponseModel]

            if (handlePersistence)
            {
                rawPredictionDf.unpersist()
            }

            models
        }
        else
        {
            // build features
            // TODO: Bug here? Shouldn't we be scaling this dataset as well like for multinomial case?
            val trainingDataset = buildPlattFeatures(rawPredictionDf)

            val lr = new BinaryLogisticRegressionWithDoubleResponse()
                    .setRegParam(0.1)
                    .setFeaturesCol(PLATT_FEATURES_COLUMN)
                    .setLabelCol($(labelCol))
                    .setRawPredictionCol(PLATT_SCORE_COLUMN)
                    .setProbabilityCol(PLATT_PROBABILITY_COLUMN)
                    .setPredictionCol(PLATT_PREDICTION_COLUMN)

            val lrModel = lr.fit(trainingDataset)

            Array(lrModel)
        }

        val model = new PlattScalarModel(uid, models).setParent(this)

        // Copies the Params from PlattScalar to PlattScalarModel
        copyValues(model)
    }

    override def copy(extra: ParamMap): PlattScalar = defaultCopy(extra).asInstanceOf[PlattScalar]

    override def write: MLWriter = new PlattScalar.PlattScalarWriter(this)
}

/**
  * Companion Object for [[PlattScalar]]
  * Contains implementations for writing and reading the model
  */
object PlattScalar extends MLReadable[PlattScalar]
{

    override def read: MLReader[PlattScalar] = new PlattScalarReader

    override def load(path: String): PlattScalar = super.load(path)

    /** [[MLWriter]] instance for [[PlattScalar]] */
    private[PlattScalar] class PlattScalarWriter(instance: PlattScalar) extends MLWriter
    {

        PlattScalarParams.validateParams(instance)

        override protected def saveImpl(path: String): Unit =
        {
            PlattScalarParams.saveImpl(path, instance, sc)
        }
    }

    private class PlattScalarReader extends MLReader[PlattScalar]
    {

        /** Checked against metadata when loading model */
        private val className = classOf[PlattScalar].getName

        override def load(path: String): PlattScalar =
        {
            val metadata = PlattScalarParams.loadImpl(path, sc, className)
            val plattScalar = new PlattScalar(metadata.uid)
                    .setParallelism(ConfigValues.flashmlParallelism)

            metadata.getAndSetParams(plattScalar)
            plattScalar
        }
    }

}


