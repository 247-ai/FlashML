package org.apache.spark.ml.classification

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.PlattScalarModel
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasProbabilityCol}
import org.apache.spark.ml.param.{Param, ParamMap, ParamPair, Params}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Model, Transformer}
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}

/**
 * Params for [[UpliftTransformer]].
 */
trait UpliftParams extends Params with HasFeaturesCol with HasProbabilityCol
{
    type ModelType = Model[_]
    type PlattScalarModelType = PlattScalarModel
    /**
     * param for the base classifier
     */
    final val baseClassifier: Param[ModelType] = new Param(this, "baseClassifier", "base classifier")

    def getBaseClassifier: ModelType = $(baseClassifier)

    /**
     * param fo platt scaling model
     */
    final val plattScalar: Param[PlattScalarModelType] = new Param(this, "plattScalingModel", "platt scaling model")

    def getPlattScaler: PlattScalarModelType = $(plattScalar)
}

object UpliftParams
{
    def validateParams(instance: UpliftParams): Unit =
    {
        def checkElement(elem: Params, name: String): Unit = elem match
        {
            case _: MLWritable => // good
            case other =>
                throw new UnsupportedOperationException("Uplift write will fail " +
                    s" because it contains $name which does not implement MLWritable." +
                    s" Non-Writable $name: ${other.uid} of type ${other.getClass}")
        }

        instance match
        {
            case uplift: UpliftTransformer =>
                checkElement(uplift.getBaseClassifier, "model")
                if (uplift.extractParamMap.contains(uplift.plattScalar))
                    checkElement(uplift.getPlattScaler, "model")
            case _ => // no need to check PlattScalar here
        }
    }
}

/**
 * Uplift Transformation applies the model on the data twice. First the treatment
 * variable is set to 0 and scoring is done, next it is set to 1 followed by scoring.
 * These two scores are subtracted to get the uplift probability. <br/>
 * <br />
 * Uplift Transformation is not applicable for multi intent models.
 *
 * @since 22/8/18
 */
class UpliftTransformer(override val uid: String)
    extends Transformer
        with UpliftParams
        with MLWritable
{
    def this() = this(Identifiable.randomUID("uplift"))

    def setBaseClassifier(value: Model[_]): this.type =
    {
        set(baseClassifier, value.asInstanceOf[ModelType])
    }

    def setPlattScalar(value: PlattScalarModel): this.type =
    {
        set(plattScalar, value.asInstanceOf[PlattScalarModel])
    }

    def copy(extra: ParamMap): UpliftTransformer = defaultCopy(extra)

    def transform(df: Dataset[_]): DataFrame =
    {
        // Uplift specific UDFs
        val upliftProbabilityCoder: (Vector, Vector) => Vector = (num1: Vector, num2: Vector) =>
        {
            Vectors.dense(Array(1 - (num1(1) - num2(1)), num1(1) - num2(1)))
        }
        val upliftProbabilityFunc = udf[Vector, Vector, Vector](upliftProbabilityCoder)
        val rawPredictionCoder: Vector => Vector = (num1: Vector) =>
        {
            Vectors.dense(Array(if (num1(0) >= 1) 100
            else scala.math.log(num1(0) / (1 - num1(0))), if (num1(1) <= 0) -100
            else scala.math.log(num1(1) / (1 - num1(1)))))
        }
        val rawProbabilityFunc = udf[Vector, Vector](rawPredictionCoder)
        var renamedDataSet = df
            .withColumnRenamed("probability", "modelProbability")
            .withColumnRenamed("prediction", "modelPrediction")
            .withColumnRenamed("rawPrediction", "modelRawPrediction")

        // Uplift Treatment
        // Repeat for treatment values
        for (treatmentValue <- 0 to 1)
        {
            // Prob column name
            val treatmentProbColumn = if (treatmentValue == 0)
                "probabilityTreatmentNegative"
            else
                "probabilityTreatmentPositive"
            // Setting the uplift variable to 0 or 1
            val treatmentCoder: Vector => Vector = (arg: Vector) =>
            {
                // Converted Vector to Array because value update is not a member of class org.apache.spark.ml.linalg.Vector
                val features = arg.toArray
                features(features.length - 1) = if (treatmentValue == 0) 0.0 else 1.0
                Vectors.dense(features)
            }
            val treatmentFunc = udf[Vector, Vector](treatmentCoder)
            val intermediateDf1 = renamedDataSet.withColumn("features", treatmentFunc(col("features")))
            var intermediateDf2 = getBaseClassifier.transform(intermediateDf1)
            if (isDefined(plattScalar))
            {
                intermediateDf2 = getPlattScaler.transform(intermediateDf2)
            }

            // Reassigning to original dataset because this is a two round loop
            renamedDataSet = intermediateDf2.withColumnRenamed("probability", treatmentProbColumn)
                .drop(Seq("prediction", "rawPrediction"): _*)
        }
        renamedDataSet
            .withColumn("probability",
                upliftProbabilityFunc(col("probabilityTreatmentPositive"),
                    col("probabilityTreatmentNegative")))
            .withColumn("prediction", lit(1.0))
            .withColumn("rawPrediction", rawProbabilityFunc(col("probability")))
    }

    override def transformSchema(schema: StructType): StructType =
    {
        // Add the return field
        schema
            .add(StructField("modelProbability", new VectorUDT, false))
            .add(StructField("modelRawPrediction", new VectorUDT, false))
            .add(StructField("modelPrediction", DoubleType, false))
    }

    override def write: MLWriter = new UpliftTransformer.UpliftTransformerWriter(this)
}

object UpliftTransformer extends MLReadable[UpliftTransformer]
{
    override def read: MLReader[UpliftTransformer] = new UpliftTransformerReader

    override def load(path: String): UpliftTransformer = super.load(path)

    /** [[MLWriter]] instance for [[UpliftTransformer]] */
    private[UpliftTransformer] class UpliftTransformerWriter(instance: UpliftTransformer) extends MLWriter
    {
        UpliftParams.validateParams(instance)

        override protected def saveImpl(path: String): Unit =
        {
            val params = instance.extractParamMap().toSeq
            val jsonParams = render(params
                .filter{ case ParamPair(p, v) => p.name != "baseClassifier" && p.name != "plattScalingModel" }
                .map{ case ParamPair(p, v) => p.name -> parse(p.jsonEncode(v)) }
                .toList)
            DefaultParamsWriter.saveMetadata(instance, path, sc, None, Some(jsonParams))
            val baseClassifierPath = new Path(path, s"baseClassifier").toString
            instance.getBaseClassifier.asInstanceOf[MLWritable].save(baseClassifierPath)
            if (instance.extractParamMap.contains(instance.plattScalar))
            {
                val plattScalarPath = new Path(path, s"plattScalar").toString
                instance.getPlattScaler.save(plattScalarPath)
            }
        }
    }

    private class UpliftTransformerReader extends MLReader[UpliftTransformer]
    {
        /** Checked against metadata when loading model */
        private val className = classOf[UpliftTransformer].getName

        override def load(path: String): UpliftTransformer =
        {
            implicit val format: DefaultFormats.type = DefaultFormats
            val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
            val baseClassifierPath = new Path(path, s"baseClassifier").toString
            val baseClassifer: Model[_] = DefaultParamsReader.loadParamsInstance[Model[_]](baseClassifierPath, sc)
            // Need to check if platt scaling exists, onlu then load
            val plattScalarPath = new Path(path, s"plattScalar")
            val hadoopConf = sc.hadoopConfiguration
            val fs = plattScalarPath.getFileSystem(hadoopConf)
            val qualifiedOutputPath = plattScalarPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
            val plattScalar: Option[PlattScalarModel] = if (fs.exists(qualifiedOutputPath))
                Some(DefaultParamsReader.loadParamsInstance[PlattScalarModel](plattScalarPath.toString, sc))
            else None
            val upliftTransformer = new UpliftTransformer(metadata.uid)
            metadata.getAndSetParams(upliftTransformer)
            plattScalar match
            {
                case Some(ps) => upliftTransformer.setBaseClassifier(baseClassifer).setPlattScalar(ps)
                case None => upliftTransformer.setBaseClassifier(baseClassifer)
            }
        }
    }

}