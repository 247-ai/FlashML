package org.apache.spark.ml.classification

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}

/**
  * Params for [[TopKIntents]].
  */
trait TopKIntentsParams extends Params with HasOutputCol {

  /**
    * param for the base classifier
    */
  final val stringIndexerModel: Param[StringIndexerModel] = new Param(this, "stringIndexerModel", "String Indexer Model")

  def getStringIndexerModel: StringIndexerModel = $(stringIndexerModel)

  /**
    * param for number of intents
    */
  final val kValue: IntParam = new IntParam(this, "kValue", "Number of Intents")

  def getKValue: Int = $(kValue)
}


object TopKIntentsParams {

  def validateParams(instance: TopKIntentsParams): Unit = {
    def checkElement(elem: Params, name: String): Unit = elem match {
      case _: MLWritable => // good
      case other =>
        throw new UnsupportedOperationException("Uplift write will fail " +
          s" because it contains $name which does not implement MLWritable." +
          s" Non-Writable $name: ${other.uid} of type ${other.getClass}")
    }

    instance match {
      case topKIntents: TopKIntents =>
        checkElement(topKIntents.getStringIndexerModel, "model")
      case _ =>
    }
  }
}

/**
  * Calculate the top K intents for Multi Intent models. By default multi-intent model scoring
  * only gives the final prediction which has the probability. This gives the top K intents along
  * with their probability for analysis.
  * <br />
  * Schema note: the output of [[TopKIntents]] is stored in a column call "top_intents", and can be loaded
  * back in a spark_sql session, or extracted in a text file using an UDF like below:<br />
  * &nbsp;&nbsp;&nbsp;&nbsp; val extractTopK = udf((xs: Seq[Row]) => xs.mkString(","))
  * @since 22/8/18
  */
class TopKIntents(override val uid: String)
  extends Transformer
    with TopKIntentsParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("topKIntent"))

  def setStringIndexerModel(value: StringIndexerModel): this.type = {
    set(stringIndexerModel, value.asInstanceOf[StringIndexerModel])
  }

  def setKValue(value: Int): this.type = set(kValue, value)

  def setOutputCol(value: String = "top_intents"): this.type = set(outputCol, value)

  def copy(extra: ParamMap): TopKIntents = defaultCopy(extra)

  def transform(df: Dataset[_]): DataFrame = {

    // Get StringIndexerLabels for intents
    val intentLabels = getStringIndexerModel.labels

    def topIntentUDF(labels: Array[String]) = udf((probabilityCol: Vector) => {
      // Get index and probability from array
      val scoresAndLabels =
        for ((score, idx) <- probabilityCol.toArray.zipWithIndex) yield (labels(idx), score)
      scoresAndLabels.sortBy(-_._2).take(getKValue)
    })

    df
            .withColumn($(outputCol), topIntentUDF(intentLabels)(col("probability")))

  }

  /**
    * @param schema
    * @return
    */
  override def transformSchema(schema: StructType): StructType =
  {
    // Add the return field
    schema.add(StructField($(outputCol), ArrayType(StructType(Array(StructField("_1", StringType, false), StructField("_2", DoubleType, false)))), true))
  }

  override def write: MLWriter = new TopKIntents.TopKIntentsWriter(this)

}

object TopKIntents extends MLReadable[TopKIntents] {

  override def read: MLReader[TopKIntents] = new TopKIntentsReader

  override def load(path: String): TopKIntents = super.load(path)

  /** [[MLWriter]] instance for [[TopKIntents]] */
  private[TopKIntents] class TopKIntentsWriter(instance: TopKIntents) extends MLWriter {

    TopKIntentsParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      val params = instance.extractParamMap().toSeq
      val jsonParams = render(params
        .filter { case ParamPair(p, v) => p.name != "stringIndexerModel" }
        .map { case ParamPair(p, v) => p.name -> parse(p.jsonEncode(v)) }
        .toList)

      DefaultParamsWriter.saveMetadata(instance, path, sc, None, Some(jsonParams))

      val stringIndexerModelPath = new Path(path, s"stringIndexerModel").toString
      instance.getStringIndexerModel.asInstanceOf[MLWritable].save(stringIndexerModelPath)
    }
  }

  private class TopKIntentsReader extends MLReader[TopKIntents] {

    /** Checked against metadata when loading model */
    private val className = classOf[TopKIntents].getName

    override def load(path: String): TopKIntents = {
      implicit val format: DefaultFormats.type = DefaultFormats
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val stringIndexerModelPath = new Path(path, s"stringIndexerModel").toString
      val stringIndexerModel: StringIndexerModel = DefaultParamsReader.loadParamsInstance[StringIndexerModel](stringIndexerModelPath, sc)

      val topKIntents = new TopKIntents(metadata.uid)
      metadata.getAndSetParams(topKIntents)

      topKIntents.setStringIndexerModel(stringIndexerModel)
    }
  }
}