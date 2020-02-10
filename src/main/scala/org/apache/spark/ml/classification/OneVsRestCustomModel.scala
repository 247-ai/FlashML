/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE parameter distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this parameter to You under the Apache License, Version 2.0
 * (the "License"); you may not use this parameter except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This is a customization of spark.ml OneVsRest classifer to provide raw scores of all
 * the k models.
 */

package org.apache.spark.ml.classification

import java.util.UUID

import ml.combust.mleap.core.classification.ProbabilisticClassificationModel

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.language.existentials
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.json4s.{DefaultFormats, JObject, _}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.ml._
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.classification.{ClassificationModel, Classifier, LinearSVCModel}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.{Param, ParamMap, ParamPair, Params}
import org.apache.spark.ml.param.shared.{HasParallelism, HasProbabilityCol, HasWeightCol}
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ThreadUtils

private[ml] trait ClassifierTypeTrait {
  // scalastyle:off structural.type
  type ClassifierType = Classifier[F, E, M] forSome {
    type F
    type M <: ClassificationModel[F, M]
    type E <: Classifier[F, E, M]
  }
  // scalastyle:on structural.type
}

/**
  * Params for [[OneVsRestCustom]].
  */
private[ml] trait OneVsRestCustomParams extends PredictorParams
  with ClassifierTypeTrait with HasWeightCol {

  /**
    * param for the base binary classifier that we reduce multiclass classification into.
    * The base classifier input and output columns are ignored in favor of
    * the ones specified in [[OneVsRestCustom]].
    */
  val classifier: Param[ClassifierType] = new Param(this, "classifier", "base binary classifier")
  def getClassifier: ClassifierType = $(classifier)
  final val rawPredictionCol: Param[String] = new Param[String](this, "rawPredictionCol", "raw prediction column name")
  setDefault(rawPredictionCol, "rawPrediction")
  final def getRawPredictionCol: String = $(rawPredictionCol)
}

private[ml] object OneVsRestCustomParams extends ClassifierTypeTrait {

  def validateParams(instance: OneVsRestCustomParams): Unit = {
    def checkElement(elem: Params, name: String): Unit = elem match {
      case stage: MLWritable => // good
      case other =>
        throw new UnsupportedOperationException("OneVsRestCustom write will fail " +
          s" because it contains $name which does not implement MLWritable." +
          s" Non-Writable $name: ${other.uid} of type ${other.getClass}")
    }

    instance match {
      case ovrModel: OneVsRestCustomModel => ovrModel.models.foreach(checkElement(_, "model"))
      case _ => // no need to check OneVsRestCustom here
    }

    checkElement(instance.getClassifier, "classifier")
  }

  def saveImpl(
                path: String,
                instance: OneVsRestCustomParams,
                sc: SparkContext,
                extraMetadata: Option[JObject] = None): Unit = {

    val params = instance.extractParamMap().toSeq
    val jsonParams = render(params
      .filter { case ParamPair(p, v) => p.name != "classifier" }
      .map { case ParamPair(p, v) => p.name -> parse(p.jsonEncode(v)) }
      .toList)

    DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata, Some(jsonParams))

    val classifierPath = new Path(path, "classifier").toString
    instance.getClassifier.asInstanceOf[MLWritable].save(classifierPath)
  }

  def loadImpl(
                path: String,
                sc: SparkContext,
                expectedClassName: String): (DefaultParamsReader.Metadata, ClassifierType) = {

    val metadata = DefaultParamsReader.loadMetadata(path, sc, expectedClassName)
    val classifierPath = new Path(path, "classifier").toString
    val estimator = DefaultParamsReader.loadParamsInstance[ClassifierType](classifierPath, sc)
    (metadata, estimator)
  }
}

/**
  * Model produced by [[OneVsRestCustom]].
  * This stores the models resulting from training k binary classifiers: one for each class.
  * Each example is scored against all k models, and the model with the highest score
  * is picked to label the example.
  *
  * @param labelMetadata Metadata of label column if it exists, or Nominal attribute
  *                      representing the number of classes in training dataset otherwise.
  * @param models The binary classification models for the reduction.
  *               The i-th model is produced by testing the i-th class (taking label 1) vs the rest
  *               (taking label 0).
  */
final class OneVsRestCustomModel private[ml] (
                                               override val uid: String,
                                               private[ml] val labelMetadata: Metadata,
                                               val models: Array[_ <: ClassificationModel[_, _]],
                                               val isProbModel : Boolean)
  extends Model[OneVsRestCustomModel] with OneVsRestCustomParams with MLWritable with HasProbabilityCol {

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)


  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = false, getClassifier.featuresDataType)
  }


  override def transform(dataset: Dataset[_]): DataFrame = {
    // Check schema
    transformSchema(dataset.schema, logging = true)

    // determine the input columns: these need to be passed through
    val origCols = dataset.schema.map(f => col(f.name))

    // add an accumulator column to store predictions of all the models
    val accColName = "ovr" + $(rawPredictionCol)
    val probAccColName = "ovr" + ${probabilityCol}
    val initUDF = udf { () => Map[Int, Double]() }
    var newDataset = dataset.withColumn(accColName, initUDF())
    if(!models(0).isInstanceOf[LinearSVCModel])newDataset = newDataset.withColumn(probAccColName, initUDF())

    // persist if underlying dataset is not persistent.
    val handlePersistence = !dataset.isStreaming && dataset.storageLevel == StorageLevel.NONE
    if (handlePersistence) {
      newDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // update the accumulator column with the result of prediction of models
    val aggregatedDataset = models
      .zipWithIndex
      .foldLeft[DataFrame](newDataset) {
      case (df, (model, index)) =>

        val rawPredictionCol = model.getRawPredictionCol
        val probabilityCol = "probability"
        //if(df.schema.contains(probabilityCol))
        val columns = if(!models(0).isInstanceOf[LinearSVCModel]) origCols ++ List(col(rawPredictionCol), col(accColName), col(probabilityCol), col(probAccColName))
        else origCols ++ List(col(rawPredictionCol), col(accColName))

        // add temporary column to store intermediate scores and update
        val tmpColName = "mbc$tmp" + UUID.randomUUID().toString
        val probTmpColName = "mbc$prob" + UUID.randomUUID().toString
        val updateUDF = udf { (predictions: Map[Int, Double], prediction: Vector) =>
          predictions + ((index, prediction(1)))
        }

        model.setFeaturesCol($(featuresCol))
        val transformedDataset = model.transform(df).select( columns: _*)
        val updatedDataset = if(!model.isInstanceOf[LinearSVCModel]){
          transformedDataset
            .withColumn(tmpColName, updateUDF(col(accColName), col(rawPredictionCol)))
            .withColumn(probTmpColName, updateUDF(col(probAccColName),col(probabilityCol)))
        } else {
          transformedDataset
            .withColumn(tmpColName, updateUDF(col(accColName), col(rawPredictionCol)))
        }
        val newColumns = if(!model.isInstanceOf[LinearSVCModel]) origCols ++ List(col(tmpColName),col(probTmpColName))
        else origCols ++ List(col(tmpColName))

        // switch out the intermediate column with the accumulator column
        val intermediateDF = updatedDataset.select(newColumns: _*).withColumnRenamed(tmpColName, accColName)
        if(!model.isInstanceOf[LinearSVCModel])
          intermediateDF.withColumnRenamed(probTmpColName, probAccColName)
        else intermediateDF
    }

    if (handlePersistence) {
      newDataset.unpersist()
    }

    // output the index of the classifier with highest confidence as prediction
    val labelUDF = udf { predictions: Map[Int, Double] =>
      predictions.maxBy(_._2)._1.toDouble
    }

    // Convert Map of Scores to DenseVector for compatibility with Logistic Output
    val mapToVectorUDF = udf { predictions: Map[Int, Double] =>
      Vectors
        .dense(predictions
          .toSeq
          .sortBy(_._1)
          .map(_._2)
          .toArray[Double])
    }

    val resultDF = aggregatedDataset
      // output label and label metadata as prediction
      .withColumn($(predictionCol), labelUDF(col(accColName)), labelMetadata)
      .withColumn(accColName + "_tempVec", mapToVectorUDF(col(accColName)))
      .withColumnRenamed(accColName + "_tempVec", $(rawPredictionCol))
      .drop(accColName)
    if(!models(0).isInstanceOf[LinearSVCModel])
      resultDF.withColumn(probAccColName + "_tempVec", mapToVectorUDF(col(probAccColName)))
        .withColumnRenamed(probAccColName + "_tempVec", $(probabilityCol))
        .drop(probAccColName)
    else resultDF
  }


  override def copy(extra: ParamMap): OneVsRestCustomModel = {
    val copied = new OneVsRestCustomModel(
      uid, labelMetadata, models.map(_.copy(extra).asInstanceOf[ClassificationModel[_, _]]),!models(0).isInstanceOf[LinearSVCModel])
    copyValues(copied, extra).setParent(parent)
  }


  override def write: MLWriter = new OneVsRestCustomModel.OneVsRestCustomModelWriter(this)
}


object OneVsRestCustomModel extends MLReadable[OneVsRestCustomModel] {


  override def read: MLReader[OneVsRestCustomModel] = new OneVsRestCustomModelReader


  override def load(path: String): OneVsRestCustomModel = super.load(path)

  /** [[MLWriter]] instance for [[OneVsRestCustomModel]] */
  private[OneVsRestCustomModel] class OneVsRestCustomModelWriter(instance: OneVsRestCustomModel) extends MLWriter {

    OneVsRestCustomParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      val extraJson = ("labelMetadata" -> instance.labelMetadata.json) ~
        ("numClasses" -> instance.models.length)
      OneVsRestCustomParams.saveImpl(path, instance, sc, Some(extraJson))
      instance.models.map(_.asInstanceOf[MLWritable]).zipWithIndex.foreach { case (model, idx) =>
        val modelPath = new Path(path, s"model_$idx").toString
        model.save(modelPath)
      }
    }
  }

  private class OneVsRestCustomModelReader extends MLReader[OneVsRestCustomModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[OneVsRestCustomModel].getName

    override def load(path: String): OneVsRestCustomModel = {
      implicit val format = DefaultFormats
      val (metadata, classifier) = OneVsRestCustomParams.loadImpl(path, sc, className)
      val labelMetadata = Metadata.fromJson((metadata.metadata \ "labelMetadata").extract[String])
      val numClasses = (metadata.metadata \ "numClasses").extract[Int]
      val models = Range(0, numClasses).toArray.map { idx =>
        val modelPath = new Path(path, s"model_$idx").toString
        DefaultParamsReader.loadParamsInstance[ClassificationModel[_, _]](modelPath, sc)
      }
      val ovrModel = new OneVsRestCustomModel(metadata.uid, labelMetadata, models,!models(0).isInstanceOf[LinearSVCModel])
      metadata.getAndSetParams(ovrModel)
      ovrModel.set("classifier", classifier)
      ovrModel
    }
  }
}

/**
  * Reduction of Multiclass Classification to Binary Classification.
  * Performs reduction using one against all strategy.
  * For a multiclass classification with k classes, train k models (one per class).
  * Each example is scored against all k models and the model with highest score
  * is picked to label the example. <br />
  * <br />
  * This is a customization of spark.ml OneVsRest classifer to provide raw scores of all
  * the k models.
  */

final class OneVsRestCustom(override val uid: String)
  extends Estimator[OneVsRestCustomModel] with OneVsRestCustomParams with HasParallelism with HasProbabilityCol with MLWritable {

  private val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  def this() = this(Identifiable.randomUID("ovr_custom"))

  def setClassifier(value: Classifier[_, _, _]): this.type = {
    set(classifier, value.asInstanceOf[ClassifierType])
  }

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  /**
    * The implementation of parallel one vs. rest runs the classification for
    * each class in a separate threads.
    *
    */

  def setParallelism(value: Int): this.type = {
    set(parallelism, value)
  }

  /**
    * Sets the value of param [[weightCol]].
    *
    * This is ignored if weight is not supported by [[classifier]].
    * If this is not set or empty, we treat all instance weights as 1.0.
    * Default is not set, so all instances have weight one.
    *
    */

  def setWeightCol(value: String): this.type = set(weightCol, value)


  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true, getClassifier.featuresDataType)
  }


  override def fit(dataset: Dataset[_]): OneVsRestCustomModel = instrumented { instr =>
    transformSchema(dataset.schema)

    instr.logParams(this, labelCol, featuresCol, predictionCol, parallelism)
    instr.logNamedValue("classifier", $(classifier).getClass.getCanonicalName)

    // determine number of classes either from metadata if provided, or via computation.
    val labelSchema = dataset.schema($(labelCol))
    val computeNumClasses: () => Int = () => {
      val Row(maxLabelIndex: Double) = dataset.agg(max(col($(labelCol)).cast(DoubleType))).head()
      // classes are assumed to be numbered from 0,...,maxLabelIndex
      maxLabelIndex.toInt + 1
    }
    val numClasses = MetadataUtils.getNumClasses(labelSchema).fold(computeNumClasses())(identity)
    instr.logNumClasses(numClasses)

    val weightColIsUsed = isDefined(weightCol) && $(weightCol).nonEmpty && {
      getClassifier match {
        case _: HasWeightCol => true
        case c =>
          logWarning(s"weightCol is ignored, as it is not supported by $c now.")
          false
      }
    }

    val multiclassLabeled = if (weightColIsUsed) {
      dataset.select($(labelCol), $(featuresCol), $(weightCol))
    } else {
      dataset.select($(labelCol), $(featuresCol))
    }

    // persist if underlying dataset is not persistent.
    val handlePersistence = dataset.storageLevel == StorageLevel.NONE
    if (handlePersistence) {
      multiclassLabeled.persist(StorageLevel.MEMORY_AND_DISK)
    }

    val executionContext = getExecutionContext

    // create k columns, one for each binary classifier.
    import com.tfs.flashml.util.Implicits._
    val modelFutures = Range(0, numClasses).map { index =>
      // generate new label metadata for the binary problem.
      val newLabelMeta = BinaryAttribute.defaultAttr.withName("label").toMetadata()
      val labelColName = "classId$" + index
      val trainingDataset = multiclassLabeled.withColumn(
        labelColName, when(col($(labelCol)) === index.toDouble, 1.0).otherwise(0.0), newLabelMeta)
      val classifier = getClassifier
      val paramMap = new ParamMap()
      paramMap.put(classifier.labelCol -> labelColName)
      paramMap.put(classifier.featuresCol -> getFeaturesCol)
      paramMap.put(classifier.predictionCol -> getPredictionCol)
      Future {
        logger.info(s"Running OVR for class with index ${index + 1} of $numClasses, params: [${classifier.getClass.getSimpleName}/${classifier.paramMap.toSingleLineString}]")
        if (weightColIsUsed) {
          val classifier_ = classifier.asInstanceOf[ClassifierType with HasWeightCol]
          paramMap.put(classifier_.weightCol -> getWeightCol)
          classifier_.fit(trainingDataset, paramMap)
        } else {
          classifier.fit(trainingDataset, paramMap)
        }
      }(executionContext)
    }
    val models = modelFutures
      .map(ThreadUtils.awaitResult(_, Duration.Inf)).toArray[ClassificationModel[_, _]]
    instr.logNumFeatures(models.head.numFeatures)

    if (handlePersistence) {
      multiclassLabeled.unpersist()
    }

    // extract label metadata from label column if present, or create a nominal attribute
    // to output the number of labels
    val labelAttribute = Attribute.fromStructField(labelSchema) match {
      case _: NumericAttribute | UnresolvedAttribute =>
        NominalAttribute.defaultAttr.withName("label").withNumValues(numClasses)
      case attr: Attribute => attr
    }
    val model = new OneVsRestCustomModel(uid, labelAttribute.toMetadata(), models,!models(0).isInstanceOf[LinearSVCModel]).setParent(this)
    copyValues(model)
  }


  override def copy(extra: ParamMap): OneVsRestCustom = {
    val copied = defaultCopy(extra).asInstanceOf[OneVsRestCustom]
    if (isDefined(classifier)) {
      copied.setClassifier($(classifier).copy(extra))
    }
    copied
  }


  override def write: MLWriter = new OneVsRestCustom.OneVsRestWriter(this)
}


object OneVsRestCustom extends MLReadable[OneVsRestCustom] {


  override def read: MLReader[OneVsRestCustom] = new OneVsRestReader


  override def load(path: String): OneVsRestCustom = super.load(path)

  /** [[MLWriter]] instance for [[OneVsRestCustom]] */
  private[OneVsRestCustom] class OneVsRestWriter(instance: OneVsRestCustom) extends MLWriter {

    OneVsRestCustomParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      OneVsRestCustomParams.saveImpl(path, instance, sc)
    }
  }

  private class OneVsRestReader extends MLReader[OneVsRestCustom] {

    /** Checked against metadata when loading model */
    private val className = classOf[OneVsRestCustom].getName

    override def load(path: String): OneVsRestCustom = {
      val (metadata, classifier) = OneVsRestCustomParams.loadImpl(path, sc, className)
      val ovr = new OneVsRestCustom(metadata.uid)
      metadata.getAndSetParams(ovr)
      ovr.setClassifier(classifier)
    }
  }
}
