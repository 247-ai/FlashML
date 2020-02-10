package org.apache.spark.ml.bundle.extensions.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.ModelSerializer
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.classification.ClassificationModel
import org.apache.spark.ml.classification.OneVsRestCustomModel

import scala.collection.mutable.ArrayBuffer

/**
  * Serialization of OVR custom which gives accumulated rawprediction and probability which is not happening in actual OVR
  */
class OneVsRestCustomOp extends SimpleSparkOp[OneVsRestCustomModel] {
  override val Model: OpModel[SparkBundleContext, OneVsRestCustomModel] = new OpModel[SparkBundleContext, OneVsRestCustomModel] {
    override val klazz: Class[OneVsRestCustomModel] = classOf[OneVsRestCustomModel]

    override def opName: String = "ovr_custom"

    override def store(model: Model, obj: OneVsRestCustomModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      var i = 0
      for(cModel <- obj.models) {
        val name = s"model$i"
        ModelSerializer(context.bundleContext(name)).write(cModel)
        i = i + 1
        name
      }

      model.withValue("num_classes", Value.long(obj.models.length))
        .withValue("num_features", Value.long(obj.models.head.numFeatures))
        .withValue("isProbModel",Value.boolean(obj.isProbModel))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): OneVsRestCustomModel = {
      val numClasses = model.value("num_classes").getLong.toInt
      val isProbModel = model.value("isProbModel").getBoolean

      val models = (0 until numClasses).toArray.map {
        i => ModelSerializer(context.bundleContext(s"model$i")).read().get.asInstanceOf[ClassificationModel[_, _]]
      }

      var labelMetadata = NominalAttribute.defaultAttr.
        withName("prediction").
        withName("rawPrediction").
        withNumValues(models.length)
      if(isProbModel) labelMetadata = labelMetadata.withName("probability")
      new OneVsRestCustomModel(uid = "", models = models, labelMetadata = labelMetadata.toMetadata,isProbModel=isProbModel)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: OneVsRestCustomModel): OneVsRestCustomModel = {
    var labelMetadata = NominalAttribute.defaultAttr.
      withName(shape.output("prediction").name).
      withName(shape.output("rawPrediction").name).
      withNumValues(model.models.length)
    if(model.isProbModel)
      labelMetadata = labelMetadata.withName(shape.output("probability").name)
    new OneVsRestCustomModel(uid = uid, models = model.models, labelMetadata = labelMetadata.toMetadata(),isProbModel=model.isProbModel)
  }

  override def sparkInputs(obj: OneVsRestCustomModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: OneVsRestCustomModel): Seq[SimpleParamSpec] = {
    if(obj.isProbModel)
      Seq("rawPrediction" -> obj.rawPredictionCol,
        "probability" -> obj.probabilityCol,
        "prediction"-> obj.predictionCol)
    else
      Seq("rawPrediction" -> obj.rawPredictionCol,
        "prediction"-> obj.predictionCol)
  }
}

