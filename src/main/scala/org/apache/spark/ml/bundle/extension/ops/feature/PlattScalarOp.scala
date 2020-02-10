package org.apache.spark.ml.bundle.extension.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionWithDoubleResponseModel, PlattScalarModel}

/**
  * Serialization of platt scaling to generate the probability column in case of svm prediction
  */
class PlattScalarOp extends SimpleSparkOp[PlattScalarModel] {
  override val Model: OpModel[SparkBundleContext, PlattScalarModel] = new OpModel[SparkBundleContext, PlattScalarModel] {
    override val klazz: Class[PlattScalarModel] = classOf[PlattScalarModel]

    override def opName: String = "platt_scalar"

    override def store(model: Model, obj: PlattScalarModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {

      var m = model.withValue("num_classes", Value.long(obj.models.length))
        .withValue("num_features", Value.long(obj.models.head.numFeatures))

      obj.models.zipWithIndex.foreach {
        case (x, i) => m = m.withValue(s"coefficients$i",Value.vector(x.coefficients.toArray))
          .withValue(s"intercept$i",Value.double(x.intercept))
      }
      m
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): PlattScalarModel = {
      val numClasses = model.value("num_classes").getLong.toInt

      var labelMetadata = NominalAttribute.defaultAttr.
        withName("prediction").
        withName("probability")
      new PlattScalarModel(uid = "",models = Array[BinaryLogisticRegressionWithDoubleResponseModel]())
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: PlattScalarModel): PlattScalarModel = {
    var labelMetadata = NominalAttribute.defaultAttr.
      withName(shape.output("prediction").name).
      withName(shape.output("probability").name).
      withNumValues(model.models.length)
    new PlattScalarModel(uid = uid, models = model.models)
  }

  override def sparkInputs(obj: PlattScalarModel): Seq[ParamSpec] = {
    Seq("rawPrediction" -> obj.rawPredictionCol)
  }

  override def sparkOutputs(obj: PlattScalarModel): Seq[SimpleParamSpec] = {
      Seq("probability" -> obj.probabilityCol,
      "prediction" -> obj.predictionCol)
  }
}

