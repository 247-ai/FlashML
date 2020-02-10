package org.apache.spark.ml.bundle.extension.ops.feature

import ml.bundle.DataShape
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.feature.ImputerCustom
import org.apache.spark.sql.mleap.TypeConvertersCustom._

/**
  * Serializer for imputer to make it running in mleap platform.
  */
class ImputerCustomOp extends SimpleSparkOp[ImputerCustom] {
  override val Model: OpModel[SparkBundleContext, ImputerCustom] = new OpModel[SparkBundleContext, ImputerCustom] {
    override val klazz: Class[ImputerCustom] = classOf[ImputerCustom]

    override def opName: String = "imputercustom"

    override def store(model: Model, obj: ImputerCustom)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val dataset = context.context.dataset.get
      val inputSchema = dataset.schema(obj.getInputCol).dataType.toString
      val inputShape = sparkToMleapDataShape(dataset.schema(obj.getInputCol),dataset):DataShape

      model.withValue("input_shape", Value.dataShape(inputShape.withIsNullable(true)))
        .withValue("input_type", Value.string(inputSchema))
        .withValue("surrogate_value", Value.string(obj.getReplacementValue))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): ImputerCustom = {
      val surrogateValue = model.value("surrogate_value").getString

      new ImputerCustom(uid = "").setReplacementValue(surrogateValue)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: ImputerCustom): ImputerCustom = {
    new ImputerCustom(uid = uid).setReplacementValue(model.getReplacementValue)
  }

  override def sparkInputs(obj: ImputerCustom): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: ImputerCustom): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
