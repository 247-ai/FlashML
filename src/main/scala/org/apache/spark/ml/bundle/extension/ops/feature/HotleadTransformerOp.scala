package org.apache.spark.ml.bundle.extension.ops.feature

import com.tfs.flashml.publish.transformer.HotleadTransformer
import ml.bundle.DataShape
import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import ml.combust.mleap.core.types
import org.apache.spark.ml.bundle._
import org.apache.spark.sql.mleap.TypeConvertersCustom._
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import org.apache.spark.ml.linalg.{Matrix, MatrixUDT, Vector, VectorUDT}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.util.Try

/**
 * Serializer for gram assembler to make it running in mleap platform.
 */
class HotleadTransformerOp extends SimpleSparkOp[HotleadTransformer] {
  override val Model: OpModel[SparkBundleContext, HotleadTransformer] = new OpModel[SparkBundleContext, HotleadTransformer] {
    override val klazz: Class[HotleadTransformer] = classOf[HotleadTransformer]

    override def opName: String = "hotlead_predictor"

    override def store(model: Model, obj: HotleadTransformer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val dataset = context.context.dataset.get
      //serializing the input shapes to get the schema of all the columns of the dataset and make it available in execution engine.
      val inputShapes = obj.getInputCols.map(i => sparkToMleapDataShape(dataset.schema(i),dataset): DataShape)
      val thresholds = obj.getThreshold
      val topThresholds = obj.getTopThresholds
      val nPages = obj.getNPages

      model.withValue("input_shapes", Value.dataShapeList(inputShapes))
        .withValue("thresholds",Value.doubleList(thresholds))
        .withValue("topThresholds",Value.doubleList(topThresholds))
        .withValue("nPages",Value.int(nPages))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): HotleadTransformer = { new HotleadTransformer(uid = "") }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: HotleadTransformer): HotleadTransformer = {
    new HotleadTransformer(uid = uid)
  }

  override def sparkInputs(obj: HotleadTransformer): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCols)
  }

  override def sparkOutputs(obj: HotleadTransformer): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}