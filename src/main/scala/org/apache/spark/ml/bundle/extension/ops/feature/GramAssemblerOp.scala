package org.apache.spark.ml.bundle.extension.ops.feature

import com.tfs.flashml.core.featuregeneration.transformer.GramAssembler
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
class GramAssemblerOp extends SimpleSparkOp[GramAssembler] {
  override val Model: OpModel[SparkBundleContext, GramAssembler] = new OpModel[SparkBundleContext, GramAssembler] {
    override val klazz: Class[GramAssembler] = classOf[GramAssembler]

    override def opName: String = "gram_assembler"

    override def store(model: Model, obj: GramAssembler)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val dataset = context.context.dataset.get
      //serializing the input shapes to get the schema of all the columns of the dataset and make it available in execution engine.
      val inputShapes = obj.getInputCols.map(i => sparkToMleapDataShape(dataset.schema(i),dataset): DataShape)

      model.withValue("input_shapes", Value.dataShapeList(inputShapes))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): GramAssembler = { new GramAssembler(uid = "") }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: GramAssembler): GramAssembler = {
    new GramAssembler(uid = uid)
  }

  override def sparkInputs(obj: GramAssembler): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCols)
  }

  override def sparkOutputs(obj: GramAssembler): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}