package org.apache.spark.ml.bundle.extension.ops.feature

import com.tfs.flashml.core.featuregeneration.transformer.SkipGramGenerator
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}


/**
  * Serializer for skip gram to make it running in the mleap platform
  */
class SkipGramOp extends SimpleSparkOp[SkipGramGenerator] {
  override val Model: OpModel[SparkBundleContext, SkipGramGenerator] = new OpModel[SparkBundleContext, SkipGramGenerator] {
    override val klazz: Class[SkipGramGenerator] = classOf[SkipGramGenerator]

    override def opName: String = "skip_gram"

    override def store(model: Model, obj: SkipGramGenerator)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("windowSize", Value.int(obj.getWindowSize))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): SkipGramGenerator = {
      new SkipGramGenerator(uid = "").setWindowSize(model.value("windowSize").getInt)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: SkipGramGenerator): SkipGramGenerator = {
    new SkipGramGenerator(uid = uid)
  }

  override def sparkInputs(obj: SkipGramGenerator): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: SkipGramGenerator): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
