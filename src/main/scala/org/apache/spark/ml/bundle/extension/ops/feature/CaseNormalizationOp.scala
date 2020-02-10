package org.apache.spark.ml.bundle.extension.ops.feature

import com.tfs.flashml.core.preprocessing.transformer.CaseNormalizationTransformer
import org.apache.spark.ml.bundle.SparkBundleContext

/**
  * Serializer for case normalization to run in mleap platform
  */
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle._

class CaseNormalizationOp extends SimpleSparkOp[CaseNormalizationTransformer] {
  override val Model: OpModel[SparkBundleContext, CaseNormalizationTransformer] = new OpModel[SparkBundleContext,CaseNormalizationTransformer] {
    override val klazz: Class[CaseNormalizationTransformer] = classOf[CaseNormalizationTransformer]

    override def opName: String = "case_norm"

    override def store(model: Model, obj: CaseNormalizationTransformer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      // add the labels and values to the Bundle model that
      // will be serialized to our MLeap bundle
      model
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): CaseNormalizationTransformer = { new CaseNormalizationTransformer(uid = "") }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: CaseNormalizationTransformer): CaseNormalizationTransformer = {
    new CaseNormalizationTransformer(uid = uid)
  }

  override def sparkInputs(obj: CaseNormalizationTransformer): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: CaseNormalizationTransformer): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
