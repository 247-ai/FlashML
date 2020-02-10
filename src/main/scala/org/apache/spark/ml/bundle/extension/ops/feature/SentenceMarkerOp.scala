package org.apache.spark.ml.bundle.extension.ops.feature

import com.tfs.flashml.core.preprocessing.transformer.SentenceMarker
import org.apache.spark.ml.bundle.SparkBundleContext
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle._

/**
  * Serialization for sentence marker
  */
class SentenceMarkerOp extends SimpleSparkOp[SentenceMarker] {
  override val Model: OpModel[SparkBundleContext, SentenceMarker] = new OpModel[SparkBundleContext,SentenceMarker] {
    override val klazz: Class[SentenceMarker] = classOf[SentenceMarker]

    override def opName: String = "sentence_marker"

    override def store(model: Model, obj: SentenceMarker)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      // add the labels and values to the Bundle model that
      // will be serialized to our MLeap bundle
      model
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): SentenceMarker = { new SentenceMarker(uid = "") }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: SentenceMarker): SentenceMarker = {
    new SentenceMarker(uid = uid)
  }

  override def sparkInputs(obj: SentenceMarker): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: SentenceMarker): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
