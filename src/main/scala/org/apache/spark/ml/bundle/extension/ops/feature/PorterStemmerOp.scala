package org.apache.spark.ml.bundle.extension.ops.feature

import com.tfs.flashml.core.preprocessing.transformer.PorterStemmingTransformer
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle._


/**
  * Serialization of spark custom transformer to excute it in the mleap platform. This class serializes the porter stemmer transformer which
  * stem the word to its root.
  */

class PorterStemmerOp extends SimpleSparkOp[PorterStemmingTransformer] {
  override val Model: OpModel[SparkBundleContext, PorterStemmingTransformer] = new OpModel[SparkBundleContext,PorterStemmingTransformer] {
    override val klazz: Class[PorterStemmingTransformer] = classOf[PorterStemmingTransformer]

    override def opName: String = "porter_stemmer"

    override def store(model: Model, obj: PorterStemmingTransformer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val exceptions = obj.getExceptions.toSeq
      val delimiter = obj.getDelimiter

      // add the key and values to the Bundle model that
      // will be serialized to our MLeap bundle
      model.withValue("exceptions", Value.stringList(exceptions)).
        withValue("delimiter", Value.string(delimiter))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): PorterStemmingTransformer = { new PorterStemmingTransformer(uid = "") }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: PorterStemmingTransformer): PorterStemmingTransformer = {
    new PorterStemmingTransformer(uid = uid)
  }

  override def sparkInputs(obj: PorterStemmingTransformer): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: PorterStemmingTransformer): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
