package org.apache.spark.ml.bundle.extension.ops.feature

import com.tfs.flashml.core.preprocessing.transformer.WordSubstitutionTransformer
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle._

/**
  * Serialization of spark custom transformer to excute it in the mleap platform. This class serializes the word substitutor
  * which substitutes the word with given value.
  */
class WordSubstitutionOp extends SimpleSparkOp[WordSubstitutionTransformer] {
  override val Model: OpModel[SparkBundleContext, WordSubstitutionTransformer] = new OpModel[SparkBundleContext,WordSubstitutionTransformer] {
    override val klazz: Class[WordSubstitutionTransformer] = classOf[WordSubstitutionTransformer]

    override def opName: String = "word_substitute"

    override def store(model: Model, obj: WordSubstitutionTransformer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val (labels, values) = obj.getDictonary.toSeq.unzip
      val pattern = obj.getDelimiter

      // add the key and values to the Bundle model that
      // will be serialized to our MLeap bundle
      model.withValue("labels", Value.stringList(labels)).
        withValue("values", Value.stringList(values)).
        withValue("pattern",Value.string(pattern))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): WordSubstitutionTransformer = { new WordSubstitutionTransformer(uid = "") }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: WordSubstitutionTransformer): WordSubstitutionTransformer = {
    new WordSubstitutionTransformer(uid = uid)
  }

  override def sparkInputs(obj: WordSubstitutionTransformer): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: WordSubstitutionTransformer): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
