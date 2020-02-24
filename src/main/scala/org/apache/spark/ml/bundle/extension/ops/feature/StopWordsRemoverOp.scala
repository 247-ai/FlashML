package org.apache.spark.ml.bundle.extension.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.feature.StopWordsRemoverCustom

/**
  * Serializer for stop words remover transformer
  */
class StopWordsRemoverOp extends SimpleSparkOp[StopWordsRemoverCustom] {
  override val Model: OpModel[SparkBundleContext, StopWordsRemoverCustom] = new OpModel[SparkBundleContext,StopWordsRemoverCustom] {
    override val klazz: Class[StopWordsRemoverCustom] = classOf[StopWordsRemoverCustom]

    override def opName: String = "stopwords_remove"

    override def store(model: Model, obj: StopWordsRemoverCustom)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val stopwords = obj.getStopWords.toSeq
      val pattern = obj.getDelimiter

      model.withValue("stopwords", Value.stringList(stopwords))
        .withValue("pattern",Value.string(pattern))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): StopWordsRemoverCustom = { new StopWordsRemoverCustom(uid = "") }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: StopWordsRemoverCustom): StopWordsRemoverCustom = {
    new StopWordsRemoverCustom(uid = uid)
  }

  override def sparkInputs(obj: StopWordsRemoverCustom): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: StopWordsRemoverCustom): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
