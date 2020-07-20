package org.apache.spark.ml.bundle.extension.ops.feature

import com.tfs.flashml.util.ConfigValues
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.classification.{LinearSVCModel, LogisticRegressionModel, UpliftTransformer}
import org.apache.spark.ml.linalg.Vectors

/**
  * Serializer for uplift transformer to make it running in mleap platform.
  */
class UpliftTransformerOp extends SimpleSparkOp[UpliftTransformer] {
  override val Model: OpModel[SparkBundleContext, UpliftTransformer] = new OpModel[SparkBundleContext, UpliftTransformer] {
    override val klazz: Class[UpliftTransformer] = classOf[UpliftTransformer]

    override def opName: String = "uplift"

    override def store(model: Model, obj: UpliftTransformer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val (baseCoefficients,baseIntercept) = obj.getBaseClassifier match{
        case lr:LogisticRegressionModel => (lr.coefficients,lr.intercept)
        case svm:LinearSVCModel => (svm.coefficients,svm.intercept)
        case _ => throw new Exception("Uplift is supported only for binomial cases")
      }
      val (plattCoefficients,plattIntercept) = if(obj.getBaseClassifier.isInstanceOf[LinearSVCModel] && ConfigValues.plattScalingEnabled)
      (obj.getPlattScaler.models(0).coefficients,obj.getPlattScaler.models(0).intercept) else (Vectors.dense(Array(0.0)),0.0)
      val resultModel = model.withValue("baseCoefficients",Value.vector(baseCoefficients.toArray))
        .withValue("baseIntercept",Value.double(baseIntercept))
      if(obj.getBaseClassifier.isInstanceOf[LinearSVCModel] && ConfigValues.plattScalingEnabled)
        resultModel.withValue("plattCoefficients",Value.vector(plattCoefficients.toArray))
        .withValue("plattIntercept",Value.double(plattIntercept))
      resultModel
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): UpliftTransformer = { new UpliftTransformer(uid = "") }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: UpliftTransformer): UpliftTransformer = {
    new UpliftTransformer(uid = uid)
  }

  override def sparkInputs(obj: UpliftTransformer): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: UpliftTransformer): Seq[SimpleParamSpec] = {
    Seq("probability" -> obj.probabilityCol)
  }
}

