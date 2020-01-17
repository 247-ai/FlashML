package com.tfs.flashml.publish.model

import com.tfs.flashml.core.ModelTrainingEngine
import com.tfs.flashml.core.modeltraining.ModelTrainingUtils.log
import com.tfs.flashml.util.{ConfigUtils, FlashMLConfig, PublishUtils}
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.spark.SparkException
import org.apache.spark.ml.classification.{LinearSVCModel, LogisticRegressionModel, OneVsRestCustomModel, PlattScalarModel}
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Class for publishing the models.
  */
object ModelPublisher {

  private val name = ConfigUtils.mlAlgorithm.toLowerCase
  private val nPages = FlashMLConfig.getIntArray(FlashMLConstants.PUBLISH_PAGES)
  private val pageVariable = FlashMLConfig.getString(FlashMLConstants.PAGE_VARIABLE)
  private val thresholds = FlashMLConfig.getDoubleArray(FlashMLConstants.PUBLISH_THRESHOLDS)
  private val isMultiIntent = ConfigUtils.modelingMethod.contains(FlashMLConstants.EXPERIMENT_MODELING_METHOD_MULTI_INTENT)
  private val isOVR = FlashMLConfig.getString(FlashMLConstants.BUILD_TYPE).toLowerCase.equals(FlashMLConstants.BUILD_TYPE_OVR)
  private var pageNum = 0
  private val log = LoggerFactory.getLogger(getClass)

  private val defaultPrecision = 8
  //This option allows users to custom set the precision of the Co-efficients in the Publish scripts
  val decimalPrecision = if(FlashMLConfig.hasKey(FlashMLConstants.PUBLISH_PRECISION) && FlashMLConfig.getInt(FlashMLConstants.PUBLISH_PRECISION)!=0) FlashMLConfig.getInt(FlashMLConstants.PUBLISH_PRECISION) else defaultPrecision

  private def throwException(msg: String) =
  {
    log.error(msg)
    throw new SparkException(msg)
  }

  /**
    * Method to generate the JS code for each page.
    * @param pageNumber
    * @return
    */
  def generateJS(pageNumber: Int, globalVar:mutable.Set[String]): StringBuilder = {

    pageNum = pageNumber
    val modelJS = name match
    {
      case FlashMLConstants.LOGISTIC_REGRESSION if isMultiIntent && isOVR => generateOvrLogisticRegressionJS(globalVar)
      case FlashMLConstants.LOGISTIC_REGRESSION if isMultiIntent => generateMultinomialLogisticRegressionJS(globalVar)
      case FlashMLConstants.LOGISTIC_REGRESSION => generateBinaryLogisticRegressionJS(globalVar)
      case FlashMLConstants.SVM => generateBinarySVMJS(globalVar)
      case _ => throwException("$_ is unsupported in JS generation")

    }
    modelJS
  }

  def generateBinarySVMJS(globalVar:mutable.Set[String]): StringBuilder = {

    val pageString = if (pageNum == 0) "noPage" else "page" + pageNum
    val pipeline = ModelTrainingEngine.loadPipelineModel(pageNum)
    val svmModel = pipeline.stages(1).asInstanceOf[LinearSVCModel]

    val lrModel = pipeline
      .stages(2)
            .asInstanceOf[PlattScalarModel]
      .models(0)

    val svmCoefficients = svmModel.coefficients
    val svmIntercept = svmModel.intercept
    val svmFeaturesCol = svmModel.getFeaturesCol
    val lrCoefficients = lrModel.coefficients
    val lrIntercept = lrModel.intercept
    val binarySVMJS = new StringBuilder
    val binarySVMGlobalJS = new StringBuilder

    binarySVMGlobalJS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "function svmModel(svmIntercept, calibIntercept, svmCoefficients, calibCoefficients, features, shiftKey){"
    binarySVMGlobalJS ++= SVMPublisher.generateSVMDotProductString
    binarySVMGlobalJS ++= SVMPublisher.generateCalibratorDotProductString
    binarySVMGlobalJS ++= SVMPublisher.generateProbabilityString
    binarySVMGlobalJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "return score;"
    binarySVMGlobalJS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "}"
    globalVar+=(""+binarySVMGlobalJS)

    binarySVMJS ++= SVMPublisher.generateJS(svmCoefficients, svmIntercept, svmFeaturesCol, lrCoefficients, lrIntercept, globalVar)

    binarySVMJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var score = svmModel(svmIntercept, calibIntercept, svmCoefficients, calibCoefficients, "+ svmFeaturesCol+",shiftKey);"

    binarySVMJS ++= binaryModelActionJS
    binarySVMJS
  }

  def generateBinaryLogisticRegressionJS(globalVar:mutable.Set[String]): StringBuilder = {
    val pageString = if (pageNum == 0) "noPage" else "page" + pageNum

    val lrModel = ModelTrainingEngine.loadPipelineModel(pageNum).stages(1).asInstanceOf[LogisticRegressionModel]
    val coefficients = lrModel.coefficients
    val intercept = lrModel.intercept
    val featuresCol = lrModel.getFeaturesCol
    var binaryLrJS = new StringBuilder
    var binaryLrGlobalJS = new StringBuilder

    binaryLrGlobalJS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "function lrModel(intercept, coefficients, features, shiftKey){"
    binaryLrGlobalJS ++= LogisticRegressionPublisher.generateDotProductString
    binaryLrGlobalJS ++= LogisticRegressionPublisher.generateProbabilityString
    binaryLrGlobalJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "return score;"
    binaryLrGlobalJS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "}"
    globalVar+=(""+binaryLrGlobalJS)

    binaryLrJS ++= LogisticRegressionPublisher.generateJS(coefficients, intercept, featuresCol, -1)
    binaryLrJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var score = lrModel(intercept, coefficients, "+ featuresCol+ ", shiftKey);"

    binaryLrJS ++= binaryModelActionJS
    binaryLrJS
  }

  def generateOvrLogisticRegressionJS(globalVar:mutable.Set[String]): StringBuilder = {
    val pageString = if (pageNum == 0) "noPage" else "page" + pageNum
    val ovrLrJS = new StringBuilder(PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var scoreArray = [];")

    val lrModel = ModelTrainingEngine.loadPipelineModel(pageNum).stages(1).asInstanceOf[OneVsRestCustomModel]
    val featuresCol = lrModel.getFeaturesCol
    val models = lrModel.models
    var ovrLrGlobalJS = new StringBuilder

    ovrLrGlobalJS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "function lrModel(intercept, coefficients, features, shiftKey){"
    ovrLrGlobalJS ++= LogisticRegressionPublisher.generateDotProductString
    ovrLrGlobalJS ++= LogisticRegressionPublisher.generateProbabilityString
    ovrLrGlobalJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "return score;"
    ovrLrGlobalJS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "}"
    globalVar+=(""+ovrLrGlobalJS)

    for ((model, idx) <- models.zipWithIndex) {
      val coefficients = model.asInstanceOf[LogisticRegressionModel].coefficients
      val intercept = model.asInstanceOf[LogisticRegressionModel].intercept
      val modelJS = new StringBuilder
      //modelJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "function lrModel_" + pageString + "_" + idx + "(){"
      modelJS ++= LogisticRegressionPublisher.generateJS(coefficients, intercept, featuresCol, idx)
      //modelJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "}"
      modelJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "scoreArray[" + idx + "] = lrModel(intercept_"+idx+", coefficients_"+idx+", "+featuresCol+", shiftKey);"
      ovrLrJS ++= modelJS
    }
    ovrLrJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "maxScore = Math.max(...scoreArray);"
    ovrLrJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "index = scoreArray.indexOf(maxScore);"
    ovrLrJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "return index;"
    ovrLrJS
  }

  def generateMultinomialLogisticRegressionJS(globalVar:mutable.Set[String]): StringBuilder = {
    val pageString = if (pageNum == 0) "noPage" else "page" + pageNum
    val multinomialJS = new StringBuilder(PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var scoreArray = [];")
    var multinomialGlobalJS = new StringBuilder

    val lrModel = ModelTrainingEngine.loadPipelineModel(pageNum).stages(1).asInstanceOf[LogisticRegressionModel]
    val coefficientMatrix = lrModel.coefficientMatrix
    val interceptVector = lrModel.interceptVector
    val featuresCol = lrModel.getFeaturesCol

    multinomialGlobalJS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "function lrModel(intercept, coefficients, features,shiftKey){"
    multinomialGlobalJS ++= LogisticRegressionPublisher.generateDotProductString
    multinomialGlobalJS ++= LogisticRegressionPublisher.generateProbabilityString
    multinomialGlobalJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "return score;"
    multinomialGlobalJS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "}"
    globalVar+=(""+multinomialGlobalJS)

    var idx = 0
    for (i <- coefficientMatrix.rowIter) {
      val modelJS = new StringBuilder
      //modelJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "function lrModel_" + pageString + "_" + idx + "(intercept, coefficients, "+featuresCol+"){"
      modelJS ++= LogisticRegressionPublisher.generateJS(i, interceptVector(idx), featuresCol, idx)
      //modelJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "}"
      modelJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "scoreArray[" + idx + "] = lrModel(intercept_"+idx+", coefficients_"+idx+", "+featuresCol+",shiftKey);"
      multinomialJS ++= modelJS
      idx = idx + 1
    }
    multinomialJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "maxScore = Math.max(...scoreArray);"
    multinomialJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "index = scoreArray.indexOf(maxScore);"
    multinomialJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "return index;"
    multinomialJS
  }

  def binaryModelActionJS: StringBuilder = {
    val conditionsJS = new StringBuilder
    val conditionsList = ArrayBuffer[String]()
    if (pageNum == 0) {
      for ((p, t) <- nPages.zipAll(thresholds, 0, 0)) {
        conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var condition" + p + " = "
        conditionsJS ++= "(score >= " + t + " && " + pageVariable + (if (p.equals(nPages.max)) " >= "
        else " == ") + p + ");"
        conditionsList.append("condition" + p)
      }
    }
    else {
      conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var condition" + pageNum + " = "
      conditionsJS ++= "(score >= " + thresholds(pageNum - 1) + ");"
      conditionsList.append("condition" + pageNum)
    }
    //conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "function canOfferInvite()"
   // conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "{"
    //conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "if(" + conditionsList.mkString(" || ") + ")"
    //conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "{"
    //conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 3) + "return true;"
    //conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "}"
    //conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "else"
    //conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "{"
    //conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 3) + "return false;"
    //conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "}"
    //conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "}"
    conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var model_result = (" + conditionsList.mkString(" || ") + ")?true:false"
    conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "result['output_flag'] = model_result;"
    conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "result['score'] = score;"
    conditionsJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "return result;"
    conditionsJS
  }

}
