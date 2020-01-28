package com.tfs.flashml.publish.model

import java.text.DecimalFormat

import com.tfs.flashml.util.{ConfigUtils, PublishUtils}
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.spark.ml.linalg.Vector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SVMPublisher {

  def generateJS(svmCoefficients:Vector, svmIntercept:Double, svmFeatureCol:String, calibCoefficients:Vector, calibIntercept:Double, globalVar:mutable.Set[String]) : StringBuilder = {
    val js = new StringBuilder
    js ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var svmIntercept = " + svmIntercept  + ";"
    js ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var calibIntercept = " + calibIntercept  + ";"
    js ++= createSVMCoefficientsMap(svmCoefficients.toArray)
    js ++= createCalibCoefficientsMap(calibCoefficients.toArray)
    //js ++= generateSVMDotProductString(svmFeatureCol)
    //js ++= generateCalibratorDotProductString
    //js ++= generateProbabilityString
    //js ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "return score;"
    js
  }

  def createSVMCoefficientsMap(svmCoefficientsArray: Array[Double]) : StringBuilder ={
    val mapString = new StringBuilder
    val mapArray = new ArrayBuffer[String]()
    var formatString = s"#.${"#" * ModelPublisher.decimalPrecision}"
    mapString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var svmCoefficients = { "
    for((value,index) <- svmCoefficientsArray.zipWithIndex)
    {
      if (value != 0)
      {
        val coeffWithPrecision = if (!ModelPublisher.defaultPrecisionFlag) doubleToString(value,formatString).toDouble + value
        mapArray += PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + index + PublishUtils.indent(2) + ": " + coeffWithPrecision

      }
    }
    mapString ++= mapArray.mkString(",")
    mapString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "};"
    mapString
  }


  def createCalibCoefficientsMap(calibCoefficientsArray: Array[Double]) : StringBuilder = {

    val mapString = new StringBuilder
    val mapArray = new ArrayBuffer[String]()
    mapString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var calibCoefficients = { "
    for((value,index) <- calibCoefficientsArray.zipWithIndex){
      if (value != 0)
        mapArray += PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + index + PublishUtils.indent(2) + ": " + value
    }
    mapString ++= mapArray.mkString(",")
    mapString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "};"
    mapString
  }

  def generateSVMDotProductString() : StringBuilder ={
    val svmDpString = new StringBuilder
    svmDpString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var svmDotProduct = 0;"
    svmDpString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "for (var key in features){"
    svmDpString ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "if (svmCoefficients[key] != undefined){"
    svmDpString ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "svmDotProduct += svmCoefficients[key]*features[key];"
    svmDpString ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "}"
    svmDpString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    svmDpString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "svmDotProduct += svmIntercept;"
    svmDpString
  }

  def generateCalibratorDotProductString:StringBuilder ={
    val calibDpString = new StringBuilder
    calibDpString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "function calibrator(svmDotProduct){"
    calibDpString ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var calibDotProduct = 0;"
    calibDpString ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "calibDotProduct += calibCoefficients[0]*svmDotProduct"
    calibDpString ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "calibDotProduct += calibIntercept;"
    calibDpString ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "return calibDotProduct;"
    calibDpString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    calibDpString
  }

  def generateProbabilityString : StringBuilder = {
    if (ConfigUtils.modelingMethod.contains(FlashMLConstants.UPLIFT))
    {
      val probString = new StringBuilder
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var dotProduct = calibrator(svmDotProduct);"
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var score_0 = 1.0/(1+Math.exp(-dotProduct));"
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var newSvmDotProduct = svmDotProduct;"
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "if(svmCoefficients[shiftKey] != undefined){"
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "newSvmDotProduct = svmDotProduct + svmCoefficients[shiftKey];"
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "dotProduct = calibrator(newSvmDotProduct);"
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var score_1 = 1.0/(1+Math.exp(-dotProduct));"
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var score = score_1 - score_0;"
      probString
    }
    else
    {
      val probString = new StringBuilder
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var dotProduct = calibrator(svmDotProduct);"
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var score = 1.0/(1+Math.exp(-dotProduct));"
      probString
    }
  }

  def doubleToString(value: Double, formatString:String): String =
  {
    val formatter = new DecimalFormat(formatString)
    formatter.format(value)
  }

}
