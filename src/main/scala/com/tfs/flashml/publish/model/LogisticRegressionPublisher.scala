package com.tfs.flashml.publish.model

import java.text.DecimalFormat

import com.tfs.flashml.publish.model.SVMPublisher.doubleToString
import com.tfs.flashml.util.{ConfigValues, PublishUtils}
import org.apache.spark.ml.linalg.Vector

import scala.collection.mutable.ArrayBuffer

object LogisticRegressionPublisher {

  def generateJS(coefficients:Vector,intercept:Double,featureCol:String, idx:Int = -1) : StringBuilder = {

    val js =new StringBuilder
    if(idx == -1){
      js ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "var intercept = " + intercept  + ";"
    }else{
      js ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "var intercept_"+ idx+ " = " + intercept  + ";"
    }
    js ++= createCoefficientsMap(coefficients.toArray, idx)
    //js ++= generateDotProductString
    //js ++= generateProbabilityString
    //js ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "return score;"
    js
  }

  def createCoefficientsMap(coefficientsArray: Array[Double], idx:Int) : StringBuilder ={

    val mapString = new StringBuilder
    val mapArray = new ArrayBuffer[String]()
    var formatString = s"#.${"#" * ModelPublisher.decimalPrecision}"
    if(idx == -1){
      mapString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "var coefficients = { "
    }else{
      mapString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "var coefficients_" + idx + " = { "
    }

    for((value,index) <- coefficientsArray.zipWithIndex){
      if (value != 0)
      mapArray += PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 2) + index + PublishUtils.indent(2) + ": " + doubleToString(value, formatString).toDouble
    }
    mapString ++= mapArray.mkString(",")
    mapString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "};"
    mapString
  }

  def generateDotProductString : StringBuilder ={
    val dpString = new StringBuilder
    dpString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var dotProduct = 0;"
    dpString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "for (var key in features){"
    dpString ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "if (coefficients[key] != undefined){"
    dpString ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "dotProduct += coefficients[key]*features[key];"
    dpString ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "}"
    dpString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    dpString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "dotProduct += intercept;"
    dpString
  }

  def generateProbabilityString : StringBuilder = {
    if (ConfigValues.isUplift) {
      var probString = new StringBuilder
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var score_0 = 1.0/(1+Math.exp(-dotProduct));"
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "if(coefficients[shiftKey] != undefined){"
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "dotProduct += coefficients[shiftKey];"
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var score_1 = 1.0/(1+Math.exp(-dotProduct));"
      probString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var score = score_1 - score_0;"
      probString
    }
    else {
      new StringBuilder(PublishUtils.getNewLine + PublishUtils.indent(2) + "var score = 1.0/(1+Math.exp(-dotProduct));")
    }
  }

  def doubleToString(value: Double, formatString:String): String =
  {
    val formatter = new DecimalFormat(formatString)
    formatter.format(value)
  }

}


