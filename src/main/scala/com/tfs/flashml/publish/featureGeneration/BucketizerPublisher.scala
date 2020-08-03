package com.tfs.flashml.publish.featureGeneration

import com.tfs.flashml.publish.preprocessing.StopWordsProcessorPublisher.stopWordsFuncJS
import com.tfs.flashml.util.{ConfigValues, PublishUtils}

object BucketizerPublisher {

  def generateJS(inputCol: String, outputCol:String, splits : Array[Double]): StringBuilder = {
    val bucketizerJS = /*getBucketizerJSFunction */ new StringBuilder
    bucketizerJS ++= PublishUtils.indent(ConfigValues.defaultIndent + 1) + "var " + outputCol + " = " + "binning(" + inputCol + "," + "[" + splits.dropRight(1).drop(1).mkString(",") + "]);"
  }

  def getBucketizerJSFunction : StringBuilder = {
    val bucketizerFunctionJS = new StringBuilder
    bucketizerFunctionJS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "function binning(inputValue,binningIntervals) {"
    bucketizerFunctionJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "for(var i = 0; i < binningIntervals.length; i++) {"
    bucketizerFunctionJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "if(inputValue < binningIntervals[i]) {"
    bucketizerFunctionJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "return String(i.toFixed(1));"
    bucketizerFunctionJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "}"
    bucketizerFunctionJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    bucketizerFunctionJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "return String(binningIntervals.length.toFixed(1));"
    bucketizerFunctionJS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "}" + PublishUtils.getNewLine
    bucketizerFunctionJS
  }

}
