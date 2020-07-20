package com.tfs.flashml.publish.preprocessing

import com.tfs.flashml.util.{ConfigValues, PublishUtils}

import scala.collection.mutable

/**
 * Class for publishing JS code for geenrating sentence marker.
 *
 * @since 3/28/17.
 */
object SentenceMarkerPublisher
{

  def generateJS(input: String, output: String, sentenceMarkerFunction: Boolean, globalVar: mutable.Set[String]) =
  {
    val sentMarkerJsTmp = if (sentenceMarkerFunction) sentenceMarkerFunctionJS
    else new StringBuilder
    globalVar += ("" + sentMarkerJsTmp)
    val sentMarkerJs = new StringBuilder
    sentMarkerJs ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "var " +
      output + " = sentenceMarker(" + input + ");"
    sentMarkerJs
  }

  def sentenceMarkerFunctionJS: StringBuilder =
  {
    val sentMarkFunc = new StringBuilder
    sentMarkFunc ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "function sentenceMarker(line){"
    sentMarkFunc ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "line = 'class_ss_' + line + '_class_se'"
    sentMarkFunc ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "return line"
    sentMarkFunc ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "}"
    sentMarkFunc
  }

}
