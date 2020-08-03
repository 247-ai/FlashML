package com.tfs.flashml.publish.featureengineering

import com.tfs.flashml.util.{FlashMLConfig, ConfigValues, PublishUtils}
import com.tfs.flashml.util.conf.FlashMLConstants
import scala.collection.JavaConverters._

object VectorAssemblerPublisher
{

  //This variable is the collection of all numeric variables in a 1D array
  //Flattened if applicable
  val numericFeatures = ConfigValues.allNumericVariables.asInstanceOf[Array[String]]

  val upliftTreatmentVariable: String = ConfigValues.upliftColumn

  def generateJS(inputColumns: Array[String], output: String): StringBuilder =
  {

    var vectorAssemblerJS = new StringBuilder
    vectorAssemblerJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "var " +
      output + " = " + inputColumns.head + ";"
    vectorAssemblerJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "var " +
      "shiftKey = size_" + inputColumns.head + ";"
    for (col <- inputColumns.tail)
    {
      if (!numericFeatures.contains(col) && upliftTreatmentVariable != col)
      {
        vectorAssemblerJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) +
          "for(var key in " + col + "){"
        vectorAssemblerJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 2) +
          output + "[parseInt(key) + shiftKey] = "
        vectorAssemblerJS ++= col + "[key];"
        vectorAssemblerJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "}"
        vectorAssemblerJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) +
          "shiftKey = shiftKey + size_" + col + ";"
      }
      else if (numericFeatures.contains(col))
      {
        vectorAssemblerJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) +
          output + "[shiftKey] = " + col + ";"

        vectorAssemblerJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) +
          "shiftKey = shiftKey + 1;"
      }
      else
      {
        vectorAssemblerJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + output + "[shiftKey] = 0;"
      }
    }
    vectorAssemblerJS
  }

}
