package com.tfs.flashml.publish.preprocessing

import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigUtils, PublishUtils}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * Class for publishing JS code for contractions replacement.
 *
 * @since 3/28/17.
 */
object ContractionReplacementPublisher
{
  private val log = LoggerFactory.getLogger(getClass)

  def generateJS(expansionMap: Array[String], input: String, output: String, contReplFunction: Boolean,
                 pattern: String) =
  {
    val contReplJs = if (contReplFunction) contReplFunctionJS(pattern)
    else new StringBuilder
    contReplJs ++= expansionMapJS(expansionMap)
    contReplJs ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var " + output + " = contractionReplacement("
    contReplJs ++= input + ",expansionsMap);"
    contReplJs
  }

  def contReplFunctionJS(pattern: String) =
  {
    val contReplString = new StringBuilder
    contReplString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "function " +
      "contractionReplacement(line,expansionMap){"
    contReplString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "var " +
      "termArr = line.replace(/\\\"/g,\"\\\\\\\"\")"
    contReplString ++= ".split(/"
    val javaPattern = pattern.stripPrefix("[").stripSuffix("]").replace("//", "\\/\\/").split('|').fold("")(
      (accumulator, subPattern) =>
      {
        if (subPattern.contains("(") || subPattern.contains(")"))
        {
          val openCount = subPattern.count(_ == '(')
          val closeCount = subPattern.count(_ == ')')
          if (openCount == closeCount)
          {
            accumulator + "+|" + subPattern
          }
          else
            accumulator + "+|\\" + subPattern
        }
        else
          accumulator + "+|\\" + subPattern
      }) + "+/);"
    contReplString ++= javaPattern.substring(2)
    contReplString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "for (i in " +
      "termArr){"
    contReplString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 3) + "var update" +
      " = expansionsMap[termArr[i]]"
    contReplString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 3) + "if (update" +
      " != undefined){"
    contReplString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 4) + "termArr[i]" +
      " = update;"
    contReplString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 3) + "}"
    contReplString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "}"
    contReplString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "return " +
      "termArr.join(\"" + FlashMLConstants.CUSTOM_DELIMITER + "\");"
    contReplString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "}"
    contReplString
  }

  def expansionMapJS(expansionMap: Array[String]): StringBuilder =
  {
    val expansionsMapString = new StringBuilder
    val expansionsMapArray = new ArrayBuffer[String]()
    expansionsMapString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var " +
      "expansionsMap = {"
    for (i <- expansionMap)
    {
      expansionsMapArray += PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "\"" + i.split(",")(0) + "\" : \"" + i.split(",")(1) + "\""
    }
    expansionsMapString ++= expansionsMapArray.mkString(",")
    expansionsMapString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "}"

    expansionsMapString
  }

}
