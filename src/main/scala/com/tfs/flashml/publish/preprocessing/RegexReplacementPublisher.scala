package com.tfs.flashml.publish.preprocessing

import com.tfs.flashml.util.PublishUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable

package object RegexReplacementPublisher
{

  private val log = LoggerFactory.getLogger(getClass)
  private val functionName = "regexProcessor"
  private val regexPatternArg = "_regexPatternArg"

  def generateJS(inputCol: String, outputCol: String, regexes: Array[(String, String)], globalVar: mutable
  .Set[String]): StringBuilder =
  {
    val res = new StringBuilder
    //include regex segment
    //GlobalVar includes only required functions (as a collection - set of Strings) to the JS
    globalVar += ("" + generateRegexReplJS)
    res ++= includeRegex(inputCol, regexes)
    res ++= PublishUtils.getNewLine + PublishUtils.indent(1)
    res ++= s"var $outputCol = $functionName($inputCol,$inputCol$regexPatternArg);"
    res
  }

  private def generateRegexReplJS: StringBuilder =
  {
    val res = new StringBuilder
    res ++= PublishUtils.getNewLine + PublishUtils.indent(1) + s"function $functionName(inputString,regexes){"
    res ++= PublishUtils.getNewLine + PublishUtils.indent(2) + s"var ignoreCasePattern=" + "\"" + "(" + "?i:" +
      ")" + "\";"
    res ++= PublishUtils.getNewLine + PublishUtils.indent(2) + s"var iCPlength = ignoreCasePattern.length;"
    res ++= PublishUtils.getNewLine + PublishUtils.indent(2) + s"for(i=0;i<regexes.length;i++){"
    res ++= PublishUtils.getNewLine + PublishUtils.indent(3) + s"var pos = regexes[i][1].indexOf" +
      s"(ignoreCasePattern);"
    res ++= PublishUtils.getNewLine + PublishUtils.indent(3) + s"if(pos == 0 ) "
    res ++= PublishUtils.getNewLine + PublishUtils.indent(4) + s"inputString = inputString.replace(new RegExp" +
      s"(regexes[i][1].slice(iCPlength),'gi'),regexes[i][0]);"
    res ++= PublishUtils.getNewLine + PublishUtils.indent(3) + s"else"
    res ++= PublishUtils.getNewLine + PublishUtils.indent(4) + s"inputString = inputString.replace(new RegExp" +
      s"(regexes[i][1],'g'),regexes[i][0]);"
    res ++= PublishUtils.getNewLine + PublishUtils.indent(2) + s"}"
    res ++= PublishUtils.getNewLine + PublishUtils.indent(2) + s"return(inputString);"
    res ++= PublishUtils.getNewLine + PublishUtils.indent(1) + s"}"
    res
  }

  def includeRegex(inputCol: String, regexes: Array[(String, String)]): StringBuilder =
  {
    val res = new StringBuilder
    res ++= PublishUtils.getNewLine + PublishUtils.indent(1) + s"var $inputCol$regexPatternArg = ["

    regexes
      .zipWithIndex
      .foreach(obj =>
      {
        val reg = obj._1
        val pos = obj._2
        res ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "[" + "\"" + reg._1 + "\"" + "," + "\"" + reg._2 + "\"" + "]"
        if (pos != regexes.length - 1) res ++= ","
      })
    res ++= "];"

  }

}
