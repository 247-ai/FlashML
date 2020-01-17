package com.tfs.flashml.publish.preprocessing

import com.tfs.flashml.util.{ConfigUtils, PublishUtils}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object CaseNormalizer
{

    private val log = LoggerFactory.getLogger(getClass)

    def generateJS(input: String, output: String, caseNormFunc: Boolean, globalVar: mutable.Set[String]) =
    {
        val caseNormJsTmp = if (caseNormFunc) caseNormFuncJS
        else new StringBuilder
        globalVar += ("" + caseNormJsTmp)
        val caseNormJs = new StringBuilder
        caseNormJs ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var " + output + " = caseNormalizer(" + input + ");"
        //caseNormJs ++= ",caseNormArray_" + output + ");"
        caseNormJs
    }

    def caseNormFuncJS =
    {
        val caseNormString = new StringBuilder
        caseNormString ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "function caseNormalizer(line){"
        caseNormString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "return line.toLowerCase();"
        /*caseNormString ++= PublishUtils.getNewLine +PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "for(var i
         =0;i < tempArr.length;i++){"
        caseNormString ++= PublishUtils.getNewLine +PublishUtils.indent(ConfigUtils.defaultIndent + 3) + "normArr
        .push(tempArr[i].toLowerCase());"
        caseNormString ++= PublishUtils.getNewLine +PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "}"*/
        //caseNormString ++= PublishUtils.getNewLine +PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "return
      // normArr"
        caseNormString ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "}"
        caseNormString
    }

}
