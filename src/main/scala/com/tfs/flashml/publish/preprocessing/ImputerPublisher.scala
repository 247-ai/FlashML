package com.tfs.flashml.publish.preprocessing

import com.tfs.flashml.util.{ConfigUtils, PublishUtils}

import scala.collection.mutable

object ImputerPublisher
{

    //Generates the imputer JS function
    def generateJS(replaceValue: String, inputCol: String, imputerFunction: Boolean, globalVar: mutable.Set[String]) =
    {
        val imputerJStmp = if (imputerFunction) imputerFunctionJS(replaceValue)
        else new StringBuilder
        globalVar += ("" ++ imputerJStmp)
        val imputerJS = new StringBuilder
        imputerJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + s"$inputCol = " +
                s"nullChecker( $inputCol,'$replaceValue');"
        imputerJS
    }

    def imputerFunctionJS(replaceValue: String) =
    {

        val imputerFunc = new StringBuilder
        imputerFunc ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "function nullChecker(colValue," +
                "replaceValue){"
        imputerFunc ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "if(colValue){"
        imputerFunc ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "return colValue;"
        imputerFunc ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "} else {"
        imputerFunc ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "if(colValue === ''){"
        imputerFunc ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "return colValue;"
        imputerFunc ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "} else {"
        imputerFunc ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "return replaceValue;"
        imputerFunc ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "}"
        imputerFunc ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
        imputerFunc ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "}"
        imputerFunc
    }
}
