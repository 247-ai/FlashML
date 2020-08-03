package com.tfs.flashml.publish.preprocessing

import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigValues, PublishUtils}

/**
 * Class for publishing JS code for stop words.
 *
 * @since 3/28/17.
 */
object StopWordsProcessorPublisher
{
    def generateJS(words: Array[String], input: String, output: String, stopWordsFunc: Boolean, pattern: String) =
    {
        val stopWordsJs = if (stopWordsFunc) stopWordsFuncJS(pattern)
        else new StringBuilder
        stopWordsJs ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "var " +
          "stopWordsArray_" + output + " = "

        stopWordsJs ++= words.mkString("[\"", "\",\"", "\"];")
        stopWordsJs ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "var " +
          output + " = stopWordsRemover(" + input
        stopWordsJs ++= ",stopWordsArray_" + output + ");"
    }

    def stopWordsFuncJS(pattern: String) =
    {
        val stopWordsString = new StringBuilder
        stopWordsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "function " +
          "stopWordsRemover(line,stopWordsArr){"
        stopWordsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 2) + "var " +
          "termArr = line.replace(/\\\"/g,\"\\\\\\\"\")"
        stopWordsString ++= ".split(/"
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
        stopWordsString ++= javaPattern.substring(2)
        stopWordsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 2) + "while" +
          "(stopWordsArr.length){"
        stopWordsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 3) + "var " +
          "stopWord = stopWordsArr.pop();"
        stopWordsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 3) + "while" +
          "(termArr.indexOf(stopWord) != -1){"
        stopWordsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 4) + "termArr" +
          ".splice(termArr.indexOf(stopWord),1);"
        stopWordsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 3) + "}"
        stopWordsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 2) + "}"
        stopWordsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 2) + "return " +
          "termArr.join(\"" + FlashMLConstants.CUSTOM_DELIMITER + "\");"
        stopWordsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "}"
    }

}
