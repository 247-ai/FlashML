package com.tfs.flashml.publish.preprocessing

import com.tfs.flashml.util.{ConfigValues, PublishUtils}

/**
 * Class for publishing JS code for tokenization.
 *
 * @since 3/28/17.
 */
object TokenizerPublisher
{

  def generateJS(pattern: String, input: String, output: String) =
  {
    val tokenizerJs = new StringBuilder
    tokenizerJs ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "var " +
      output + " = " + input + ".replace(/\\\"/g,\"\\\\\\\"\")"
    tokenizerJs ++= ".split(/"
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
      }) + "+/).filter(function(v){ return !(v === undefined || v === \"\" || v === null); });"
    tokenizerJs ++= javaPattern.substring(2)

    tokenizerJs
  }

}
