package com.tfs.flashml.publish.featureGeneration

import com.tfs.flashml.util.{ConfigValues, PublishUtils}
import org.slf4j.LoggerFactory

/**
  * Class for publishing JS code for n-grams.
  *
  * @since 3/28/17.c
  */
object NGramPublisher {

  private val log = LoggerFactory.getLogger(getClass)

  def generateJS(n:Int, input:String, output:String, nGramFunction:Boolean) = {
    val nGramJs = if (nGramFunction) nGramsFuncJS else new StringBuilder
    nGramJs ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "var " + output + " = nGramGenerator(" + input + "," + n + ");"
    nGramJs
  }

  def nGramsFuncJS ={
    val nGramsString = new StringBuilder
    nGramsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "function nGramGenerator(termArr,n){"
    nGramsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 2) + "var nGrams = [];"
    nGramsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 2) + "for (var k=1;k<=n;k++){"
    nGramsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 3) + "for (var i = 0; i <= termArr.length - k; i++){"
    nGramsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 4) + "gram_temp = [];"
    nGramsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 4) + "for (var j = 0; j < k; j++){"
    nGramsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 5) + "gram_temp.push(termArr[i + j]);"
    nGramsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 4) + "}"
    nGramsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 4) + "gram = gram_temp.join(\" + \");"
    nGramsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 4) + "nGrams.push(gram);"
    nGramsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 3) + "}"
    nGramsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 2) + "}"
    nGramsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 2) + "return nGrams"
    nGramsString ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "}"
    nGramsString
  }

}
