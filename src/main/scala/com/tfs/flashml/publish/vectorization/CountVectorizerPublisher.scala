package com.tfs.flashml.publish.featureengineering

import com.tfs.flashml.util.{ConfigUtils, PublishUtils}

import scala.collection.mutable

/**
 * Class for publishing JS code for CountVectorizer.
 *
 * @since 3/24/17.
 */
object CountVectorizerPublisher
{

  def generateJS(vocabulary: Array[String], input: String, output: String, binarizer: Boolean, vocabSize: Int,
                 cvFunction: Boolean, globalVar: mutable.Set[String]): StringBuilder =
  {
    val cntVectorizerJsTmp = if (cvFunction) countVectorizerJS
    else new StringBuilder
    globalVar += ("" + cntVectorizerJsTmp)
    val cntVectorizerJs = new StringBuilder
    cntVectorizerJs ++= createVocabularyMap(vocabulary, output)
    cntVectorizerJs ++= ""
    cntVectorizerJs ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var " +
      "binarizer_" + output + " = " + binarizer.toString + ";"
    cntVectorizerJs ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var " +
      "size_" + output + " = " + vocabSize + ";"
    cntVectorizerJs ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var " +
      output + " = countVectorizer("
    cntVectorizerJs ++= input + ",countVectorizerMap_" + output + " ,binarizer_" + output + ");"
    cntVectorizerJs
  }

  def createVocabularyMap(vocabulary: Array[String], output: String): StringBuilder =
  {
    val vocabMapJS = new StringBuilder
    vocabMapJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var " +
      "countVectorizerMap_" + output + " ={"

    for ((value, index) <- vocabulary.zipWithIndex)
    {
      vocabMapJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 2) + "'" +
        value + "'" + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + ": " + index + ","
    }
    vocabMapJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "}"
    vocabMapJS
  }

  def countVectorizerJS(): StringBuilder =
  {
    val cntVectJS = new StringBuilder
    cntVectJS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "function countVectorizer(termArr," +
      "countVectorizerMap,binarizer){"
    cntVectJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var vectorizedTF = {};"
    cntVectJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "for (var ind in termArr){"
    cntVectJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var vectorIndex = " +
      "countVectorizerMap[termArr[ind]];"
    cntVectJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "if (vectorIndex in vectorizedTF && " +
      "!binarizer){"
    cntVectJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "vectorizedTF[vectorIndex] = " +
      "vectorizedTF[vectorIndex] +1;"
    cntVectJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "}"
    cntVectJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "else{"
    cntVectJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "vectorizedTF[vectorIndex] = 1;"
    cntVectJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "}"
    cntVectJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    cntVectJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "return vectorizedTF"
    cntVectJS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "}"
    cntVectJS
  }
}
