package com.tfs.flashml.publish.preprocessing

import com.tfs.flashml.util.{ConfigValues, PublishUtils}

import scala.collection.mutable

/**
  * Class to publish skip-grams.
  * @since 3/29/17.
  */
object SkipGramPublisher
{

  def generateJS(windowSize: Int, input: String, output: String, andRulesFunction: Boolean, globalVar: mutable.Set[String]) =
  {
    val andRulesJsTmp = if (andRulesFunction) andRulesFunctionJS
    else new StringBuilder
    globalVar += ("" + andRulesJsTmp)
    val andRulesJs = new StringBuilder
    andRulesJs ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "var " + output + " = "
    andRulesJs ++= input + ".concat(andRules(" + input + "," + windowSize + "));"
    //println(andRulesJs)
    andRulesJs
  }

  def andRulesFunctionJS =
  {
    val andRulesString = new StringBuilder
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "function andRules(termArr,windowSize){"
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "function nGramFilter(term){"
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "return term.indexOf(' + ') ==-1"
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var filteredArray = termArr.filter(nGramFilter);"
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var andRulesArr = [];"
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var n = filteredArray.length;"
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "for (var i=0;i<(n-1);i++){"
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "for (var j=i+2;j<n;j++){"
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "if((j-i) < windowSize && filteredArray[i] != filteredArray[j]){"
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(5) + "andRulesArr.push(filteredArray[i] + \" and \" + filteredArray[j]);"
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "}"
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "}"
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "return andRulesArr"
    andRulesString ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "}"
    andRulesString
  }

}
