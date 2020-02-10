package com.tfs.flashml.core.preprocessing

import java.io.File

import com.tfs.flashml.util.Json

import scala.util.control.Breaks._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.control.Breaks

/**
 * Helper Object for preprocessing Engine. Loads the resources required for transformations.
 *
 * @since 23/8/18
 */
object PreprocessingStageLoader
{

  private case class customStringCaseClass(stringValue: String)

  private case class customNestedStringArrayList(arrList: java.util.ArrayList[customStringCaseClass])

  private case class customHashMap(customMapObj: java.util.HashMap[customStringCaseClass, customStringCaseClass])

  //private case class customHashMap(obj: java.util.HashMap[String, String])


  /**
   * @param parameters filepath(String) and Array containing all required words as Strings
   * @return Array of all the stopwords (strings)
   */
  def getStopWords(parameters: Any): Array[String] =
  {
    parameters match
    {

      case filePath: String => Source.fromFile(filePath).mkString.split("\n")

      case arrayStopWords: java.util.ArrayList[customStringCaseClass] =>

        arrayStopWords
          .asInstanceOf[java.util.ArrayList[String]]
          .asScala
          .toArray
    }
  }

  /**
   * @param parameters filepath(String) or Map containing word Classes as keys and list of words mapped to each key
   * @return mapping of each word to its word class
   *         Order of values may vary
   */
  def getWordClassReplacements(parameters: Any): Seq[(String, String)] =
  {
    //case class customNestedStringArrayList(arrList: java.util.ArrayList[customStringCaseClass])

    parameters match
    {
      case wordClassReplHashMap: java.util.HashMap[customStringCaseClass, customNestedStringArrayList] =>
      {
        wordClassReplHashMap
          .asInstanceOf[java.util.HashMap[String, java.util.ArrayList[String]]]
          .asScala
          .foldLeft(Seq[(String, String)]())((accumulator, keyValue) =>
          {
            accumulator ++ keyValue._2.asScala
              .map(word => (word, keyValue._1))
          })
      }

      case filePath: String =>
      {
        val inputClasses = Source.fromFile(filePath).mkString
        val inputClassesMap = Json.parse(inputClasses).asMap
        inputClassesMap
          .foldLeft(Seq[(String, String)]())((accumulator, keyValue) =>
          {
            accumulator ++ keyValue._2.asArray
              .map(word => (word.asString, keyValue._1)).toSeq
          })
      }
    }
  }

  def computeWordRegexSeq(wordClasses: Seq[(String, String)]): Seq[(String, String)] =
  {

    val wordClassesMap = mutable.LinkedHashMap(wordClasses: _*)
    var linewiseWordClassList = new mutable.LinkedHashMap[Int, List[String]]()
    val sortedWordList = wordClassesMap.keySet.toList
    val currentWordClass = wordClassesMap.get(sortedWordList(0))
    linewiseWordClassList.put(0, List[String]())

    /**
     * This method will create a map of (class,list[class elements]) in a specific order
     * Eg: (class_visa,(visa credit card)),(class_credit,(credit card)),(class_visa,(card))
     *
     * @param wordClasses         - Map of element and its class name
     * @param wordList            - word list sorted by length
     * @param linewiseWordClasses - output map contains class name and their elements in required order
     * @param wordClass           - class name for which we need to fetch the elements currently
     * @return
     */
    def getLinewiseWordlist(wordClasses: mutable.LinkedHashMap[String, String], wordList: List[String],
                            linewiseWordClasses: mutable.LinkedHashMap[Int, List[String]], wordClass: String)
    : mutable.LinkedHashMap[Int, List[String]] =
    {

      var wordToAdd: String = ""
      val loop = new Breaks
      var lineNo = linewiseWordClasses.size - 1
      var currentWordClass = wordClass
      if (wordList.isEmpty)
      {
        return linewiseWordClasses
      }
      loop.breakable
      {
        for (word <- wordList)
        {
          if (wordClasses(word).equals(currentWordClass))
          {
            wordToAdd = word
            loop.break()
          }
        }
      }

      if (wordToAdd.equals(""))
      {
        wordToAdd = wordList.head
        currentWordClass = wordClasses(wordToAdd)
        lineNo = linewiseWordClasses.size
        linewiseWordClasses.put(lineNo, List[String]())
      }

      loop.breakable
      {
        for (word <- wordList)
        {
          if (wordToAdd.equals(word))
            loop.break()

          if ((".*" + wordToAdd + ".*").r.pattern.matcher(word).matches())
          {
            val newWordClass = wordClasses(word)
            lineNo = linewiseWordClasses.size
            if (linewiseWordClasses.get(lineNo - 1).nonEmpty)
            {
              linewiseWordClasses.put(lineNo, List[String]())
            }

            return getLinewiseWordlist(wordClasses, wordList, linewiseWordClasses, newWordClass)
          }
        }
      }

      lineNo = linewiseWordClasses.size - 1
      val newWordList = linewiseWordClasses(lineNo) :+ wordToAdd
      linewiseWordClasses.put(lineNo, newWordList)
      val remainingList = wordList.filter(_ != wordToAdd)
      return getLinewiseWordlist(wordClasses, remainingList, linewiseWordClasses, currentWordClass)

    }

    linewiseWordClassList = getLinewiseWordlist(wordClassesMap, sortedWordList, linewiseWordClassList,
      currentWordClass.get)
    getWordClassRegex(wordClassesMap, linewiseWordClassList)
  }


  /**
   * This method will create the regex strings from the map we create in getLinewiseWordlist method
   *
   * @param wordClasses           - Map of element and its class name
   * @param linewiseWordClassList - output map contains class name and their elements as regex in required order
   * @return
   */
  def getWordClassRegex(wordClasses: mutable.LinkedHashMap[String, String], linewiseWordClassList: mutable
  .LinkedHashMap[Int, List[String]]): Seq[(String, String)] =
  {

    //val wordClassRegex = new ArrayBuffer[(String,String)]()
    // Regex are added with "s" to accommodate plurals also

    linewiseWordClassList.foldLeft(ArrayBuffer[(String, String)]())
    { (accumulator, regexWordList) =>
    {
      val sb = new mutable.StringBuilder()
      val currentWordList = regexWordList._2
      val currentWordClass = wordClasses(currentWordList(0))
      sb.append("(?i)\\b").append("(?:")
      sb.append(currentWordList.mkString("|"))
      sb.append(")'?s?").append("\\b")
      accumulator.append((currentWordClass, sb.toString))
      accumulator
    }
    }
  }

  /**
   * @param parameter filePath (string) or a map containing the mappings from regex label to the regex pattern
   *                  (String)
   * @return collection of multiple Tuples wherein each value is present as (regex label, pattern)
   */
  def getRegexes(parameter: Any): Seq[(String, String)] =
  {

    parameter match
    {
      case filePath: String =>
      {
        Source
          .fromFile(filePath, "utf-8")
          .getLines()
          .map(_.split("\\s+"))
          .toArray[Array[String]]
          .map(arr => Tuple2(arr(1), arr(0)))

      }

      case mapArray: java.util.ArrayList[customHashMap] =>
      {
        //HashMap is reversed, i.e. in order (RegexPattern -> Label) which is now reversed
        mapArray
          .asInstanceOf[java.util.ArrayList[java.util.HashMap[String, String]]]
          .asScala
          .map(obj =>
          {
            val tempReversedMap = obj
              .asScala
              .last
            (tempReversedMap._2, tempReversedMap._1)
          })
      }
    }
  }

  def getRegexRemovalPatterns(parameter: Any): Seq[(String, String)] =
  {
    parameter match
    {
      case filePath: String =>
      {
        Source
          .fromFile(filePath, "utf-8")
          .getLines()
          .map(x => ("", x)).toSeq
      }

      case regexArray: java.util.ArrayList[customStringCaseClass] =>
      {
        regexArray
          .asInstanceOf[java.util.ArrayList[String]]
          .asScala
          .toArray
          .map(x => ("", x))
          .toSeq

      }
    }
  }

  /**
   * @param parameter filePath(String) or a map of each wordclass (as String) to an array
   * @return Map containing the one-to-one mapping of each string to its word class Label
   *         Order of values may vary
   */
  def getWordReplacements(parameter: Any): Map[String, String] =
  {

    parameter match
    {
      case filePath: String =>
      {
        Source
          .fromFile(filePath)
          .getLines
          .map(_.split(","))
          .foldLeft(Map[String, String]())((acc, line) => acc + (line(0) -> line(1)))
      }

      case parameter: customHashMap =>
      {
        parameter.asInstanceOf[java.util.HashMap[String, String]]
          .asScala
          .toMap
      }
    }
  }

  /**
   * @param parameter filePath(String) or Array of exception words (as Strings)
   * @return Array of Strings containing all the exception words
   */
  def getExceptions(parameter: Any): Array[String] =
  {
    parameter match
    {

      case exceptionFile: String => if (new File(exceptionFile).exists()) Source.fromFile(exceptionFile, "UTF-8").getLines.toArray
      else Array.empty[String]

      case arrayStringExceptions: java.util.ArrayList[customStringCaseClass] =>
        arrayStringExceptions
          .asInstanceOf[java.util.ArrayList[String]]
          .asScala
          .toArray

    }
  }
}