package com.tfs.flashml.core.preprocessing.transformer

import java.io.Serializable

import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.collection.JavaConverters._


/**
 * Dictionary based word substitution. Covers Contraction Replacement and Lemmatization.
 *
 * @since 25/8/18
 */
class WordSubstitutionTransformer(override val uid: String)
    extends UnaryTransformer[String, String, WordSubstitutionTransformer]
        with Serializable
        with DefaultParamsWritable
{

    def this() = this(Identifiable.randomUID("wordSubstitution"))

    /**
     * Dictionary Map which containes the words to be replaced and their replacment
     */
    val wordDict: StringMapParam = new StringMapParam(this, "dictionary", "dictionary")

    val pattern: Param[String] = new Param(this, "pattern", "regex pattern used for tokenizing")


    def getDictionary: Map[String, String] = $(wordDict)

    def setDictionary(dictionary: Map[String, String]): this.type = set(wordDict, dictionary)

    def getDelimiter: String = $(pattern)

    def setDelimiter(value: String): this.type = set(pattern, value)

    /**
     * First replace the word found in the dictionary with their substitutions.
     * There could be case like "couldn't" is replaced by "could not", then we need
     * to split the token again
     */
    override protected def createTransformFunc: String => String =
    {
        getSubstitution
        /*_.map(token => $(wordDict).getOrElse(token, token))
          .foldLeft(Seq[String]())((accumulator, subsToken) => accumulator ++ subsToken.split("\\s"))*/
    }

    private def getSubstitution(line: String): String =
    {
        val delimiter = $(pattern) + "|(" + FlashMLConstants.CUSTOM_DELIMITER + ")"
        val words = delimiter.r.split(line).toSeq
        words.flatMap(token => $(wordDict).getOrElse(token, token).split(delimiter)).mkString(FlashMLConstants.CUSTOM_DELIMITER)
    }

    override protected def validateInputType(inputType: DataType): Unit =
    {
        require(inputType == StringType)
    }

    override protected def outputDataType: DataType = StringType
}

object WordSubstitutionTransformer extends DefaultParamsReadable[WordSubstitutionTransformer]
{
    override def load(path: String): WordSubstitutionTransformer = super.load(path)
}

class StringMapParam(parent: Params, name: String, doc: String, isValid: Map[String, String] => Boolean)
    extends Param[Map[String, String]](parent, name, doc, isValid)
{

    def this(parent: Params, name: String, doc: String) =
        this(parent, name, doc, (_: Map[String, String]) => true)

    /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
    def w(value: java.util.HashMap[String, String]): ParamPair[Map[String, String]] = w(value.asScala.toMap)

    override def jsonEncode(value: Map[String, String]): String =
    {
        import org.json4s.JsonDSL._
        compact(render(value.toSeq))
    }

    override def jsonDecode(json: String): Map[String, String] =
    {
        implicit val formats = DefaultFormats
        parse(json).extract[Seq[(String, String)]].toMap
    }
}

