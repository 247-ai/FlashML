package com.tfs.flashml.core.preprocessing.transformer

import java.io.Serializable

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}
import org.json4s
import org.json4s.{DefaultFormats, JString}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.collection.JavaConverters._

/**
 * Transformer to replace regexes within a particular string.
 *
 * @since 10/8/18
 */

class RegexReplacementTransformer(override val uid: String)
        extends UnaryTransformer[String, String, RegexReplacementTransformer]
                with Serializable
                with DefaultParamsWritable
{

    def this() = this(Identifiable.randomUID("regexRepl"))

    val sortedRegexMapSeq = new StringArrayArrayParam(this, "regexReplacements", "an Array[Array[String]], with 2 " +
            "elements in the inner array where 1st element is replacement & 2nd element array")
    val pattern: Param[String] = new Param(this, "pattern", "regex pattern used for tokenizing")


    /**
     * Sets the regex classes for the processor.
     * Array is used to maintain order of the regex
     *
     * @param value - Sequence for 2 element tuple, where the 1st element
     *              is the ordered classes and the 2nd the regex
     * @return Regex Replacement Processor with regexes set
     */
    def setRegexReplacements(value: Seq[(String, String)]): this.type =
    {
        /* accepting arguments as tuple to restrict user from giving more entries */
        set(sortedRegexMapSeq, value.toArray.map(_.productIterator.map(_.asInstanceOf[String]).toArray))
        RegexReplacementTransformer.this
    }

    def setDelimiter(value: String): this.type = set(pattern, value)

    def getDelimiter: String = $(pattern)

    def getRegexReplacements: Array[(String, String)] = $(sortedRegexMapSeq).map
    { case Array(replacement, regex) => (replacement, regex)

    }


    override protected def createTransformFunc: String => String =
    {

        //This method is applied in a distributed fashion to each element of the column.

        //Each regex is tested on each element of the sequence.
        /*sortedRegexMapSeq.foldLeft[Seq[String]](_)((acc, regexTuple) => {
          val regexObj = regexTuple._2.r
          //Begin index is 1 because 1st character of the class is the order.
          acc.map(regexObj.replaceAllIn(_, regexTuple._1.substring(1)))
        })*/
        regexReplace
    }

    private def regexReplace(line: String): String =
    {
        getRegexReplacements
                .foldLeft(line)((accumulator, regexTuple) =>
                {
                    val regexObj = regexTuple._2.r
                    regexObj.replaceAllIn(accumulator, regexTuple._1)
                })
    }

    override protected def validateInputType(inputType: DataType): Unit =
    {
        require(inputType == StringType)
    }

    override protected def outputDataType: DataType = StringType
}

object RegexReplacementTransformer extends
        DefaultParamsReadable[RegexReplacementTransformer]
{
    override def load(path: String): RegexReplacementTransformer = super.load(path)
}

class StringArrayArrayParam(parent: Params, name: String, doc: String, isValid: Array[Array[String]] => Boolean)
        extends Param[Array[Array[String]]](parent, name, doc, isValid)
{

    /* constructor with no validation */
    def this(parent: Params, name: String, doc: String) =
        this(parent, name, doc, _ => true)

    /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
    def w(value: java.util.List[java.util.List[String]]): ParamPair[Array[Array[String]]] = w(value.asScala.map(_.asScala.toArray).toArray)

    override def jsonEncode(value: Array[Array[String]]): String =
    {
        import org.json4s.JsonDSL._
        compact(render(value.toSeq.map(_.toSeq)))
    }

    override def jsonDecode(json: String): Array[Array[String]] =
    {
        import org.json4s.JsonAST.{JArray, JString}
        parse(json) match
        {
            case JArray(values) =>
                values.map
                {
                    case JArray(values) =>
                        values.map
                        {
                            case JString(x) => x
                            case _ => throw new IllegalArgumentException(s"Cannot decode $json to Array[Array[String]].")
                        }.toArray
                    case _ =>
                        throw new IllegalArgumentException(s"Cannot decode $json to Array[Array[String]].")
                }.toArray
            case _ =>
                throw new IllegalArgumentException(s"Cannot decode $json to Array[Array[String]].")
        }
    }
}
