package com.tfs.flashml.core.preprocessing.transformer

import java.io.Serializable

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

/** Prepend  "_class_ss " to the sentence and append " _class_se"
 * to end of sentence
 *
 * @since 11/11/16.
 */
class SentenceMarker(override val uid: String)
        extends UnaryTransformer[String, String, SentenceMarker]
                with Serializable
                with DefaultParamsWritable
{

    def this() = this(Identifiable.randomUID("sentenceMarker"))

    def getFeatures(words: String): String =
    {
        return "_class_ss " + words + " _class_se"
    }

    override protected def createTransformFunc: String => String =
    {
        getFeatures
    }

    override protected def validateInputType(inputType: DataType): Unit =
    {
        require(inputType == StringType, s"Input type must be StringType type but got $inputType.")
    }

    override protected def outputDataType: DataType =
    {
        StringType
    }
}

object SentenceMarker
        extends DefaultParamsReadable[SentenceMarker]
{
    override def load(path: String): SentenceMarker = super.load(path)
}
