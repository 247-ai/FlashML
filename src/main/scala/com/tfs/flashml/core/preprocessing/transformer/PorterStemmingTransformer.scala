package com.tfs.flashml.core.preprocessing.transformer

import java.io.Serializable

import com.github.aztek.porterstemmer._
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{Param, StringArrayParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StringType}

/**
  * Transformer to apply stemming on the sequence
  */

class PorterStemmingTransformer(override val uid: String)
  extends UnaryTransformer[String, String, PorterStemmingTransformer]
    with Serializable
    with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("porter_stemmer"))

  /**
    * A list of word on which stemming is not to be applied on
    */
  var stemExceptions: StringArrayParam = new StringArrayParam(this,"stemException", "provide a stem exception dictionary")

  val pattern: Param[String] = new Param(this,"delimiter","regex pattern used for tokenizing")

  def setExceptions(exceptions: Array[String]): this.type = {
    set(stemExceptions,exceptions)
  }

  def setDelimiter(value:String): this.type = set(pattern,value)

  def getDelimiter : String = $(pattern)

  def getExceptions: Array[String] = ${stemExceptions}

  setDefault( stemExceptions -> Array())

  override protected def createTransformFunc: String => String = {
    getFeatures
  }

  private def getFeatures(words: String): String = {
    //todo change null check to options

    def replacement(word: String): String = {
      if(!$(stemExceptions).isEmpty) {
        if (!$(stemExceptions).contains(word) && !word.contains("_class_")) {
          val update = Option(PorterStemmer.stem(word))
          update match {
              case Some(str) => str
              case None => word
          }
        }
        else
          word
      }
      else {
        if (!word.contains("_class_")) {
          val update = Option(PorterStemmer.stem(word))
          update match {
            case Some(str) => str
            case None => word
          }
        }
        else
          word
      }
    }

    ($(pattern) + "|(" +FlashMLConstants.CUSTOM_DELIMITER+")").r.split(words).toSeq.map(word => replacement(word)).mkString(FlashMLConstants.CUSTOM_DELIMITER)
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType)
  }

  override protected def outputDataType: DataType = StringType

}

object PorterStemmingTransformer
  extends DefaultParamsReadable[PorterStemmingTransformer] {
  override def load(path: String): PorterStemmingTransformer = super.load(path)
}