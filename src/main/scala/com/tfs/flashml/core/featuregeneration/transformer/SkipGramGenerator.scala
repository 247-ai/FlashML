package com.tfs.flashml.core.featuregeneration.transformer

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.IntParam
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructType}

/**
  * Generates new features (word phrases) in a certain window size by skipping words in-between. These features
  * are also called skip-grams. The words are appended using &.
  */
class SkipGramGenerator(override val uid: String)
  extends UnaryTransformer[Seq[String], Seq[String], SkipGramGenerator]
    with Serializable
    with DefaultParamsWritable
{

  def this() = this(Identifiable.randomUID("skip_gram"))

  val windowSize: IntParam = new IntParam(this, "windowSize", "AND rule window size")

  def setWindowSize(value: Int): this.type = set(windowSize, value)

  def getWindowSize: Int = $(windowSize)

  setDefault(windowSize -> 5)

  def getANDFeatures(words: Seq[String]): Seq[String] =
  {
    // TODO:  Optimize below function to return StreamType instead of evaluated Vector
    // The full evaluation is happening because of flatMap
    (3 to $(windowSize))
      .flatMap(words
        .iterator
        .sliding(_)
        .withPartial(false)
        .map(w =>
        {
          if (w.head != w.last)
          {
            w.head + " & " + w.last
          }
          else ""
        })
      ).filter(!_.isEmpty)
  }

  override protected def createTransformFunc: Seq[String] => Seq[String] =
  {
    getANDFeatures
  }

  override protected def outputDataType: DataType =
  {
    new ArrayType(StringType, false)
  }

  override protected def validateInputType(inputType: DataType): Unit =
  {
    require(inputType == ArrayType(StringType), s"Input type must be ArrayType(StringType) type but got $inputType.")
  }

  override def transformSchema(schema: StructType): StructType =
  {
    if ($(windowSize) < 3)
    {
      throw new IllegalArgumentException(s"windowSize must be atleast 3.  Use bi-grams instead for less")
    }
    super.transformSchema(schema)
  }
}

object SkipGramGenerator extends DefaultParamsReadable[SkipGramGenerator]
{
  override def load(path: String): SkipGramGenerator = super.load(path)
}
