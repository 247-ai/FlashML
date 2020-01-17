package com.tfs.flashml.core.featuregeneration.transformer

import org.apache.spark.SparkException
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Assembles the output of n-gram transformers and the original sequence into one column.
 *
 * @since 22/8/18
 */

class GramAssembler(override val uid: String)
        extends Transformer with HasInputCols with HasOutputCol
                with DefaultParamsWritable
{

    def this() = this(Identifiable.randomUID("gramAdd"))

    def setInputCols(value: Array[String]): this.type = set(inputCols, value)

    def setOutputCol(value: String): this.type = set(outputCol, value)

    def copy(extra: ParamMap): GramAssembler = defaultCopy(extra)

    def transform(df: Dataset[_]): DataFrame =
    {
        // Data transformation.
        val assembleFunc = udf
        { r: Row =>
            GramAssembler.assemble(r.toSeq: _*)
        }

        val args: Array[Column] = $(inputCols).map
        { c => df(c) }

        df.select(col("*"), assembleFunc(struct(args: _*)).as($(outputCol)))
    }

    override def transformSchema(schema: StructType): StructType =
    {
        // Check that the input type is a string
        $(inputCols) foreach
                { strCol: String =>
                    val idx = schema.fieldIndex(strCol)
                    val field = schema.fields(idx)
                    if (!(field.dataType == ArrayType(StringType, false) || field.dataType == ArrayType(StringType,
                      true)))
                    {
                        throw new Exception(s"Input type ${field.dataType} did not match required type ArrayType" +
                                s"(StringType)")
                    }
                }
        // Add the return field
        schema.add(StructField($(outputCol), ArrayType(StringType, true), true))
    }
}

object GramAssembler
        extends DefaultParamsReadable[GramAssembler]
{
    override def load(path: String): GramAssembler = super.load(path)

    //Join all the Array Columns into one
    def assemble(rowEntity: Any*): collection.mutable.WrappedArray[String] =
    {

        var outputArray = rowEntity(0).asInstanceOf[collection.mutable.WrappedArray[String]]
        case class customStringCaseClass(stringValue: String)


        rowEntity
                .drop(1).foreach
        {
            case v: collection.mutable.WrappedArray[customStringCaseClass] =>
                outputArray ++= v
                        .asInstanceOf[collection.mutable.WrappedArray[String]]
            case null =>
                throw new SparkException("Values to assemble cannot be null.")
            case o =>
                throw new SparkException(s"$o of type ${o.getClass.getName} is not supported.")
        }

        outputArray
    }
}

