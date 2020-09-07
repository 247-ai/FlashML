package com.tfs.flashml.publish.transformer

/*import java.io.Serializable

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{DoubleArrayParam, Param}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DoubleType, StringType}

/**
 * Transformer to convert all words to lower case.
 * @since 24/8/18
 */

class HotleadTransformer(override val uid: String)
  extends UnaryTransformer[Double, Boolean, HotleadTransformer]
    with Serializable
    with DefaultParamsWritable{

  def this() = this(Identifiable.randomUID("hotlead_predict"))
  
  var thresholds: DoubleArrayParam = new DoubleArrayParam(this,"ProbThresholds","Thresholds cut-off for hotlead")
  
  var pageNo:Param[Int] = new Param(this,"pageNum", "Page number of current features")
  
  var nPages: Param[Int] = new Param(this,"nPages","Total no of pages for which the model has been built")
  
  var isPageLevel:Param[Boolean] = new Param(this,"isPageLevel","Identify whether it is a page level model or not")
  
  def setIsPageLevel(value:Boolean) : this.type = {
    set(isPageLevel,value)
  }
  
  def setNPages(value:Int):this.type = {
    set(nPages,value)
  }
  
  def setThreshold(value:Array[Double]): this.type = {
    set(thresholds,value)
  }
  
  def setPageNo(value:Int): this.type = {
    set(pageNo,value)
  }

  def getIsPageLevel:Boolean = $(isPageLevel)
  
  def getPageNo:Int = $(pageNo)
  
  def getNPages:Int = $(nPages)
  
  def getThreshold:Array[Double] = $(thresholds)

  override protected def createTransformFunc: Double => Boolean = {
    isHotlead
  }
  
  def isHotlead(prob:Double):Boolean = {
    if($(isPageLevel) == false)
      var result = false
      0 to $(nPages) foreach(i => if())
    else
      if(prob > $(thresholds)($(pageNo)-1)) true else false
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == DoubleType, s"Input type must be Double type but got $inputType.")
  }

  override protected def outputDataType: DataType = {
    BooleanType
  }
}

object HotleadTransformer
  extends DefaultParamsReadable[HotleadTransformer] {
  override def load(path: String): HotleadTransformer = super.load(path)
}*/

import org.apache.spark.SparkException
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.param.{DoubleArrayParam, IntArrayParam, Param, ParamMap}
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

class HotleadTransformer(override val uid: String)
    extends Transformer with HasInputCols with HasOutputCol
        with DefaultParamsWritable
{
    def this() = this(Identifiable.randomUID("hotlead_predict"))

    def setInputCols(value: Array[String]): this.type = set(inputCols, value)

    def setOutputCol(value: String): this.type = set(outputCol, value)

    var thresholds: DoubleArrayParam = new DoubleArrayParam(this, "ProbThresholds", "Thresholds cut-off for hotlead")
    var topThresholds: DoubleArrayParam = new DoubleArrayParam(this, "TopThresholds", "TOP(Time on page) Thresholds cut-off for hotlead")
    var pageNo: Param[Int] = new Param(this, "pageNum", "Page number of current features")
    var nPages: Param[Int] = new Param(this, "nPages", "Total no of pages for which the model has been built")
    var isPageLevel: Param[Boolean] = new Param(this, "isPageLevel", "Identify whether it is a page level model or not")

    def setIsPageLevel(value: Boolean): this.type =
    {
        set(isPageLevel, value)
    }

    def setNPages(value: Int): this.type =
    {
        set(nPages, value)
    }

    def setThreshold(value: Array[Double]): this.type =
    {
        set(thresholds, value)
    }

    def setTopThresholds(value: Array[Double]): this.type =
    {
        set(topThresholds, value)
    }

    def setPageNo(value: Int): this.type =
    {
        set(pageNo, value)
    }

    def getIsPageLevel: Boolean = $(isPageLevel)

    def getPageNo: Int = $(pageNo)

    def getNPages: Int = $(nPages)

    def getThreshold: Array[Double] = $(thresholds)

    def getTopThresholds: Array[Double] = $(topThresholds)

    def copy(extra: ParamMap): HotleadTransformer = defaultCopy(extra)

    def transform(df: Dataset[_]): DataFrame =
    {
        // Data transformation.
        val isHotlead = udf[Boolean, Row]
        { r: Row =>
            var page = r.get(0).asInstanceOf[Int]
            var prob = r.get(1).asInstanceOf[DenseVector](1)
            val top = if (r.length > 2) r.get(2).asInstanceOf[Double]
            else 0.0
            if (page >= $(nPages))
                if (prob >= $(thresholds).last && (if (r.length > 2)
                {
                    if (top >= $(topThresholds).last) true
                    else false
                }
                else true)) true
                else false
            else if (prob >= $(thresholds)(page - 1) && (if (r.length > 2)
            {
                if (top >= $(topThresholds)(page - 1)) true
                else false
            }
            else true)) true
            else false
        }
        val args: Array[Column] = $(inputCols).map(c => df(c))
        val res = df.select(col("*"), isHotlead(struct(args: _*)).as($(outputCol)))
        res
    }

    override def transformSchema(schema: StructType): StructType =
    {
        // Add the return field
        schema.add(StructField($(outputCol), BooleanType, true))
    }
}

object HotleadTransformer
    extends DefaultParamsReadable[HotleadTransformer]
{
    override def load(path: String): HotleadTransformer = super.load(path)

    /* //Join all the Array Columns into one
     def hotleadCheck(rowEntity: Any*): collection.mutable.WrappedArray[String] =
     {

       var outputArray = rowEntity(0).asInstanceOf[collection.mutable.WrappedArray[Double]]
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
     }*/
}


