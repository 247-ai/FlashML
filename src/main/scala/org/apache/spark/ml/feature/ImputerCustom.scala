/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.feature

import scala.collection.mutable.ArrayBuilder
import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute, UnresolvedAttribute}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.{Param, ParamMap, StringArrayParam}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Created this custom transformer because the default imputer will do null replacement only for the numeric columns and also it replace
  * all null and NaN of those numeric columns to its average. So in this we are doing replacement for both text and numeric columns and also that too
  * with the user provided values.
  */

class ImputerCustom (override val uid: String)
  extends Transformer with HasInputCols with HasOutputCol with DefaultParamsWritable {


  def this() = this(Identifiable.randomUID("imputerCustom"))

  val inputColumn: Param[String] = new Param(this,"input column","Param for input column name")

  val replaceValue: Param[String] = new Param(this,"Replacement value","Value to replace with null")

  /** @group setParam */

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setInputColumn(value: String):this.type = set(inputColumn,value)

  def setReplacementValue(value: String):this.type = set(replaceValue,value)

  def getReplacementValue:String = ${replaceValue}

  def getInputColumn:String = ${inputColumn}

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def transform(dataset: Dataset[_]): DataFrame = {
    val schema = dataset.schema
    val replace = schema(${inputColumn}).dataType match {
      case s:StringType => ${replaceValue}
      case d:DoubleType => ${replaceValue}.toDouble
      case i:IntegerType => ${replaceValue}.toInt
      case f:FloatType => ${replaceValue}.toFloat
      case l:LongType => ${replaceValue}.toLong
    }
    dataset.na.fill(Map(${inputColumn} -> replace)).select(col("*"))
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): ImputerCustom = defaultCopy(extra)
}

object ImputerCustom extends DefaultParamsReadable[ImputerCustom] {

  override def load(path: String): ImputerCustom = super.load(path)

}
