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

import java.util.Locale
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

/**
 * A feature transformer that filters out stop words from input.
 * Created a custom class because the default Spark transformer accepts seq[String] but
 * this class takes String input, which is then splitted using the pattern which we set
 * using setter function.
 *
 * @note null values from input array are preserved unless adding null to stopWords
 *       explicitly.
 * @see <a href="http://en.wikipedia.org/wiki/Stop_words">Stop words (Wikipedia)</a>
 */

class StopWordsRemoverCustom(override val uid: String)
    extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable
{
    def this() = this(Identifiable.randomUID("stopwords_remove"))

    /** @group setParam */
    def setInputCol(value: String): this.type = set(inputCol, value)

    /** @group setParam */
    def setOutputCol(value: String): this.type = set(outputCol, value)

    /**
     * The words to be filtered out.
     * Default: English stop words
     *
     * @see `StopWordsRemover.loadDefaultStopWords()`
     * @group param
     */
    val stopWords: StringArrayParam =
        new StringArrayParam(this, "stopWords", "the words to be filtered out")
    val pattern: Param[String] = new Param(this, "pattern", "regex pattern used for tokenizing")

    /** @group setParam */
    def setStopWords(value: Array[String]): this.type = set(stopWords, value)

    def setDelimiter(value: String): this.type = set(pattern, value)

    def getDelimiter: String = $(pattern)

    /** @group getParam */
    def getStopWords: Array[String] = $(stopWords)

    /**
     * Whether to do a case sensitive comparison over the stop words.
     * Default: false
     *
     * @group param
     */
    val caseSensitive: BooleanParam = new BooleanParam(this, "caseSensitive",
        "whether to do a case-sensitive comparison over the stop words")

    /** @group setParam */
    def setCaseSensitive(value: Boolean): this.type = set(caseSensitive, value)

    /** @group getParam */
    def getCaseSensitive: Boolean = $(caseSensitive)

    /**
     * Locale of the input for case insensitive matching. Ignored when [[caseSensitive]]
     * is true.
     * Default: Locale.getDefault.toString
     *
     * @group param
     */
    val locale: Param[String] = new Param[String](this, "locale",
        "Locale of the input for case insensitive matching. Ignored when caseSensitive is true.",
        ParamValidators.inArray[String](Locale.getAvailableLocales.map(_.toString)))

    /** @group setParam */
    def setLocale(value: String): this.type = set(locale, value)

    /** @group getParam */
    def getLocale: String = $(locale)

    setDefault(stopWords -> StopWordsRemoverCustom.loadDefaultStopWords("english"),
        caseSensitive -> false, locale -> Locale.getDefault.toString)

    override def transform(dataset: Dataset[_]): DataFrame =
    {
        val outputSchema = transformSchema(dataset.schema)
        val t = if ($(caseSensitive))
        {
            val stopWordsSet = $(stopWords).toSet
            udf[String, String]
            {
                terms: String =>
                    val tokens = ($(pattern) + "|(" + FlashMLConstants.CUSTOM_DELIMITER + ")").r.split(terms).toSeq
                    tokens.filter(s => !stopWordsSet.contains(s)).mkString(FlashMLConstants.CUSTOM_DELIMITER)
            }
        }
        else
        {
            val lc = new Locale($(locale))
            val toLower = (s: String) => if (s != null) s.toLowerCase(lc)
            else s
            val lowerStopWords = $(stopWords).map(toLower(_)).toSet
            udf[String, String]
            {
                terms: String =>
                    val tokens = ($(pattern) + "|(" + FlashMLConstants.CUSTOM_DELIMITER + ")").r.split(terms).toSeq
                    tokens.filter(s => !lowerStopWords.contains(toLower(s))).mkString(FlashMLConstants.CUSTOM_DELIMITER)
            }
        }
        val metadata = outputSchema($(outputCol)).metadata
        dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
    }

    override def transformSchema(schema: StructType): StructType =
    {
        val inputType = schema($(inputCol)).dataType
        require(inputType.sameType(StringType), "Input type must be " +
            s"${StringType.catalogString} but got ${inputType.catalogString}.")
        SchemaUtils.appendColumn(schema, $(outputCol), inputType, schema($(inputCol)).nullable)
    }

    override def copy(extra: ParamMap): StopWordsRemoverCustom = defaultCopy(extra)
}

object StopWordsRemoverCustom extends DefaultParamsReadable[StopWordsRemoverCustom]
{
    private[feature]
    val supportedLanguages = Set("danish", "dutch", "english", "finnish", "french", "german",
        "hungarian", "italian", "norwegian", "portuguese", "russian", "spanish", "swedish", "turkish")

    override def load(path: String): StopWordsRemoverCustom = super.load(path)

    /**
     * Loads the default stop words for the given language.
     * Supported languages: danish, dutch, english, finnish, french, german, hungarian,
     * italian, norwegian, portuguese, russian, spanish, swedish, turkish
     *
     * @see <a href="http://anoncvs.postgresql.org/cvsweb.cgi/pgsql/src/backend/snowball/stopwords/">
     *      here</a>
     */
    @Since("2.0.0")
    def loadDefaultStopWords(language: String): Array[String] =
    {
        require(supportedLanguages.contains(language),
            s"$language is not in the supported language list: ${supportedLanguages.mkString(", ")}.")
        val is = getClass.getResourceAsStream(s"/org/apache/spark/ml/feature/stopwords/$language.txt")
        scala.io.Source.fromInputStream(is)(scala.io.Codec.UTF8).getLines().toArray
    }
}
