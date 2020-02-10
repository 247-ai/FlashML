package com.tfs.flashml.dal

import com.tfs.flashml.util.ConfigUtils
import com.tfs.flashml.util.ConfigUtils.primaryKeyColumns
import com.tfs.flashml.util.FlashMLConfig._
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Parent class for reading datasets from different sources.
 *
 * @since 8/11/16.
 */
abstract class DataReader
{

  private val log = LoggerFactory.getLogger(getClass)

  def read: DataFrame

  /**
   * The following function translates an ID column to a double random variable between 0 and 100.
   * Assumes that the first 8 characters are hexadecimal.
   *
   * @param col
   * @return
   */
  def randomVarFromCol(col: Column): Column =
  {
    (conv(substring(col, 1, 8), 16, 10)).cast("double") * 100 / FlashMLConstants.MAX8BITHEXDOUBLE
  }

  /**
   * Method to generate a random variable column. <br />
   * We want to generate new random variable column in the following two cases:<br />
   * (1)  No random variable column is defined, but a column for generating random variable is defined. Currently
   * we support coulmns having the first 8 characters as hexadecimal string as valid columns for generating
   * random numbers.<br />
   * (2)  Random variable column is defined, but not all rows have the random variable value populated. In this case,
   * we will again fall back on having a random variable generator column defined as described above.
   *
   * @param df
   * @return
   */
  def generateRandomVariable(df: DataFrame): DataFrame =
  {
    if (getString(FlashMLConstants.SAMPLING_TYPE) == FlashMLConstants.SAMPLING_TYPE_CONDITIONAL)
    {
      if (getString(FlashMLConstants.RANDOM_VARIABLE).isEmpty && !getString(FlashMLConstants
        .RANDOM_NUMBER_GENEARATOR_VARIABLE).isEmpty)

      {
        df.withColumn(FlashMLConstants.RANDOM_VARIABLE_COLUMN_NAME, randomVarFromCol(col(getString
        (FlashMLConstants.RANDOM_NUMBER_GENEARATOR_VARIABLE))))
      }
      else if (df.filter(s"${getString(FlashMLConstants.RANDOM_VARIABLE)} is null").count() > 0 && !getString
      (FlashMLConstants.RANDOM_NUMBER_GENEARATOR_VARIABLE).isEmpty)
      {
        df.drop(getString(FlashMLConstants.RANDOM_VARIABLE))
          .withColumn(getString(FlashMLConstants.RANDOM_VARIABLE), randomVarFromCol(col(getString
          (FlashMLConstants.RANDOM_NUMBER_GENEARATOR_VARIABLE))))
      }
      else
      {
        // In this case, we are making sure that the random variable is of type double.
        df.withColumn(getString(FlashMLConstants.RANDOM_VARIABLE), df.col(getString(FlashMLConstants
          .RANDOM_VARIABLE)).cast(DoubleType))
      }
    }
    else
      df
  }

  /**
   * Validate all the queries. Simple validation requires queries to be SELECT only.
   *
   * @param queries
   * @return
   */
  def validateInputSQLQueries(queries: Array[String]): Boolean =
  {
    for (query <- queries)
    {
      if (query.trim.toLowerCase().startsWith("select")) return false
    }
    true
  }

  /**
   * Below functions constructs a recursive view of queries
   * built for tail-recursion
   *
   * @param queries      A List of queries to be executed.
   * @param index        Length of queries list
   * @param prefix       Table prefix for intermediate tables
   * @param sparkSession instance
   * @return Last query result DataFrame
   */
  def processSQLViewsRec(queries: mutable.Buffer[String], index: Int, prefix: String, sparkSession: SparkSession)
  : DataFrame =
  {

    val tempView = sparkSession.sql(queries.head)
    tempView.createOrReplaceTempView(prefix + index.toString)

    if (queries.length < 2)
    {
      tempView
    }
    else
    {
      processSQLViewsRec(queries.tail, index + 1, prefix, sparkSession)
    }
  }

  /**
   * Method to process the input data, including applying filter.
   *
   * @param df
   * @return
   */
  def processInputData(df: DataFrame): DataFrame =
  {
    val filterCondition: String = if (config.getString(FlashMLConstants.FILTER_CONDITION) == "")
    {
      config.getString(FlashMLConstants.RESPONSE_VARIABLE) + " is not null"
    }
    else config.getString(FlashMLConstants.FILTER_CONDITION)

    val filteredDF = df
      .filter(filterCondition)
      .select(ConfigUtils.columnNamesVariables
        .distinct
        .map(col): _*)

    // Generate time on page variable
    def generateTimeOnPageColumn(df: DataFrame): DataFrame =
    {
      if (!df.columns.contains(ConfigUtils.topVariable) && !ConfigUtils.cumulativeSessionTime.isEmpty)
      {
        val window = Window
          .partitionBy(ConfigUtils.primaryKeyColumns.map(col): _*)
          .orderBy(ConfigUtils.pageColumn)
        df.withColumn(ConfigUtils.topVariable, lead(ConfigUtils.cumulativeSessionTime, 1).over(window) - col
        (ConfigUtils.cumulativeSessionTime))

      }
      else
        df
    }

    val dfWithTop = generateTimeOnPageColumn(filteredDF)

    // Random Variable Generation
    val dfWithRV = generateRandomVariable(dfWithTop)
    dfWithRV.persist(StorageLevel.MEMORY_ONLY_SER)

    if (getBool(FlashMLConstants.SAVEPOINTING_REQUIRED))
      SavePointManager.saveInputData(dfWithRV)
    dfWithRV
  }
}
