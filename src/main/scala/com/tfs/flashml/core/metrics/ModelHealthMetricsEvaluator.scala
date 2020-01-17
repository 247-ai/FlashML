package com.tfs.flashml.core.metrics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{udf, _}
import org.slf4j.LoggerFactory

/**
 * Evaluator for model health metrics like PSI (Population Stability Index) and VSI (Variable Stability Index). <br />
 * References: <br />
 * 1. https://mwburke.github.io/2018/04/29/population-stability-index.html <br />
 * 2. https://stats.stackexchange.com/questions/219822/what-is-the-intuition-behind-the-population-stability-index
 */
class ModelHealthMetricsEvaluator
{
    private val log = LoggerFactory.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.OFF)

    //ntile describes the required Quantile Number for calculations
    private val N_TILE = 10

    private val ss = SparkSession.builder().getOrCreate()

    import ss.implicits._

    private val udfComputeSI = udf((col1value: Double, col2value: Double) =>
    {
        (col1value - col2value) * Math.log(col1value / col2value) * 100
    })


    /**
     * Module for calculating PSI (population stability index)
     *
     * @param baseDF              DataFrame containing base Data as a DataFrame, generally smaller than the newer
     *                            DataSet
     * @param scoreColumnVariable String containing the name of the Column containing the scores
     * @param maxNOP              Integer describing max number of allowed Page Numbers
     * @param newDataDF           DataFrame containing data from the newer DataSet, generally much larger in size
     *                            than base DataSet
     * @param pageCountVariable   String containing the name of the Page Number column
     * @return DataFrame containing (pageNum, PSIscore, numberOfQuantiles, excludedRangeIntervals)
     *
     *         NOTE:  DataFrames need not be cached as function caches both DataFrames for optimization
     */
    def psi(baseDF: DataFrame, scoreColumnVariable: String, maxNOP: Int, newDataDF: DataFrame, pageCountVariable: String): DataFrame =
    {
        // dataFrames cached for optimization
        baseDF.cache()
        newDataDF.cache()

        val lowerBoundaryLimit = 0
        val upperBoundaryLimit = 1

        val baseDataDF = baseDF.filter(col(pageCountVariable) <= maxNOP)
        val newDatasetDF = newDataDF.filter(col(pageCountVariable) <= maxNOP)
        val discretizer = new QuantileDiscretizer()
                .setInputCol(scoreColumnVariable)
                .setOutputCol("bucketNumber")
                .setNumBuckets(N_TILE)

        val result = (1 to maxNOP).par.map(pageNum =>
        {
            val currentPageBaseDF = baseDataDF
                    .filter(col(pageCountVariable) === pageNum)
                    .select(scoreColumnVariable)
            val currentPageNewDataDF = newDatasetDF
                    .filter(col(pageCountVariable) === pageNum)
                    .select(scoreColumnVariable)

            val resultFit = discretizer
                    .fit(currentPageBaseDF)
            //discretizer modeled on baseDataSet, will be used to generate splits in both baseDataSet & newDataSet

            //now both train & predict DF have been given quantile number with column name "bucketNumber"
            val baseDFstats = resultFit
                    .transform(currentPageBaseDF)
                    .groupBy("bucketNumber")
                    .count()
            val newDataDFStats = resultFit
                    .transform(currentPageNewDataDF)
                    .groupBy("bucketNumber")
                    .count()

            var joinedDataDF = baseDFstats
                    .join(newDataDFStats, newDataDFStats("bucketNumber") === baseDFstats("bucketNumber"))
                    .select(newDataDFStats("count")
                            .as("newDataCount"), baseDFstats("count")
                            .as("baseCount"), newDataDFStats("bucketNumber"))

            val combinedDFquantileValues = joinedDataDF
                    .select("bucketNumber")
                    .distinct()
                    .collect()
                    .map(r =>
                    {
                        r.getDouble(0).toInt
                    })
                    .toSet

            val splits = resultFit.getSplits

            splits(0) = lowerBoundaryLimit
            splits(splits.length - 1) = upperBoundaryLimit
            // the splits contain ranges sorted in increasing order from -Inf to +Inf
            //splits boundary points set to lower & upper limits

            val excludedBucketRanges = Set(0 until N_TILE)
                    .flatten
                    .diff(combinedDFquantileValues)
                    .map(qNum =>
                    {
                        (splits(qNum), splits(qNum + 1))
                    })
                    .toList

            //excludedRanges must be returned as a List instead of a Set due to encoding errors
            //Set of all Quantile numbers which are absent in Combined List are obtained. These quantile numbers will
            // give us the range
            // of values which form a bucket of population zero
            //The range of all such buckets is collected

            val combinedDF = joinedDataDF
                    .withColumn("basePercentage", col("baseCount") / sum("baseCount")
                            .over())
                    .withColumn("newDataPercentage", col("newDataCount") / sum("newDataCount")
                            .over())
                    .withColumn("psiValue", udfComputeSI(col("newDataPercentage"), col("basePercentage")))
            //The method .over() is used here as a compulsory requirement
            //Since Spark 2.x this is used to specify which of the rows it is acted upon
            // add UDF to compute value

            //the UDF is used to return a new column named "psiValue" containing PSI score for that bucket
            val quantileNumbers = splits.length - 1
            val psiValue = combinedDF
                    .agg(sum("psiValue"))
                    .first()
                    .get(0).asInstanceOf[Double]

            //return value
            (pageNum, psiValue, quantileNumbers, excludedBucketRanges)

        })
        result.toList.toDF("pageNumber", "psiValue", "numberOfBuckets", "excludedRanges")
    }

    /**
     * Module for computing VSI
     * @param baseDataSetDF  DataFrame describing base DataSet
     * @param newDataSetDF   DataFrame describing newer DataSet, generally larger in size than baseDataSet
     * @param variablesArray Array containing all the numberical Variables as Strings
     *
     *                       NOTE:  baseDataDF & newDataDF need not be cached by caller as both DataFrames will be
     *                       cached by the function for optimization
     */
    def vsi(baseDataSetDF: DataFrame, newDataSetDF: DataFrame, variablesArray: Array[String]): DataFrame =
    {
        baseDataSetDF.cache()
        newDataSetDF.cache()
        val result = variablesArray.par.map(variable =>
        {
            val currentVariableBaseDF = baseDataSetDF
                    .groupBy(variable)
                    .count()
            val currentVariableNewDataDF = newDataSetDF
                    .groupBy(variable)
                    .count()

            val combinedDF = currentVariableBaseDF
                    .join(currentVariableNewDataDF, currentVariableNewDataDF(variable) === currentVariableBaseDF
                    (variable))
                    .select(currentVariableNewDataDF(variable), currentVariableBaseDF("count")
                            .as("trainCount"), currentVariableNewDataDF("count")
                            .as("predictCount"))

            val vsiDF = combinedDF
                    .withColumn("trainPercentage", col("trainCount") / sum("trainCount")
                            .over())
                    .withColumn("predictPercentage", col("predictCount") / sum("predictCount")
                            .over())
                    .withColumn("vsiValue", udfComputeSI(col("predictPercentage"), col("trainPercentage")))
            val vsiScore = vsiDF
                    .agg(sum("vsiValue"))
                    .first()
                    .get(0)
                    .asInstanceOf[Double]

            val combinedDFcommonValues = combinedDF
                    .select(variable)
                    .distinct()
                    .collect()
                    .map(r => r.getString(0))
                    .toSet
            val baseDFvalues = currentVariableBaseDF
                    .select(variable)
                    .distinct()
                    .collect()
                    .map(r => r.getString(0)).toSet
            val newDFvalues = currentVariableNewDataDF
                    .select(variable)
                    .distinct()
                    .collect()
                    .map(r => r.getString(0)).toSet

            val missingValues = baseDFvalues
                    .diff(combinedDFcommonValues).toList
            val newValues = newDFvalues
                    .diff(combinedDFcommonValues).toList

            (variable, vsiScore, combinedDFcommonValues.toList, missingValues, newValues)
        })
        result.toList.toDF("variable", "vsiScore", "commonValues", "missingValues", "newValues")
    }
}