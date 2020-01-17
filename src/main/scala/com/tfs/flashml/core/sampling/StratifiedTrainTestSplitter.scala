package com.tfs.flashml.core.sampling

import java.io.Serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

/**
  * This class is used to spit the data into a an array containing train, test dataframes
  * in a stratified fashion (i.e the fraction of each class remains same in both train & test)
  * according to the trainPercent specified
  *
  * @param trainPercent
  * @param responseVariable
  * @param seed
  */
class StratifiedTrainTestSplitter(val trainPercent: Double, val responseVariable: String, val seed: Long) extends
        TrainTestSplitter
        with Serializable
{
    assert(trainPercent < 1, "specify a fraction for train percentage")
    private val log = LoggerFactory.getLogger(getClass)

    /**
      * Returns an array of dataframe with 1st element being train dataframe
      * & 2nd element the test dataframe.
      * Split is done in a stratified fashion
      *
      * @param inputDF
      * @return
      */
    override def split(inputDF: DataFrame): Array[DataFrame] =
    {
        log.info("Generating train/test datasets using stratified sampling.")
        val index = inputDF.schema.fieldIndex(responseVariable)
        val outputRDD = getFractionedRDD(inputDF.rdd, index)
        val trainDF = SparkSession.builder().getOrCreate().createDataFrame(outputRDD, inputDF.schema)
        val testDataset = inputDF.except(trainDF)
        Array(trainDF, testDataset)
    }

    private def getFractionedRDD(input: RDD[Row], index: Int): RDD[Row] =
    {
        val inputRDD = input.keyBy(_.get(index))
        inputRDD.cache()
        val fractions = inputRDD.map(_._1).distinct.map(x => (x, trainPercent)).collectAsMap()
        val outputRDD = inputRDD.sampleByKeyExact(false, fractions, seed = seed).values
        inputRDD.unpersist()
        outputRDD
    }
}
