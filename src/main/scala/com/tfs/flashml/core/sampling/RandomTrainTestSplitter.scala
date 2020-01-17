package com.tfs.flashml.core.sampling

import java.io.Serializable

import org.apache.spark.sql.{DataFrame}

/**
  * This class is used to spit the data into an array containing train, test
  * dataframes according to the trainPercent specified
  *
  * @param trainPercent
  * @param seed
  */
class RandomTrainTestSplitter(val trainPercent: Double, val seed: Long) extends TrainTestSplitter
        with Serializable
{
    assert(trainPercent < 1, "specify a fraction for train percentage")

    /**
      * Returns an array of dataframe with 1st element being train dataframe
      * & 2nd element the test dataframe.
      *
      * @param inputDF
      * @return
      */
    override def split(inputDF: DataFrame): Array[DataFrame] =
    {
        val testPercent = 1 - trainPercent
        val trainTestSplit = Array(trainPercent, testPercent)
        inputDF.randomSplit(trainTestSplit, seed)
    }
}
