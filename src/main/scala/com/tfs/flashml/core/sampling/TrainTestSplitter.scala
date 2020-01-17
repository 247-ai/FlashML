package com.tfs.flashml.core.sampling

import org.apache.spark.sql.{DataFrame}

trait TrainTestSplitter
{
    /**
      * Given a dataframe, splits the data into train,test &
      * returns an array containing them
      *
      * @param inputDF
      * @return
      */
    def split(inputDF: DataFrame): Array[DataFrame]

}
