package com.tfs.flashml.core

import com.tfs.flashml.dal.SavePointManager
import com.tfs.flashml.util.ConfigUtils._
import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigUtils, FlashMLConfig}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * Class for scoring the dataset.
 *
 * @since 28/11/16.
 */

object Predict
{
  private val log = LoggerFactory.getLogger(getClass)

  // The list of columns that needs to be retained.
  val columnsNames = (
    ConfigUtils.primaryKeyColumns ++ Array(ConfigUtils.topVariable,
      ConfigUtils.getIndexedResponseColumn,
      FlashMLConfig.getString(FlashMLConstants.DATE_VARIABLE),
      ConfigUtils.pageColumn,
      ConfigUtils.responseColumn,
      if (ConfigUtils.isMultiIntent) FlashMLConstants.PREDICTION_LABEL else "",
      "rawPrediction",
      if (ConfigUtils.mlAlgorithm.equals(FlashMLConstants.SVM) && !ConfigUtils.isPlattScalingReqd) ""
      else if (ConfigUtils.isUplift) "modelProbability"
      else "probability",
      if (ConfigUtils.isUplift)
        "modelPrediction"
      else "prediction") ++ ConfigUtils.additionalColumns
    )
    .filter(_.nonEmpty)
    .distinct

  def score(omodelArray: Option[Array[PipelineModel]], odfArray: Option[Array[DataFrame]]): Option[Array[DataFrame]] =
  {
    log.info(s"Scoring test/validation dataframe.")
    for
      {
      modelArray <- omodelArray
      dfArray <- odfArray
    }
      yield
        {
          dfArray.tail.map(_.select((ConfigUtils.primaryKeyColumns ++ Array(ConfigUtils.responseColumn,
            FlashMLConstants.FEATURES)).map(col): _*).cache())

          val defaultFilter: String = FlashMLConfig.getString(FlashMLConstants.RESPONSE_VARIABLE) + " is not null"
          val filter: String = if (FlashMLConfig.getString(FlashMLConstants.POST_PREDICT_FILTER).isEmpty)
            defaultFilter
          else FlashMLConfig.getString(FlashMLConstants.POST_PREDICT_FILTER)

          var predictionArray: ArrayBuffer[DataFrame] = ArrayBuffer[DataFrame]()

          if (ConfigUtils.isPageLevelModel)
          {
            for (x <- dfArray.indices)
            {
              predictionArray += modelArray(x % ConfigUtils.numPages).transform(dfArray(x)).filter(filter)
            }
          }
          else
          {
            for (df <- dfArray)
            {
              predictionArray += modelArray(0).transform(df).filter(filter)
            }
          }

          predictionArray
            .foreach(_.select(columnsNames.map(col): _*)
              .persist())

          // SavePoint, if required
          if (ConfigUtils.toSavePoint)
          {
            if (ConfigUtils.isPageLevelModel)
            {
              for (x <- predictionArray.indices)
              {
                SavePointManager.saveDataFrame(predictionArray(x).select(columnsNames.map(col): _*), x % ConfigUtils.numPages + 1, DataSetType(x / ConfigUtils.numPages), FlashMLConstants.SCORING)
              }
            }
            else
            {
              for (x <- predictionArray.indices)
              {
                SavePointManager
                  .saveDataFrame(predictionArray(x)
                    .select(columnsNames.map(col): _*), 0, DataSetType(x), FlashMLConstants.SCORING)
              }
            }
          }

          dfArray
            .tail
            .map(_.unpersist())
          predictionArray.foreach(_.unpersist())
          predictionArray.toArray
        }
  }
}