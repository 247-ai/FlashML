package com.tfs.flashml.core

import com.tfs.flashml.dal.SavePointManager
import com.tfs.flashml.util.ConfigValues._
import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigValues, FlashMLConfig}
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
            ConfigValues.primaryKeyColumns ++ Array(ConfigValues.topVariable,
                ConfigValues.getIndexedResponseColumn,
                FlashMLConfig.getString(FlashMLConstants.DATE_VARIABLE),
                ConfigValues.pageColumn,
                ConfigValues.responseColumn,
                if (ConfigValues.isMultiIntent) FlashMLConstants.PREDICTION_LABEL
                else "",
                "rawPrediction",
                if (ConfigValues.mlAlgorithm.equals(FlashMLConstants.SVM) && !ConfigValues.isPlattScalingReqd) ""
                else if (ConfigValues.isUplift) "modelProbability"
                else "probability",
                if (ConfigValues.isUplift)
                    "modelPrediction"
                else "prediction") ++ ConfigValues.additionalColumns
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
                    dfArray.tail.map(_.select((ConfigValues.primaryKeyColumns ++ Array(ConfigValues.responseColumn,
                        FlashMLConstants.FEATURES)).map(col): _*).cache())

                    val defaultFilter: String = FlashMLConfig.getString(FlashMLConstants.RESPONSE_VARIABLE) + " is not null"
                    val filter: String = if (FlashMLConfig.getString(FlashMLConstants.POST_PREDICT_FILTER).isEmpty)
                        defaultFilter
                    else FlashMLConfig.getString(FlashMLConstants.POST_PREDICT_FILTER)

                    var predictionArray: ArrayBuffer[DataFrame] = ArrayBuffer[DataFrame]()

                    if (ConfigValues.isPageLevelModel)
                    {
                        for (x <- dfArray.indices)
                        {
                            predictionArray += modelArray(x % ConfigValues.numPages).transform(dfArray(x)).filter(filter)
                        }
                    }
                    else
                    {
                        for (df <- dfArray)
                        {
                            predictionArray += modelArray(0).transform(df).filter(filter)
                        }
                    }

                    predictionArray.foreach(_.select(columnsNames.map(col): _*).persist())

                    // SavePoint, if required
                    if (ConfigValues.toSavePoint)
                    {
                        if (ConfigValues.isPageLevelModel)
                        {
                            for (x <- predictionArray.indices)
                            {
                                SavePointManager.saveDataFrame(predictionArray(x).select(columnsNames.map(col): _*), x % ConfigValues.numPages + 1, DataSetType(x / ConfigValues.numPages), FlashMLConstants.SCORING)
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