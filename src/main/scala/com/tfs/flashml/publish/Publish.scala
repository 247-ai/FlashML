package com.tfs.flashml.publish

import com.tfs.flashml.core.DirectoryCreator
import com.tfs.flashml.dal.SavePointManager
import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigUtils, FlashMLConfig}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.functions.{col, concat, udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Main class for publishing models for production deployment.
 *
 * @since 28/11/16.
 */
object Publish
{

    private val log = LoggerFactory.getLogger(getClass)

    /**
     * Method to generate JS files
     */
    def generateJS(): Unit =
    {
        log.info("Generating JavaScript files")
        val ss = SparkSession.builder().getOrCreate()
        val JS = PublishAssembler.generateJS
        val jsPath = new Path(DirectoryCreator.getPublishPath, "js")
        DirectoryCreator.deleteDirectory(jsPath)

        // Write the JS file
        val finalPath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + jsPath.toString
        ss.sparkContext.parallelize(Seq(JS), 1).saveAsTextFile(finalPath)
        log.info(s"Saved Javascript files at [$finalPath]")
    }

    def generateQAData(): Unit =
    {
        log.info("Starting QA data generation")
        val spark = SparkSession.builder().getOrCreate()
        val qaPath = DirectoryCreator.getQAPath()
        DirectoryCreator.deleteDirectory(qaPath)
        val primaryKeyVariable = FlashMLConfig.getStringArray(FlashMLConstants.PRIMARY_KEY)
        val inputColumnsArray = ConfigUtils.columnVariables.distinct.filterNot(_.isEmpty)
        val pageVariable: String = FlashMLConfig.getString(FlashMLConstants.PAGE_VARIABLE)

        val predictDF = if (ConfigUtils.isPageLevelModel)
        {
            //We are loading the page1 df first to get the visitors from the first page. Because only the first page
            // will have all the visitors session available.
            var loadPath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + DirectoryCreator
                    .getBasePath + "/" +
                    s"/page1/noSegment/data/scoringTrain" + "/*.gz.parquet"

            val inputDF: DataFrame = SavePointManager.loadInputData
            val pageDF: DataFrame = spark
                    .read
                    .load(loadPath)
                    .withColumn("visitors", concat(primaryKeyVariable.map(col): _*))

            if (!FlashMLConfig.hasKey(FlashMLConstants.DATE_VARIABLE) || FlashMLConfig.getString(FlashMLConstants
                    .DATE_VARIABLE).isEmpty)
            {

                val visitorArray = pageDF
                        .select("visitors")
                        .distinct
                        .limit(FlashMLConfig.getInt(FlashMLConstants.QA_DATAPOINTS))
                        .collect
                        .map(r => r.get(0))

                var filteredDF = pageDF
                        .filter(col("visitors")
                                .isin(visitorArray: _*))
                        .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))
                        .select(
                            Array("qaJoinKey", "probability")
                                    .map(col): _*)

                for (pageNum <- 2 to FlashMLConfig.getInt(FlashMLConstants.EXPERIMENT_NUMBER_OF_PAGES))
                {
                    loadPath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + DirectoryCreator
                            .getBasePath + "/" +
                            s"/page" + pageNum + "/noSegment/data/scoringTrain" + "/*.gz.parquet"

                    val filteredTempDF = spark
                            .read
                            .load(loadPath)
                            .withColumn("visitors", concat(primaryKeyVariable.map(col): _*))
                            .filter(col("visitors").isin(visitorArray: _*))
                            .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))
                            .select(Array("qaJoinKey", "probability").map(col): _*)

                    filteredDF = filteredDF.union(filteredTempDF)

                }

                val filteredInputDF = inputDF
                        .withColumn("visitors", concat(primaryKeyVariable.map(col): _*))
                        .filter(col("visitors").isin(visitorArray: _*))
                        .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))

                filteredDF
                        .as("A")
                        .join(filteredInputDF.as("B"), col("A.qaJoinKey") === col("B.qaJoinKey"))
                        .select(inputColumnsArray
                                .map(x => col(s"B.$x")) ++ Array(col("A.probability")): _*)

            }
            else
            {
                //There is a Date Variable
                val dateVariable: String = FlashMLConfig.getString(FlashMLConstants.DATE_VARIABLE)

                val dfHavingMaxCount: String =
                    pageDF
                            .groupBy(dateVariable)
                            .count()
                            .orderBy(desc("count"))
                            .head()
                            .getAs(dateVariable)

                val visitorArray = pageDF
                        .filter(pageDF(dateVariable) === dfHavingMaxCount)
                        .select("visitors")
                        .distinct
                        .limit(FlashMLConfig.getInt(FlashMLConstants.QA_DATAPOINTS))
                        .collect
                        .map(r => r.get(0))

                var filteredDF = pageDF
                        .filter(col("visitors")
                                .isin(visitorArray: _*))
                        .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))
                        .select(
                            Array("qaJoinKey", "probability")
                                    .map(col): _*)

                for (pageNum <- 2 to FlashMLConfig.getInt(FlashMLConstants.EXPERIMENT_NUMBER_OF_PAGES))
                {
                    loadPath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + DirectoryCreator
                            .getBasePath + "/" +
                            s"/page" + pageNum + "/noSegment/data/scoringTrain" + "/*.gz.parquet"

                    val filteredTempDF = spark
                            .read
                            .load(loadPath)
                            .withColumn("visitors", concat(primaryKeyVariable.map(col): _*))
                            .filter(col("visitors").isin(visitorArray: _*))
                            .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))
                            .select(Array("qaJoinKey", "probability").map(col): _*)

                    filteredDF = filteredDF
                            .union(filteredTempDF)

                }

                // Load Input df for getting the input columns
                val filteredInputDF = inputDF
                        .withColumn("visitors", concat(primaryKeyVariable.map(col): _*))
                        .filter(col("visitors").isin(visitorArray: _*))
                        .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))

                filteredDF
                        .as("A")
                        .join(filteredInputDF.as("B"), col("A.qaJoinKey") === col("B.qaJoinKey"))
                        .select(inputColumnsArray
                                .map(x => col(s"B.$x")) ++ Array(col("A.probability")): _*)

            }

        }
        // Non page level model
        else
        {
            val loadPath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + DirectoryCreator
                    .getBasePath +
                    s"/noPage/noSegment/data/scoringTrain" + "/*.gz.parquet"

            val inputDF = SavePointManager
                    .loadInputData

            val df = spark.read
                    .load(loadPath)
                    .withColumn("visitors", concat(primaryKeyVariable
                            .map(col): _*))

            val visitorArray = if (!FlashMLConfig.hasKey(FlashMLConstants.DATE_VARIABLE) || FlashMLConfig.getString
            (FlashMLConstants.DATE_VARIABLE).isEmpty)
            {
                //Absence of date variable
                df
                        .select("visitors")
                        .distinct
                        .limit(FlashMLConfig.getInt(FlashMLConstants.QA_DATAPOINTS))
                        .collect
                        .map(r => r.get(0))

            }
            else
            {
                //Presence of date variable
                val dateVariable: String = FlashMLConfig.getString(FlashMLConstants.DATE_VARIABLE)

                val dfHavingMaxCount: String =
                    df
                            .groupBy(dateVariable)
                            .count()
                            .orderBy(desc("count"))
                            .head()
                            .getAs(dateVariable)

                df
                        .filter(df(dateVariable) === dfHavingMaxCount)
                        .select("visitors")
                        .distinct
                        .limit(FlashMLConfig.getInt(FlashMLConstants.QA_DATAPOINTS))
                        .collect
                        .map(r => r.get(0))

            }

            val filteredDF = df
                    .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))
                    .filter(col("visitors").isin(visitorArray: _*))
                    .select(Array("qaJoinKey", "probability").map(col): _*)

            val filteredInputDF = inputDF
                    .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))
                    .withColumn("visitors", concat(primaryKeyVariable.map(col): _*))
                    .filter(col("visitors").isin(visitorArray: _*))


            filteredDF
                    .as("A")
                    .join(filteredInputDF.as("B"), col("A.qaJoinKey") === col("B.qaJoinKey"))
                    .select(
                        inputColumnsArray
                                .map(x => col(s"B.$x")) ++ Array(col("A.probability")): _*)

        }

        val posProbUDF = udf((a: DenseVector) => a(1))

        val outputCol = ConfigUtils.getColumnNamesVariablesPublishPageLevel(0)

        val testingInfraDF = predictDF
                .withColumn("positive_probability", posProbUDF(col("probability")))
                .select((inputColumnsArray ++ Array("positive_probability"))
                        .distinct
                        .map(col): _*)

        val qaFormat = FlashMLConfig
                .getString(FlashMLConstants.QA_FORMAT)
                .toLowerCase

        if (qaFormat.equals("csv"))
        {
            val csvPath = new Path(qaPath, "csv")
            testingInfraDF
                    .coalesce(1)
                    .write
                    .option("header", "true")
                    .csv(FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) +
                            "/" + csvPath.toString)
        }
        else if (qaFormat.equals("json"))
        {
            val jsPath = new Path(qaPath, "json")
            testingInfraDF
                    .coalesce(1)
                    .write
                    .json(FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + jsPath.toString)
        }

        log.info("Saved QA Data in HDFS qa directory of project structure")
    }

}