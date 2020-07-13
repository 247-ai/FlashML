package com.tfs.flashml.publish

import java.io.File
import java.nio.file.Files

import com.tfs.flashml.core.featuregeneration.FeatureGenerationEngine
import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.core.{DirectoryCreator, VectorizationEngine}
import com.tfs.flashml.core.modeltraining.ModelTrainingEngine
import com.tfs.flashml.core.preprocessing.PreprocessingEngine
import com.tfs.flashml.dal.SavePointManager
import com.tfs.flashml.publish.transformer.HotleadTransformer
import com.tfs.flashml.util.{ConfigUtils, FlashMLConfig}
import ml.combust.bundle.BundleFile
import ml.combust.mleap.spark.SparkSupport._
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, concat, udf}
import org.slf4j.LoggerFactory
import resource._

import scala.collection.mutable.ArrayBuffer

/**
  * Main class for publishing models for production deployment.
  *
  * @since 28/11/16.
  */
object Publish {

  private val log = LoggerFactory.getLogger(getClass)

  /**
    * Method to generate JS files
    */
  def generateJS(): Unit = {
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

    /**
      * Method to generate Mleap bundle
      *
      * @return
      */

    def generateMleap(mleapTrainDF:Option[Array[DataFrame]]) = {
      /**
        * We are loading the pipeline stages of each step to combine it as a single pipeline.
        */
      val thresholds = if(ConfigUtils.isSingleIntent)FlashMLConfig.getDoubleArray(FlashMLConstants.PUBLISH_THRESHOLDS) else Array[Double]()
      val configTopThresholds = if(ConfigUtils.isSingleIntent)FlashMLConfig.getIntArray(FlashMLConstants.TOP_THRESHOLDS) else Array[Int]()
      val topVariable = if(ConfigUtils.isSingleIntent)FlashMLConfig.getString(FlashMLConstants.TOP_VARIABLE) else ""
      val customMetricsType = if(ConfigUtils.isSingleIntent)FlashMLConfig.getString(FlashMLConstants.CUSTOM_METRICS_TYPE) else ""
      val nPages = if(ConfigUtils.isSingleIntent)FlashMLConfig.getInt(FlashMLConstants.N_PAGES) else 0
      val topThresholds = 1.to(nPages).map(i =>
      {
        if (configTopThresholds.isEmpty || customMetricsType.equals(FlashMLConstants.PROB_ONLY_CUSTOM_METRICS)) 0
        else
          configTopThresholds(i - 1).toDouble
      }).toArray
      val inputCols = Array(FlashMLConfig.getString(FlashMLConstants.PAGE_VARIABLE),"probability") ++ Array(if(!customMetricsType.equals(FlashMLConstants.PROB_ONLY_CUSTOM_METRICS)) topVariable else "").filter(_.nonEmpty).distinct
      val mleapPagePipeline = new ArrayBuffer[Pipeline]
      if (ConfigUtils.isPageLevelModel) {
        (1 to ConfigUtils.numPages).foreach { pageIndex =>
          val hotleadModel:PipelineModel = if(ConfigUtils.isSingleIntent) {
            val allStages = ArrayBuffer[PipelineStage]()
            allStages += new HotleadTransformer()
              .setInputCols(inputCols)
              .setOutputCol("isHotLead")
              .setThreshold(thresholds)
              .setTopThresholds(topThresholds)
              .setNPages(nPages)
            val hotleadPipeline = new Pipeline().setStages(allStages.toArray)
            hotleadPipeline.fit(mleapTrainDF.get(pageIndex - 1))
          } else null
          mleapPagePipeline += new Pipeline().setStages(PreprocessingEngine.loadPipelineModel(pageIndex).stages ++
            (if(!FeatureGenerationEngine.loadPipelineModel(0).stages.isEmpty)
              FeatureGenerationEngine.loadPipelineModel(0).stages else Array[Transformer]()) ++
          VectorizationEngine.loadPipelineModel(pageIndex).stages ++
          ModelTrainingEngine.loadPipelineModel(pageIndex).stages.drop(1) ++
          (if(ConfigUtils.isSingleIntent)
            hotleadModel.stages else Array[Transformer]()))
        }
      } else {
        val hotleadModel:PipelineModel = if(ConfigUtils.isSingleIntent) {
          val allStages = ArrayBuffer[PipelineStage]()
          allStages += new HotleadTransformer()
            .setInputCols(inputCols)
            .setOutputCol("isHotLead")
            .setThreshold(thresholds)
            .setTopThresholds(topThresholds)
            .setNPages(nPages)
          val hotleadPipeline = new Pipeline().setStages(allStages.toArray)
          hotleadPipeline.fit(mleapTrainDF.get(0))
        }else null
        mleapPagePipeline += new Pipeline().setStages(PreprocessingEngine.loadPipelineModel(0).stages ++
          (if(!FeatureGenerationEngine.loadPipelineModel(0).stages.isEmpty)
            FeatureGenerationEngine.loadPipelineModel(0).stages else Array[Transformer]()) ++
          VectorizationEngine.loadPipelineModel(0).stages ++
          ModelTrainingEngine.loadPipelineModel(0).stages.drop(1) ++
          (if(ConfigUtils.isSingleIntent)
            hotleadModel.stages else Array[Transformer]()))
      }

      if (ConfigUtils.isPageLevelModel) {
        (0 until ConfigUtils.numPages).foreach(pageNum => {
          saveMleapBundle(mleapTrainDF.get(pageNum),pageNum)
        })
      }
      else {
        saveMleapBundle(mleapTrainDF.get(0))
      }

      def saveMleapBundle(df: DataFrame,pageNum: Int=0) = {
        // We need to fit the entire pipeline once again to make it serialized in mleap. Because our pipeline stages are in seperate modules.
        // So we need to combine it to one pipeline before serializing.
        val mleapModel = mleapPagePipeline(pageNum).fit(df)
        // We have to transform the pipleine with the dataframe to get the schema of the transformation stages.
        val sbc = SparkBundleContext().withDataset(mleapModel.transform(df.limit(1).toDF()))
        val folder = Files.createTempDirectory("FlashMLTemp")
        val pageString: String = if (!ConfigUtils.isPageLevelModel) "noPage" else "page" + (pageNum + 1)
        val file = new File(s"$folder/flashml-$pageString.zip")
        for (bf <- managed(BundleFile(s"jar:file:${file.getPath}"))) {
          mleapModel.writeBundle.save(bf)(sbc).get
        }
        ConfigUtils.fs.delete(new Path(DirectoryCreator.getBasePath + s"/flashml-$pageString.zip"),true)
        ConfigUtils.fs.copyFromLocalFile(new Path(s"${file.getPath}"), new Path(DirectoryCreator.getBasePath + s"/flashml-$pageString.zip"))
        if (file.exists()) file.delete()
        if (folder.toFile.exists) folder.toFile.delete()
        log.info(s"Model saved in hadoop at [${DirectoryCreator.getBasePath}" + s"/flashml-$pageString.zip]")
      }

      //def saveMleapBundleNonPage = saveMleapBundlePageLevel(_:DataFrame)(0)
    }

  def generateQAData(): Unit = {
    log.info("Starting QA data generation")
    val spark = SparkSession.builder().getOrCreate()
    val qaPath = DirectoryCreator.getQAPath()
    DirectoryCreator.deleteDirectory(qaPath)
    val primaryKeyVariable = FlashMLConfig.getStringArray(FlashMLConstants.PRIMARY_KEY)
    val inputColumnsArray = ConfigUtils.columnVariables.distinct.filterNot(_.isEmpty)
    val pageVariable: String = FlashMLConfig.getString(FlashMLConstants.PAGE_VARIABLE)

    val predictDF = if (ConfigUtils.isPageLevelModel) {
      //We are loading the page1 df first to get the visitors from the first page. Because only the first page will have all the visitors session available.
      var loadPath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + DirectoryCreator.getBasePath + "/" +
        s"/page1/noSegment/data/scoringTrain" + "/*.gz.parquet"

      val inputDF: DataFrame = SavePointManager.loadInputData
      val pageDF: DataFrame = spark.read
        .load(loadPath)
        .withColumn("visitors", concat(primaryKeyVariable.map(col): _*))

      if(!FlashMLConfig.hasKey(FlashMLConstants.DATE_VARIABLE) || FlashMLConfig.getString(FlashMLConstants.DATE_VARIABLE).isEmpty){

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

        for (pageNum <- 2 to FlashMLConfig.getInt(FlashMLConstants.EXPERIMENT_NUMBER_OF_PAGES)) {
          loadPath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + DirectoryCreator.getBasePath + "/" +
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
      else{
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

        for (pageNum <- 2 to FlashMLConfig.getInt(FlashMLConstants.EXPERIMENT_NUMBER_OF_PAGES)) {
          loadPath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + DirectoryCreator.getBasePath + "/" +
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

        //Load Input df for getting the input columns

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
    //Non page level model
    else {
      val loadPath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + DirectoryCreator.getBasePath +
        s"/noPage/noSegment/data/scoringTrain" + "/*.gz.parquet"

      val inputDF = SavePointManager
        .loadInputData

      val df = spark.read
        .load(loadPath)
        .withColumn("visitors", concat(primaryKeyVariable
          .map(col): _*))

      val visitorArray = if(!FlashMLConfig.hasKey(FlashMLConstants.DATE_VARIABLE) || FlashMLConfig.getString(FlashMLConstants.DATE_VARIABLE).isEmpty){
        //Absence of date variable
        df
          .select("visitors")
          .distinct
          .limit(FlashMLConfig.getInt(FlashMLConstants.QA_DATAPOINTS))
          .collect
          .map(r => r.get(0))

      }
      else{
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

    if (qaFormat.equals("csv")) {
      val csvPath = new Path(qaPath, "csv")
      testingInfraDF
        .coalesce(1)
        .write
        .option("header", "true")
        .csv(FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) +
          "/" + csvPath.toString)
    }
    else if (qaFormat.equals("json")) {
      val jsPath = new Path(qaPath, "json")
      testingInfraDF
        .coalesce(1)
        .write
        .json(FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + jsPath.toString)
    }

    log.info("Saved QA Data in HDFS qa directory of project structure")
  }

}