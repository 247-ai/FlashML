package com.tfs.flashml.publish.featureGeneration

import com.tfs.flashml.core.DirectoryCreator
import com.tfs.flashml.core.featuregeneration.transformer.CategoricalColumnsTransformer
import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigValues, FlashMLConfig, PublishUtils}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.Bucketizer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object FeatureGenerationPublisher {

  private var writeBucketizerFunction = true

  def generateJS(pageNumber: Int, globalVar:mutable.Set[String]): StringBuilder = {

    val featureGeneratedJS = new StringBuilder

    /*
    if(ConfigUtils.featureGenerationBinningConfig.isEmpty && ConfigUtils.featureGenerationGramsConfig.asInstanceOf[Array[Any]].isEmpty)
        return featureGeneratedJS
    */

    val pageString: String = if (pageNumber == 0) "noPage" else "page" + pageNumber

    val pipelinePath = DirectoryCreator
      .getModelPath() + s"/$pageString/noSegment/pipeline/featureGeneration_pipeline"

    val featureGenerationPipeline: PipelineModel = PipelineModel
      .load(FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + pipelinePath)

    globalVar += ""+BucketizerPublisher.getBucketizerJSFunction

    featureGenerationPipeline
      .stages
      .foreach { stage =>
      val stageJS = stage match {
        case bucketizer: Bucketizer => getBucketizerJS(bucketizer)
        case categoricalColumnsTransformer: CategoricalColumnsTransformer =>
          getCategoricalColumnsTransformerJS(categoricalColumnsTransformer)
        case op => throw new UnsupportedOperationException(s"The operation ${op.getClass} is not supported in js publish")
      }
      featureGeneratedJS ++= stageJS
    }
    featureGeneratedJS
  }

  private def getBucketizerJS(stage: Bucketizer): StringBuilder = {
    val bucket = stage.asInstanceOf[Bucketizer]
    val inputCol = bucket.getInputCol
    val outputCol = bucket.getOutputCol
    val splits = bucket.getSplits
    val bucketizerJS = BucketizerPublisher.generateJS(inputCol, outputCol, splits)

    //writeBucketizerFunction = false
    bucketizerJS
  }

  private def getCategoricalColumnsTransformerJS(stage: CategoricalColumnsTransformer): StringBuilder = {
    val inputCols = stage.getInputCols
    val categoricalArrayJS = new StringBuilder
    val categoricalArray = new ArrayBuffer[String]()
    categoricalArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigValues.defaultIndent + 1) + "var " + FlashMLConstants.CATEGORICAL_ARRAY + " = " + "["
    for (i <- inputCols)
    {
      categoricalArray += "'" + i + "_' + " + i
    }
    categoricalArrayJS ++= categoricalArray.mkString(", ")
    categoricalArrayJS ++= "];"
  }
}
