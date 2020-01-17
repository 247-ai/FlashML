package com.tfs.flashml.publish.featureengineering

import com.tfs.flashml.core.DirectoryCreator
import com.tfs.flashml.publish.Publisher
import com.tfs.flashml.util.FlashMLConfig
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.feature.{CountVectorizerModel, HashingTF, VectorAssembler}
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Class for publishing the vectorization steps.
  *
  * @since 3/23/17.
  */
object VectorizationPublisher
{
    private var hashFunction: Boolean = true
    private var cvFunction: Boolean = true
    private val hashingTFPattern = "(hashing.*)".r
    private val countVectorizerPattern = "(cntVec.*)".r
    private val vecAssemblerPattern = "(vecAssembler.*)".r
    private val log = LoggerFactory.getLogger(getClass)

    /**
      * Method to generate the JS code for each page.
      *
      * @param pageNumber
      * @return
      */
    def generateJS(pageNumber: Int, globalVar:mutable.Set[String]): StringBuilder =
    {
        hashFunction = true
        cvFunction = true
        val featureEngineerJS = new StringBuilder
        hashFunction = true
        val pageString: String = if (pageNumber == 0) "noPage"
        else "page" + pageNumber

        val pipelinePath = getVectorizationPath(pageString)

        val featureEngineeringPipeline = PipelineModel.load(FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + pipelinePath)

        val stages = featureEngineeringPipeline.stages
        for (stage <- stages)
        {
            val stageJS = stage.toString match
            {
                case hashingTFPattern(hashing) => getHashingTFJS(stage, globalVar)
                case countVectorizerPattern(cntVec) => getCountVectorizerJS(stage, globalVar)
                case vecAssemblerPattern(vecAssembler) => getVectorAssemblerJS(stage)
                case _ => new StringBuilder
            }
            featureEngineerJS ++= stageJS
        }
        featureEngineerJS
    }

    private def getVectorizationPath(pageString: String) =
    {
        DirectoryCreator.getModelPath() + s"/$pageString/noSegment/pipeline/vectorization_pipeline"
    }

    private def getHashingTFJS(stage: Transformer, globalVar:mutable.Set[String]): StringBuilder =
    {
        val hashingTFStage = stage.asInstanceOf[HashingTF]
        val numFeatures = hashingTFStage.getNumFeatures
        val inputCol = hashingTFStage.getInputCol
        val outputCol = hashingTFStage.getOutputCol
        val binarizer = hashingTFStage.getBinary
        val hashingTFJS = HashingTFPublisher.generateJS(numFeatures, inputCol, outputCol, binarizer, hashFunction, globalVar)
        hashFunction = false
        hashingTFJS
    }

    private def getCountVectorizerJS(stage: Transformer, globalVar:mutable.Set[String]): StringBuilder =
    {

        val countVectorizerStage = stage.asInstanceOf[CountVectorizerModel]
        val vocabulary = countVectorizerStage.vocabulary
        val inputCol = countVectorizerStage.getInputCol
        val outputCol = countVectorizerStage.getOutputCol
        val binarizer = countVectorizerStage.getBinary
        val vocabSize = countVectorizerStage.getVocabSize
        val countVectorizerJS = CountVectorizerPublisher.generateJS(vocabulary, inputCol, outputCol, binarizer,
            vocabSize, cvFunction, globalVar)

        // Removing this to add CountVectorizer declaration for all pages
        // cvFunction = false
        countVectorizerJS
    }

    private def getVectorAssemblerJS(stage: Transformer): StringBuilder =
    {

        val vectorAssemblerStage = stage.asInstanceOf[VectorAssembler]
        val inputCols = vectorAssemblerStage.getInputCols
        val outputCol = vectorAssemblerStage.getOutputCol
        val countVectorizerJS = VectorAssemblerPublisher.generateJS(inputCols, outputCol)
        countVectorizerJS
    }

}
