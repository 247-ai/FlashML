package com.tfs.flashml.publish.preprocessing

import com.tfs.flashml.core.DirectoryCreator
import com.tfs.flashml.core.featuregeneration.transformer.SkipGramGenerator
import com.tfs.flashml.core.preprocessing.transformer._
import com.tfs.flashml.util.FlashMLConfig
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.spark.ml.feature.{ImputerCustom, RegexTokenizer, StopWordsRemoverCustom}
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Class for publishing the preprocessing steps.
 *
 * @since 3/23/17.
 */
object PreprocessingPublisher
{

  private val log = LoggerFactory.getLogger(getClass)

  private var sentMarkerFunction = true
  private var imputerFunction = true
  private var stopWordsFunction = true
  private var nGramFunction = true
  private var andRulesFunction = true
  private var contReplFunction = true
  private var caseNormFunction = true

  private val tokenizerPattern = "(regexTok.*)".r
  private val imputerPattern = "(imputerCustom.*)".r
  private val stopWordsProcessorPattern = "(stopWords.*)".r
  private val porterStemmingPattern = "(PorterStemming.*)".r
  private val contractionReplacementPattern = "(ContractionsReplacement.*)".r
  private val wordClassesReplacementPattern = "(wordClassesReplacement.*)".r
  private val nGramPattern = "(nGram.*)".r
  private val caseNormPattern = "(caseNorm.*)".r
  private val sentenceMarkerPattern = "(sentenceMarker.*)".r
  private val andRulesPattern = "(AndRulesGenerator.*)".r
  private val regexReplacement = "(regexRepl.*)".r
  private val regexRemoval = "(regexRem.*)".r

  /**
   * Method to generate the JS code for each page.
   *
   * @param pageNumber
   * @return
   */
  def generateJS(pageNumber: Int, globalVar: mutable.Set[String]): StringBuilder =
  {
    sentMarkerFunction = true
    stopWordsFunction = true
    nGramFunction = true
    andRulesFunction = true
    contReplFunction = true
    imputerFunction = true
    caseNormFunction = true
    val preProcessingJS = new StringBuilder
    preProcessingJS ++= ""
    val pageString: String = if (pageNumber == 0) "noPage"
    else "page" + pageNumber

    val pipelinePath = getPipelinePath(pageString)

    val preProcessingPipeline = PipelineModel
      .load(FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + pipelinePath.toString)

    val stages = preProcessingPipeline.stages

    val pattern: String = stages.last match
    {
      case tokenizerStage: RegexTokenizer => tokenizerStage.getPattern
      case _ => ""
    }

    //Mention that listed method is not supported
    //We check the stage if it matches the pipeline Transformer from Preprocessing Module
    for (stage <- stages)
    {
      val stageJS = stage
      match
      {
        case _: RegexTokenizer => getTokenizerJS(stage)
        case _: ImputerCustom => getImputerJS(stage, globalVar)
        case _: StopWordsRemoverCustom => getStopWordsProcessorJS(stage, pattern)
        //case porterStemmingPattern(stemmer) => getPorterStemmingJS(stage, pattern)
        case _: SentenceMarker => getSentenceMarkerJS(stage, globalVar)
        case _: CaseNormalizationTransformer => getCaseNormalizationJS(stage, globalVar)
        case _: SkipGramGenerator => getAndRulesJS(stage, globalVar)
        //case regexReplacement(_) | regexRemoval(_) => getRegexReplacementJS(stage, globalVar)
        case _: RegexReplacementTransformer => getRegexReplacementJS(stage, globalVar)
        case op => throw new UnsupportedOperationException(s" Unsupported publish operation $op")
      }
      preProcessingJS ++= stageJS
    }
    preProcessingJS
  }

  private def getPipelinePath(pageString: String) =
  {
    DirectoryCreator
      .getModelPath()
      .toString + s"/$pageString/noSegment/pipeline/${FlashMLConstants.PREPROCESSING}_pipeline"
  }

  private def getTokenizerJS(stage: Transformer) =
  {

    val tokenizerStage = stage.asInstanceOf[RegexTokenizer]
    val pattern = tokenizerStage.getPattern
    val inputCol = tokenizerStage.getInputCol
    val outputCol = tokenizerStage.getOutputCol
    val tokenizerJS = TokenizerPublisher.generateJS(pattern, inputCol, outputCol)
    tokenizerJS
  }

  private def getImputerJS(stage: Transformer, globalVar: mutable.Set[String]) =
  {

    val imputerStage = stage.asInstanceOf[ImputerCustom]
    val replaceValue = imputerStage.getReplacementValue
    val inputCol = imputerStage.getInputCol
    val imputerJS = ImputerPublisher.generateJS(replaceValue, inputCol, imputerFunction, globalVar)
    imputerFunction = false
    imputerJS
  }

  private def getCaseNormalizationJS(stage: Transformer, globalVar: mutable.Set[String]) =
  {

    val caseNormalizationStage = stage.asInstanceOf[CaseNormalizationTransformer]
    val inputCol = caseNormalizationStage.getInputCol
    val outputCol = caseNormalizationStage.getOutputCol
    val caseNormJS = CaseNormalizer.generateJS(inputCol, outputCol, caseNormFunction, globalVar)
    caseNormFunction = false
    caseNormJS
  }

  private def getStopWordsProcessorJS(stage: Transformer, pattern: String) =
  {

    val stopWordsProcessorStage = stage.asInstanceOf[StopWordsRemoverCustom]
    val stopWords = stopWordsProcessorStage.getStopWords
    val inputCol = stopWordsProcessorStage.getInputCol
    val outputCol = stopWordsProcessorStage.getOutputCol
    val stopWordsProcessorJS = StopWordsProcessorPublisher
      .generateJS(stopWords, inputCol, outputCol, stopWordsFunction, pattern)
    stopWordsFunction = false
    stopWordsProcessorJS
  }

  private def getPorterStemmingJS(stage: Transformer, pattern: String) =
  {

    val stemmingStage = stage.asInstanceOf[PorterStemmingTransformer]
    val inputCol = stemmingStage.getInputCol
    val outputCol = stemmingStage.getOutputCol
    val stemmingJS = PorterStemmingPublisher.generateJS(inputCol, outputCol, pattern)
    stemmingJS
  }

  private def getSentenceMarkerJS(stage: Transformer, globalVar: mutable.Set[String]) =
  {

    val sentenceMarkerStage = stage.asInstanceOf[SentenceMarker]
    val inputCol = sentenceMarkerStage.getInputCol
    val outputCol = sentenceMarkerStage.getOutputCol
    val sentenceMarkerJS = SentenceMarkerPublisher.generateJS(inputCol, outputCol, sentMarkerFunction, globalVar)
    sentMarkerFunction = false
    sentenceMarkerJS
  }

  private def getAndRulesJS(stage: Transformer, globalVar: mutable.Set[String]) =
  {

    val andRulesStage = stage
      .asInstanceOf[SkipGramGenerator]
    val inputCol = andRulesStage.getInputCol
    val outputCol = andRulesStage.getOutputCol
    val windowSize = andRulesStage.getWindowSize
    val andRulesJS = SkipGramPublisher.generateJS(windowSize, inputCol, outputCol, andRulesFunction, globalVar)
    andRulesFunction = false
    andRulesJS
  }

  private def getRegexReplacementJS(stage: Transformer, globalVar: mutable.Set[String]): StringBuilder =
  {
    val regexReplacementStage = stage.asInstanceOf[RegexReplacementTransformer]
    val inputCol = regexReplacementStage.getInputCol
    val outputCol = regexReplacementStage.getOutputCol
    val regexPatterns = regexReplacementStage.getRegexReplacements
    //stored in format (Label,RegexPattern)
    RegexReplacementPublisher.generateJS(inputCol, outputCol, regexPatterns, globalVar)
  }
}
