package com.tfs.flashml.util.conf

import com.tfs.flashml.core.featuregeneration.FeatureGenerationEngine
import com.tfs.flashml.core.modeltraining.ModelTrainingEngine
import com.tfs.flashml.core.preprocessing.PreprocessingEngine
import com.tfs.flashml.core.sampling.TrainTestSampler
import com.tfs.flashml.core.{Validator, VectorizationEngine}
import com.tfs.flashml.util.ConfigValues._
import com.tfs.flashml.util.{FlashMLConfig, Json}
import org.slf4j.LoggerFactory

import scala.io.Source

object ConfigValidator extends Validator
{
    private val log = LoggerFactory.getLogger(getClass)

    case class ConfigItem(path: String, pattern: String, range: String, canBeEmpty: Boolean, dataType: String)

    /**
      * Validate the flashml config
      */
    override def validate(): Unit =
    {
        if (isPageLevelModel && numPageConfig.isEmpty)
        {
            val msg = s"Number of Pages needs to be provided in page level models."
            throw new ConfigValidatorException(msg)
        }

        if ((!isPageLevelModel || numPages == 1) && variablesScope != FlashMLConstants.SCOPE_PARAMETER_NO_PAGE)
        {
            val msg = s"Entered Scope parameters not possible with Page Level entered user option!"
            throw new ConfigValidatorException(msg)
        }
        validateConfigStepwise()
    }

    /**
      * Validate the flashml config based on pipeline steps
      */
    private def validateConfigStepwise() =
    {
        val steps: Array[String] = FlashMLConfig
                .getStringArray(FlashMLConstants.PIPELINE_STEPS)
                .map(_.toLowerCase)

        validateConfigList("generic.validationList")

        steps.foreach(step =>
        {
            step match
            {
                case FlashMLConstants.DATAREADER => validateConfigList("datareader.validationList")
                case FlashMLConstants.SAMPLING => validateConfigList("sampling.validationList")
                    TrainTestSampler.validate()
                case FlashMLConstants.PREPROCESSING => validateConfigList("preprocessing.validationList")
                    PreprocessingEngine.validate()
                case FlashMLConstants.FEATURE_GENERATION => validateConfigList("featuregeneration.validationList")
                    FeatureGenerationEngine.validate()
                case FlashMLConstants.VECTORIZATION => validateConfigList("vectorization.validationList")
                    VectorizationEngine.validate()
                case FlashMLConstants.MODELLING => validateConfigList("modelling.validationList")
                    ModelTrainingEngine.validate()
                case FlashMLConstants.SCORING => validateConfigList("scoring.validationList")
                case FlashMLConstants.PUBLISH => validateConfigList("publish.validationList")
                case FlashMLConstants.QA_DATA_GENERATION => validateConfigList("qadatageneration.validationList")
                case _ =>
            }
        })
    }

    /**
      * Fetch validation config items for each pipeline step and validate each item
      *
      * @param path Json path to the validation configuration items
      */
    def validateConfigList(path: String) =
    {
        val confList = getConfigItemList(path)
        confList.foreach(ConfigItemValidator.validate)
    }

    /**
      * The configurations read from the configChecks.json config file from resources folder.
      */
    private val config =
    {
        val jsonConfigString = Source.fromURL(getClass.getResource(FlashMLConstants.CONFIG_CHECKS_LIST)).getLines().mkString
        Json.parse(jsonConfigString)
    }

    /**
      * Fetch the list of config items from the Json path
      *
      * @param path Json path to the validation configuration items
      * @return List of ConfigItems
      */
    private def getConfigItemList(path: String): List[ConfigItem] =
    {
        val pathArr = path.split("\\.")
        var confList = generateNestedMap(config, pathArr).asArray.toList
        confList.map(x => ConfigItem(x("path").asString, x("pattern").asString, x("range").asString, x("canBeEmpty").asBoolean, x("datatype").asString))
    }

    /**
      * Recursively Parse the JSON object to fetch the config items
      *
      * @param mapObj    Map representing a JSON
      * @param pathArray config path split into a array
      * @return Json object after parsing the path
      */
    private def generateNestedMap(mapObj: Json.Value, pathArray: Array[String]): Json.Value =
    {
        if (pathArray.length.equals(1))
        {
            mapObj(pathArray(0))
        }
        else
        {
            generateNestedMap(mapObj(pathArray(0)), pathArray.drop(1))
        }
    }

}
