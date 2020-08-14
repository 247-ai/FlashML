package com.tfs.flashml.functionalTests

import com.tfs.flashml.core.VectorizationEngine
import com.tfs.flashml.core.featuregeneration.FeatureGenerationEngine
import com.tfs.flashml.core.preprocessing.PreprocessingEngine
import com.tfs.flashml.util.FlashMLConfig
import com.tfs.flashml.util.conf.ConfigValidator
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

class ConfigValidatorPositiveTest extends AnyFlatSpec
{

    private val log = LoggerFactory.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("breeze").setLevel(Level.OFF)


    //Positive Test cases for Config Validator
    println("=============================================================================================")
    log.info("Starting FlashML test application")
    println("Test case: Config Validator Test")


    "Config Validator Test for positive usecases " should "match" in
    {
        FlashMLConfig.config = ConfigFactory.load("configurationtest/validation_test_config.json")
        withClue("Common field validation: ")
        {
            ConfigValidator.validateConfigList("generic.validationList")
        }

        withClue("Data Reader validation: ")
        {
            ConfigValidator.validateConfigList("datareader.validationList")
        }

        withClue("Sampling validation: ")
        {
            ConfigValidator.validateConfigList("sampling.validationList")
        }

        withClue("Preprocessing field validation: ")
        {
            ConfigValidator.validateConfigList("preprocessing.validationList")
        }

        withClue("Feature generation field validation: ")
        {
            ConfigValidator.validateConfigList("featuregeneration.validationList")
        }

        withClue("Vectorization field validation: ")
        {
            ConfigValidator.validateConfigList("vectorization.validationList")
        }

        withClue("Model generation config validation: ")
        {
            ConfigValidator.validateConfigList("modelling.validationList")
        }

        withClue("Scoring config validation: ")
        {
            ConfigValidator.validateConfigList("scoring.validationList")
        }

        withClue("Publish config validation: ")
        {
            ConfigValidator.validateConfigList("publish.validationList")
        }

        withClue("QA data generation config validation: ")
        {
            ConfigValidator.validateConfigList("qadatageneration.validationList")
        }

        withClue("Preprocessing text variable dependency validation: ")
        {
            PreprocessingEngine.validate()
        }

        withClue("Feature Generation text variable dependency validation: ")
        {
            FeatureGenerationEngine.validate()
        }

        withClue("Vectorization text variable dependency validation: ")
        {
            VectorizationEngine.validate()
        }
    }
}
