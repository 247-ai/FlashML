package com.tfs.flashml.functionalTests

import com.tfs.flashml.util.FlashMLConfig
import com.tfs.flashml.util.conf.{ConfigValidator, ConfigValidatorException}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

class ConfigValidatorNegativeTest extends AnyFlatSpec
{

    private val log = LoggerFactory.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("breeze").setLevel(Level.OFF)


    //Negative Testcases for Config Validator
    println("=============================================================================================")
    log.info("Starting FlashML test application")
    println("Test case: Config Validator Test")


    "Config Validator Test for negative usecases " should "match" in
    {
        FlashMLConfig.config = ConfigFactory.load("configurationtest/validation_negative_test_config.json")
        withClue("Common field validation: ")
        {
            assertThrows[ConfigValidatorException]
                    {
                        ConfigValidator.validateConfigList("generic.validationList")
                    }
        }

        withClue("Data Reader validation: ")
        {
            assertThrows[ConfigValidatorException]
                    {
                        ConfigValidator.validateConfigList("datareader.validationList")
                    }
        }

        withClue("Sampling validation: ")
        {
            assertThrows[ConfigValidatorException]
                    {
                        ConfigValidator.validateConfigList("sampling.validationList")
                    }
        }

        withClue("Preprocessing field validation: ")
        {
            assertThrows[ConfigValidatorException]
                    {
                        ConfigValidator.validateConfigList("preprocessing.validationList")
                    }
        }

        withClue("Feature generation field validation: ")
        {
            assertThrows[ConfigValidatorException]
                    {
                        ConfigValidator.validateConfigList("featuregeneration.validationList")
                    }
        }

        withClue("Vectorization field validation: ")
        {
            assertThrows[ConfigValidatorException]
                    {
                        ConfigValidator.validateConfigList("vectorization.validationList")
                    }
        }

        withClue("Model generation config validation: ")
        {
            assertThrows[ConfigValidatorException]
                    {
                        ConfigValidator.validateConfigList("modelling.validationList")
                    }
        }

    }
}
