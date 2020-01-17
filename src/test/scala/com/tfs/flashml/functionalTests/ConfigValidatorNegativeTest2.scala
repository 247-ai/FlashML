package com.tfs.flashml.functionalTests

import com.tfs.flashml.core.VectorizationEngine
import com.tfs.flashml.core.featuregeneration.FeatureGenerationEngine
import com.tfs.flashml.core.preprocessing.PreprocessingEngine
import com.tfs.flashml.util.conf.{ConfigValidator, ConfigValidatorException}
import com.tfs.flashml.util.FlashMLConfig
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class ConfigValidatorNegativeTest2 extends FlatSpec {

  private val log = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("breeze").setLevel(Level.OFF)



  //Negative Testcases for Config Validator
  println("=============================================================================================")
  log.info("Starting FlashML test application")
  println("Test case: Config Validator Test")


  "Config Validator Test for negative usecases " should "match" in {
    FlashMLConfig.config= ConfigFactory.load("configurationtest/validation_negative_test_config_2.json")
    withClue("Common field validation: ") {
      assertThrows[ConfigValidatorException] {
          ConfigValidator.validateConfigList("generic.validationList")
      }
    }

    withClue("Data Reader validation: ") {
      assertThrows[ConfigValidatorException] {
          ConfigValidator.validateConfigList("datareader.validationList")
      }
    }

    withClue("Sampling validation: ") {
      assertThrows[ConfigValidatorException] {
          ConfigValidator.validateConfigList("sampling.validationList")
      }
    }


    withClue("Model generation config validation: ") {
      assertThrows[ConfigValidatorException] {
          ConfigValidator.validateConfigList("modelling.validationList")
      }
    }

    withClue("Preprocessing text variable dependency validation: ") {
      assertThrows[ConfigValidatorException] {
          PreprocessingEngine.validate()
      }
    }

    withClue("Feature Generation text variable dependency validation: ") {
      assertThrows[ConfigValidatorException] {
          FeatureGenerationEngine.validate()
      }
    }

    withClue("Vectorization text variable dependency validation: ") {
      assertThrows[ConfigValidatorException] {
          VectorizationEngine.validate()
      }
    }

  }

}
