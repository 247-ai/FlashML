package com.tfs.flashml.functionalTests

import com.tfs.flashml.core.VectorizationEngine
import com.tfs.flashml.core.featuregeneration.FeatureGenerationEngine
import com.tfs.flashml.core.preprocessing.PreprocessingEngine
import com.tfs.flashml.util.conf.ConfigValidator
import com.tfs.flashml.util.FlashMLConfig
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class ConfigValidatorPositiveTest extends FlatSpec {

  private val log = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("breeze").setLevel(Level.OFF)



  //Positive Test cases for Config Validator
  println("=============================================================================================")
  log.info("Starting FlashML test application")
  println("Test case: Config Validator Test")


  "Config Validator Test for positive usecases " should "match" in {
    FlashMLConfig.config= ConfigFactory.load("configurationtest/validation_test_config.json")
    withClue("Common field validation: ") {
      assertResult() {
          ConfigValidator.validateConfigList("generic.validationList")
      }
    }

    withClue("Data Reader validation: ") {
      assertResult() {
          ConfigValidator.validateConfigList("datareader.validationList")
      }
    }

    withClue("Sampling validation: ") {
      assertResult() {
          ConfigValidator.validateConfigList("sampling.validationList")

      }
    }

    withClue("Preprocessing field validation: ") {
      assertResult() {
          ConfigValidator.validateConfigList("preprocessing.validationList")
      }
    }

    withClue("Feature generation field validation: ") {
      assertResult() {
          ConfigValidator.validateConfigList("featuregeneration.validationList")
      }
    }

    withClue("Vectorization field validation: ") {
      assertResult() {
          ConfigValidator.validateConfigList("vectorization.validationList")
      }
    }

    withClue("Model generation config validation: ") {
      assertResult() {
          ConfigValidator.validateConfigList("modelling.validationList")
      }
    }

    withClue("Scoring config validation: ") {
      assertResult() {
          ConfigValidator.validateConfigList("scoring.validationList")
      }
    }

    withClue("Publish config validation: ") {
      assertResult() {
          ConfigValidator.validateConfigList("publish.validationList")
      }
    }

    withClue("QA data generation config validation: ") {
      assertResult() {
          ConfigValidator.validateConfigList("qadatageneration.validationList")
      }
    }

    withClue("Preprocessing text variable dependency validation: ") {
      assertResult() {
          PreprocessingEngine.validate()
      }
    }

    withClue("Feature Generation text variable dependency validation: ") {
      assertResult() {
          FeatureGenerationEngine.validate()
      }
    }

    withClue("Vectorization text variable dependency validation: ") {
      assertResult() {
          VectorizationEngine.validate()
      }
    }

  }

}
