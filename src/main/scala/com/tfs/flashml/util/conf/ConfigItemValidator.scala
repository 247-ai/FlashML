package com.tfs.flashml.util.conf

import com.tfs.flashml.util.FlashMLConfig
import com.tfs.flashml.util.conf.ConfigValidator._
import org.slf4j.LoggerFactory

object ConfigItemValidator
{
    private val log = LoggerFactory.getLogger(getClass)

    /**
      * Validating each config item
      *
      * @param configitem config item case class
      */
    def validate(configitem: ConfigItem): Unit =
    {
        if (!configitem.canBeEmpty)
        {
            emptyCheck(configitem)
        }
        val value = getValue(configitem.path, configitem.dataType)

        if (configitem.pattern.nonEmpty)
        {
            value.foreach(regexMatchCheck(_, configitem.path, configitem.pattern))
        }
        if (configitem.range.nonEmpty)
        {
            val range = configitem.range.split("-")
            value.foreach(rangeCheck(_, configitem.path, range(0).toDouble, range(1).toDouble))
        }
    }

    /**
      * Fetch value from flashml config based on path
      *
      * @param path     Path of config item
      * @param dataType Datatype of the config item
      * @return Value of the config item or None if the config is not available
      */
    private def getValue(path: String, dataType: String): Option[Any] =
    {
        if (FlashMLConfig.config.hasPath(path))
        {
            try
            {
                dataType match
                {
                    case "string" => Some(FlashMLConfig.config.getString(path))
                    case "int" => Some(FlashMLConfig.config.getInt(path))
                    case "boolean" => Some(FlashMLConfig.config.getBoolean(path))
                }
            }
            catch
            {
                case e: Exception => throw new ConfigValidatorException(s"$path Value uses a wrong datatype")
            }
        }
        else
        {
            None
        }
    }

    /**
      * Verify if the config item is available and not empty
      *
      * @param configitem config item case class
      */
    private def emptyCheck(configitem: ConfigItem): Unit =
    {
        log.info(s"Empty Check for ${configitem.path}")
        if (!FlashMLConfig.config.hasPath(configitem.path) || getValue(configitem.path, configitem.dataType).get.toString.isEmpty)
        {
            val msg = s"${configitem.path} should not be empty"
            log.error(msg)
            throw new ConfigValidatorException(msg)
        }
    }

    /**
      * Verify if the config item value matches the given regular expression pattern
      *
      * @param value      Value of config item
      * @param configName name of the config item
      * @param pattern    regex pattern
      */
    private def regexMatchCheck(value: Any, configName: String, pattern: String): Unit =
    {
        log.info(s"Regex Check for $configName : $value")
        if (!value.toString.matches(pattern))
        {
            val msg = s"Value of $configName:$value should follow $pattern pattern"
            log.error(msg)
            throw new ConfigValidatorException(msg)
        }
    }

    /**
      * Verify if the config item value is in the given range
      *
      * @param value      Value of config item
      * @param configName name of the config item
      * @param min        minimum value of the range
      * @param max        maximum value of the range
      */
    private def rangeCheck(value: Any, configName: String, min: Double, max: Double): Unit =
    {
        log.info(s"Range Check for $configName : ${value}")
        if (!(value.asInstanceOf[Double] <= max && value.asInstanceOf[Double] >= min))
        {
            val msg = s"For $configName, expected value: [$min-$max], actual value: $value"
            log.error(msg)
            throw new ConfigValidatorException(msg)
        }
    }
}
