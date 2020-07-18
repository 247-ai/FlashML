package com.tfs.flashml.util

import org.apache.spark.ml.param.ParamMap

object Implicits
{
    implicit class JsonUtils(value: Json.Value)
    {
        def asStringArray = value.asArray.map(_.asString)
    }

    implicit class StringList(value: String)
    {
        /**
          * Quote word function, like perl :-)
          * @return
          */
        def qw() = value.split(" ")
    }

    implicit class ParamMapToStringConverter(value: ParamMap)
    {
        // Sort the parameter names in alphabetical order, since the internal structure is a Map.
        def toSingleLineString = value.toSeq.sortBy(v => v.param.name).map(v => s"${v.param.name}=>${v.value}").mkString("/")
    }
}
