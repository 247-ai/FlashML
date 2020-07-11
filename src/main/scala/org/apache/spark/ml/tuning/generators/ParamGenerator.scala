package org.apache.spark.ml.tuning.generators

import org.apache.spark.ml.param.ParamMap

trait ParamGenerator extends Generator[ParamMap]
{
    def getNext: ParamMap
}

object ParamGenerator
{
    def empty: Generator[ParamMap] = new Generator[ParamMap]
    {
        override def getNext = new ParamMap()
    }
}
