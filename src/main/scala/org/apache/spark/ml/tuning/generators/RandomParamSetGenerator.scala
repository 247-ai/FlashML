package org.apache.spark.ml.tuning.generators

import org.apache.spark.ml.param.{DoubleParam, Param, ParamMap}
import org.apache.spark.ml.tuning.ParamRangeSpecifier
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.util.Random

/**
  * Generates scala params based on the provided values in paramRangeSpecifier.
  * @param paramRangeSpecifier ParamRangeSpecifier value which contains specification of values to
  *                            be generated for each Spark param
  * @param seed seed value used to create a Random number generator
  */
class RandomParamSetGenerator(paramRangeSpecifier: ParamRangeSpecifier, seed: Long) extends Generator[ParamMap]
{
    val log: Logger = LoggerFactory.getLogger(getClass)

    private val paramRangeValues = paramRangeSpecifier.getParams()

    private var generators = Map.empty[Param[_], Generator[_]]

    private val random = new Random(seed)

    private def init =
    {
        //TODO handle other scenarios
        /*Sorting the map using name & using ListMap (ordered map) to maintain the order of the generators.
        * The values generated will be different if the ordering of the map is different each time*/
        generators = ListMap(paramRangeValues.zipWithIndex.map
        { case ((a: DoubleParam, (start: Double, end: Double)), index: Int) => (a, new UniformRandomGenerator(start,
            end, random))

        case ((a: Param[Double], (start: Integer, end: Integer)), index: Int) =>
            (a, new UniformRandomGenerator(start.toDouble, end.toDouble, random))
        //TODO have to handle the data type as it eliminated by erasure, remove the log statement after that
        case ((param: Param[Double], iterable: Iterable[_]), index: Int) => log.info("paramRangeValues double  " +
                param.getClass)
            (param, new UniformIterablesRandomGenerator(iterable, random))
        case ((defaultParam: Param[_], defaultValue), index: Int) =>
            throw new Exception("invalid value. param class " + defaultParam.getClass + " value class " +
                    defaultValue.getClass)
        }.toSeq.sortBy(_._1.name):_*)
    }

    init

    /**
      *
      * @return a ParamMap with one value per Param
      */
    def getNext =
    {
        val paramMap = new ParamMap()
        generators.foreach
        { case (a, b) =>
            paramMap.put(a.asInstanceOf[Param[Any]], b.getNext) }
        paramMap
    }
}
