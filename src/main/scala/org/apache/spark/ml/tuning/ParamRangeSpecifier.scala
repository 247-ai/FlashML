package org.apache.spark.ml.tuning

import org.apache.spark.ml.param._

import scala.collection.mutable

class ParamRangeSpecifier
{

    private val paramRange = mutable.Map.empty[Param[_], Any]

    def addGrid[T](param: Param[T], value: Iterable[T]): this.type =
    {
        paramRange.put(param.asInstanceOf[Param[Any]], value)
        this
    }

    def addGrid[Double](param: Param[Double], min: Double, max: Double): this.type =
    {
        paramRange.put(param.asInstanceOf[Param[Any]], (min, max))
        this
    }

    def getParams(): mutable.Map[Param[_], Any] = paramRange

    //    def getParams():ParamMap = {
    //        return paramRange
    //    }

    /*def getParams(): (Array[ParamMap],Array[ParamMap]) = {
        val arrayRange = new ArrayBuffer[ParamMap]()
        val rangeMapArray = paramRange.flatMap(a => a._2 match {
            case (min,max)=> Array(new ParamMap().put(a._1.asInstanceOf[Param[Any]],min))
            case _=> None
            }
        )

        val iterableMapArray = paramRange.flatMap(a => a._2 match {
            case iterable:Iterable[_] => Some(iterable.map(v => new ParamMap().put(a._1.asInstanceOf[Param[Any]],v)))
            case _=> None
        }).flatten
        return (rangeMapArray.toArray,iterableMapArray.toArray)
    }*/

}
