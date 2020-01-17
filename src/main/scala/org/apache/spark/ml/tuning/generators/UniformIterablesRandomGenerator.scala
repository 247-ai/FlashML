package org.apache.spark.ml.tuning.generators

import scala.util.Random

/**
  * This Generator picks one value at random from a provided iterable object
  *
  * @param values Iterable object
  * @param random random object used to generate int value for index
  * @tparam T param type of Iterable object
  */
class UniformIterablesRandomGenerator[T](values: Iterable[T], random: Random) extends Generator[T]
{
    def this(values: Iterable[T], seed: Long) = this(values, new Random(seed))

    val valuesArray = values.toSeq

    /**
      *
      * @return one of the values picked up from the iterable
      */
    def getNext =
    {
        val maxLength = valuesArray.length
        //generates int from 0 to maxLength (including 0 & excluding maxLength) - i.e [0,maxLength)
        val randomIndex = random.nextInt(maxLength)
        valuesArray(randomIndex)
    }
}
