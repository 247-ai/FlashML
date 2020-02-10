package org.apache.spark.ml.tuning.generators

import scala.util.Random

/**
 * Generates a random number in the range start to end. Both inclusive.
 *
 * @param start  starting value for the range
 * @param end    end value for the range
 * @param random random object used to generate double value
 */
class UniformRandomGenerator(start: Double, end: Double, random: Random) extends Generator[Double]
{
    def this(start: Double, end: Double, seed: Long) = this(start, end, new Random(seed))

    /**
     *
     * @return a random double value in the range [start,end]. Both inclusive.
     */
    def getNext = start + (end - start) * random.nextDouble()
}
