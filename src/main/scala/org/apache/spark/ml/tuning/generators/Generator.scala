package org.apache.spark.ml.tuning.generators

trait Generator[T]
{
    def getNext: T
}
