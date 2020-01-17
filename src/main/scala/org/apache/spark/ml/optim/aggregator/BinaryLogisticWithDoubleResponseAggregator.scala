package org.apache.spark.ml.optim.aggregator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.util.MLUtils

/**
  * LogisticAggregator computes the gradient and loss for binary or multinomial logistic (softmax)
  * loss function, as used in classification for instances in sparse or dense vector in an online
  * fashion.
  *
  * Two LogisticAggregators can be merged together to have a summary of loss and gradient of
  * the corresponding joint dataset.
  *
  * This class is specific to computing the loss function and the gradient for a binary logisitc
  * regression problem when the response values are double (i.e., not 0 and 1).
  *
  * @param bcFeaturesStd
  * @param numClasses
  * @param fitIntercept
  * @param multinomial
  * @param bcCoefficients
  */
private[ml] class BinaryLogisticWithDoubleResponseAggregator(
    bcFeaturesStd: Broadcast[Array[Double]],
    numClasses: Int,
    fitIntercept: Boolean,
    multinomial: Boolean)(bcCoefficients: Broadcast[Vector])
  extends DifferentiableLossAggregator[Instance, BinaryLogisticWithDoubleResponseAggregator] with Logging
{
    private val numFeatures = bcFeaturesStd.value.length
    private val numFeaturesPlusIntercept = if (fitIntercept) numFeatures + 1
    else numFeatures
    private val coefficientSize = bcCoefficients.value.size
    protected override val dim: Int = coefficientSize

    // Only available for binomial case
    require(!multinomial, "This class is only available for binary classification with double response.")
    require(coefficientSize == numFeaturesPlusIntercept, s"Expected $numFeaturesPlusIntercept " +
            s"coefficients but got $coefficientSize")
    require(numClasses == 1 || numClasses == 2, s"Binary logistic aggregator requires numClasses " +
            s"in {1, 2} but found $numClasses.")

    @transient private lazy val coefficientsArray: Array[Double] = bcCoefficients.value match
    {
        case DenseVector(values) => values
        case _ => throw new IllegalArgumentException(s"coefficients only supports dense vector but " +
                s"got type ${bcCoefficients.value.getClass}.)")
    }

    /**
      * Update gradient and loss using binary loss function for this training datapoint (i.e., i-th example).
      * @param features
      * @param weight
      * @param label
      */
    private def binaryUpdateInPlace(features: Vector, weight: Double, label: Double): Unit =
    {

        val localFeaturesStd = bcFeaturesStd.value
        val localCoefficients = coefficientsArray
        val localGradientArray = gradientSumArray
        // Calculate  -(w'x + b)
        val margin = - {
            var sum = 0.0
            features.foreachActive
            { (index, value) =>
                if (localFeaturesStd(index) != 0.0 && value != 0.0)
                {
                    sum += localCoefficients(index) * value / localFeaturesStd(index)
                }
            }
            if (fitIntercept) sum += localCoefficients(numFeaturesPlusIntercept - 1)
            sum
        }

        // Calculate y^hat(i) - y(i), for the i-th example.
        val multiplier = weight * (1.0 / (1.0 + math.exp(margin)) - label)

        // Calculate the gradients
        // d/dw_j (Cost fn) = (y^hat(i) - y(i))* x_j
        features.foreachActive
        { (index, value) =>
            if (localFeaturesStd(index) != 0.0 && value != 0.0)
            {
                localGradientArray(index) += multiplier * value / localFeaturesStd(index)
            }
        }

        // d/db (Cost Fn) = (y^hat(i) - y(i))
        if (fitIntercept)
        {
            localGradientArray(numFeaturesPlusIntercept - 1) += multiplier
        }

        // Finally, calculate the loss function
        // In this case, with 0 <= response <= 1, we will have to use the exact formula
        lossSum += weight * (MLUtils.log1pExp(margin) - margin * (1 - label))
    }

    /**
      * Add a new training instance to this LogisticAggregator, and update the loss and gradient
      * of the objective function.
      *
      * @param instance The instance of data point to be added.
      * @return This LogisticAggregator object.
      */
    def add(instance: Instance): this.type =
    {
        instance match
        {
            case Instance(label, weight, features) =>
                require(numFeatures == features.size, s"Dimensions mismatch when adding new instance." +
                        s" Expecting $numFeatures but got ${features.size}.")
                require(weight >= 0.0, s"instance weight, $weight has to be >= 0.0")

                if (weight == 0.0) return this

                binaryUpdateInPlace(features, weight, label)
                weightSum += weight
                this
        }
    }
}
