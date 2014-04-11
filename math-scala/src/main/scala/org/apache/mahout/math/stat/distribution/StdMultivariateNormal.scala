package org.apache.mahout.math.stat.distribution

import org.apache.mahout.math.Vector
import org.apache.mahout.math.scalabindings._
import RLikeOps._
import scala.util.Random

/**
 * Standard Multivariate
 * @param d Dimensionality
 */
class StdMultivariateNormal(d:Int)(implicit rng:Random) extends Sampler[Vector] {

  def unary_~ : Vector = dvec(Array.tabulate(d)(i => rng.nextGaussian()))

}
