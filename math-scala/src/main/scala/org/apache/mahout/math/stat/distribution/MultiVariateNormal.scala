package org.apache.mahout.math.stat.distribution

import org.apache.mahout.math.{Matrix, Vector}
import org.apache.mahout.math.scalabindings._
import RLikeOps._
import scala.util.Random

class MultiVariateNormal(val mu:Vector, val sigma:Matrix)(implicit rnd:Random) extends Sampler[Vector] {

  assert(sigma.nrow == sigma.ncol, "Covariance matrix must be square")
  assert(sigma.nrow == mu.length, "mean has dimensionality differs from covariance parameter")

  val mL = {
    val ch = chol(sigma)
    assert( ch.isPositiveDefinite, "sigma must be positive definite")
    ch.getL
  }

  val std = mvnStd(mu.length)

  // Sampler operator
  def unary_~ : Vector = {
    val z = ~std
    mL %*% z + mu
  }
}
