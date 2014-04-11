package org.apache.mahout.math.stat.distribution

import org.apache.mahout.math.Matrix
import org.apache.mahout.math.scalabindings._
import RLikeOps._
import scala.util.Random

/**
 *
 * @author dmitriy
 */
class Wishart(v: Matrix, df: Int)(implicit rng:Random) extends Sampler[Matrix] {

  assert(v.nrow == v.ncol)


  val stdMvn = mvnStd(v.nrow)
  val mL = {
    val ch = chol(v)
    assert(ch.isPositiveDefinite,"V parameter of Wishart must be positive-definite")
    ch.getL
  }
  val mLt = mL.t

  def unary_~ : Matrix = {
    definitionSampler
  }

  // This will be fairly inefficient if df goes high. We will substitute this with fast sampler as
  // defined in "Wishart Distributions and Inverse Wishart Sampling" by Stanley Sawyer.
  private def definitionSampler = {
    val x = (0 until df).toStream.map(i => ~stdMvn).reduce(_ += _)
    (mL %*% x) %*%: mLt
  }
}
