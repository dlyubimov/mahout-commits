package org.apache.mahout.math.stat

import org.apache.mahout.math.{Vector, Matrix}
import scala.util.Random

/**
 *
 * @author dmitriy
 */
package object distribution {

  /** Standard MVN */
  def mvnStd(d:Int)(implicit rng:Random) = new StdMultivariateNormal(d)

  /** Multivariate Normal */
  def mvn(mu:Vector, sigma:Matrix)(implicit rng:Random) = new MultiVariateNormal(mu,sigma)

  /** Wishart distribution with df degrees of freedom */
  def wshrt(v: Matrix, df: Int)(implicit rng: Random) = new Wishart(v, df)


}
