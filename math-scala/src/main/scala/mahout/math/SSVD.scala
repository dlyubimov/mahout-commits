package mahout.math

import math._
import org.apache.mahout.math.{Matrix, DenseMatrix}
import scala.util.Random

/**
 * Created with IntelliJ IDEA.
 * User: dmitriy
 * Date: 7/25/13
 * Time: 9:27 PM
 * To change this template use File | Settings | File Templates.
 */
private[math] object SSVD {

  /**
   * In-core SSVD algorithm.
   *
   * @param a input matrix A
   * @param k request SSVD rank
   * @param p oversampling parameter
   * @param q number of power iterations
   * @return (U,V,s)
   */
  def ssvd(a: Matrix, k: Int, p: Int = 15, q: Int = 0) = {
    val m = a.nrow
    val n = a.ncol
    if (k > min(m, n))
      throw new IllegalArgumentException(
        "k cannot be greater than smaller of m,n")
    val pfxed = min(p, min(m, n) - k)

    // actual decomposition rank
    val r = k + pfxed

    // we actually fill the random matrix here
    // just like in our R prototype, although technically
    // that would not be necessary if we implemented specific random
    // matrix view. But ok, this should do for now.
    // it is actually the distributed version we are after -- we
    // certainly would try to be efficient there.

    val rnd = new Random()
    val omega = new DenseMatrix(n, r) := ((r, c, v) => rnd.nextGaussian)

    var y = a %*% omega
    var yty = y.t %*% y
    val at = a.t
    var ch = chol(yty)
    var bt = ch.solveRight(at %*% y)


    // power iterations
    for (i <- 0 until q) {
      y = a %*% bt
      yty = y.t %*% y
      ch = chol(yty)
      bt = ch.solveRight(at %*% y)
    }

    val bbt = bt.t %*% bt
    val (uhat, d) = eigen(bbt)

    val s = d.sqrt
    val u = ch.solveRight(y) %*% uhat
    val v = bt %*% (uhat %*%: diagv(1 /: s))

    (u(::, 0 until k), v(::, 0 until k), s(0 until k))

  }

}
