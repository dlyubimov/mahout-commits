package mahout.math

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import math._
import org.apache.mahout.math.{Matrix, DenseMatrix}
import scala.util.Random

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
