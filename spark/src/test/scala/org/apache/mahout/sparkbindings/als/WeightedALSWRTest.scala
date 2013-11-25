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

package org.apache.mahout.sparkbindings.als

import org.scalatest.FunSuite
import org.apache.mahout.math.{SparseMatrix, DenseMatrix, Matrix}
import util.Random
import org.apache.mahout.math.scalabindings._
import org.apache.mahout.math.scalabindings.RLikeOps._
import org.apache.mahout.sparkbindings.drm._
import org.apache.log4j.BasicConfigurator

/**
  *
  * @author dmitriy
  */
class WeightedALSWRTest extends FunSuite {

   test("on simulated sparse binomial input") {

     BasicConfigurator.resetConfiguration()
     BasicConfigurator.configure()

     val (p, c, pActual) = createData(40, 50, 10, 0.1)

     printf("P=\n%s\n", p)
     printf("C=\n%s\n", c)

     implicit val sc = mahoutSparkContext("local", "WeightedALSWRTest")
     try {

       val (drmP, drmC) = (drmParallelize(p), drmParallelize(c))

       //    printf("C'=\n%s\n", drmC.t.collect)
       //    printf("P'=\n%s\n", drmP.t.collect)

       val (uDrm, vDrm) = WeightedALSWR.walswr(drmP, drmC, 5, 1e-6, 8)
       val (u, v) = (uDrm.collect, vDrm.collect)

       printf("U=\n%s\n", u)
       printf("V=\n%s\n", v)

       printf("P_actual=\n%s\n", pActual)
       printf("P_predict=\n%s\n", u %*% v.t)
     } finally {
       sc.stop()
     }

   }

   test("train on ideal non-sparse input") {

     val (nrow, ncol) = (40, 50)
     val k = 10

     val (p, c) = createIdealData(nrow, ncol, k)

     printf("P=\n%s\n", p)
     printf("C=\n%s\n", c)

     implicit val sc = mahoutSparkContext("local", "WeightedALSWRTest")
     try {

       val (drmP, drmC) = (drmParallelize(p), drmParallelize(c))

       val (uDrm, vDrm) = WeightedALSWR.walswr(drmP, drmC, k, 1e-6, 8)
       val (u, v) = (uDrm.collect, vDrm.collect)

       printf("U=\n%s\n", u)
       printf("V=\n%s\n", v)

       printf("P_actual=\n%s\n", p)
       printf("P_predict=\n%s\n", u %*% v.t)

     } finally {
       sc.stop()
     }

   }


   def createIdealData(nrow: Int, ncol: Int, k: Int): (Matrix, Matrix) = {

     val (uSim, vSim) = (new DenseMatrix(nrow, k), new DenseMatrix(ncol, k))

     val rnd = new Random()

     uSim := ((r, c, v) => rnd.nextDouble())
     vSim := ((r, c, v) => rnd.nextDouble())

     val p = new DenseMatrix(nrow, ncol) := ((r, c, v) => uSim(r, ::) dot vSim(c, ::))

     (p, new SparseMatrix(nrow, ncol))
   }


   def createData(nusers: Int, nitems: Int, k: Int, visibleTrainRatio: Double,
       goodConfidence: Double = 15): (Matrix, Matrix, Matrix) = {

     val rnd = new Random(123L)

     val (uSim, vSim) = (new DenseMatrix(nusers, k), new DenseMatrix(nitems, k))

     uSim := ((r, c, v) => rnd.nextDouble())
     vSim := ((r, c, v) => rnd.nextDouble())

     val (p, c, pactual) = (new SparseMatrix(nusers, nitems), new SparseMatrix(nusers, nitems), new SparseMatrix(nusers,
       nitems))

     for (u <- 0 until nusers; i <- 0 until nitems) {

       val score = uSim(u, ::) dot vSim(i, ::)
       pactual(u, i) = if (score < 0.25 * k) 1 else 0

       if (rnd.nextDouble < visibleTrainRatio) {
         p(u, i) = pactual(u, i)
         c(u, i) = goodConfidence
       }

     }


     (p, c, pactual)
   }


 }
