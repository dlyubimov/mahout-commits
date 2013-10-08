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

package org.apache.mahout.sparkbindings.drm

import org.scalatest.FunSuite
import mahout.math._
import mahout.math.RLikeOps._
import org.apache.log4j.{Level, Logger, BasicConfigurator}
import java.io.File
import scala.util.Random
import org.apache.mahout.math.{DenseMatrix, SparseMatrix}
import org.apache.spark.SparkContext._

/**
 *
 */
class IntIndexedRowsDRMOpsTests extends FunSuite {

  BasicConfigurator.resetConfiguration()
  BasicConfigurator.configure()
  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("mahout.spark").setLevel(Level.DEBUG)

  // in case we are running from ide, it won't use jars
  // for class path so we need to get maven-computed one
  // in order to pass it to the spark
  val buildJars = Traversable.empty[String]


  test("DRM-transpose") {

    //    implicit val sc = mahoutSparkContext("spark://localhost:7077", "DrmOpsTests", buildJars)
    implicit val sc = mahoutSparkContext("local", "DrmOpsTests", buildJars)
    try {
      // in-core A
      val coreA = dense(
        (1, 2, 3),
        (3, 4, 4.5)
      )
      println(coreA.toString)

      // distribute into a DRM
      val drmA = drmParallelize(coreA)

      printf("drm A -> nrow=%d, ncol=%d.\n", drmA.nrow, drmA.ncol)

      // transpose
      val drmAt = drmA.t
      printf("drm A' -> nrow=%d, ncol=%d.\n", drmAt.nrow, drmAt.ncol)

      // actually run and collect
      val coreAt = drmAt.collect

      println(coreAt.toString)

    } finally {
      sc.stop()
    }

  }

  test("DRM-Biggertranspose") {

    implicit val sc = mahoutSparkContext("spark://localhost:7077", "DrmOpsTests", buildJars)
    //        implicit val sc = mahoutSparkContext("local", "DrmOpsTests", buildJars)
    try {

      val ncol = 10000
      val nrow = 50000
      val nzPerVector = 20

      var t = System.currentTimeMillis()

      // distribute into a DRM
      val drmA = drmParallelizeEmpty(nrow, ncol, numPartitions = 15).
        mapRows((index, vec) => {

        vec := (for (i <- 0 until nzPerVector) yield (i, i + index.toDouble))
        vec
      })

      // force computation to separate running time of
      // matrix generation from the transposition
      drmA.cached

      printf("drm A -> nrow=%d, ncol=%d in %d ms.\n", drmA.nrow, drmA.ncol, System.currentTimeMillis() - t)


      t = System.currentTimeMillis()

      // transpose
      val drmAt = drmA.t
      printf("drm A' -> nrow=%d, ncol=%d.\n", drmAt.nrow, drmAt.ncol)

      //force computation
      drmAt.cached

      printf("Transpose complete in %d ms.\n", System.currentTimeMillis() - t)

      t = System.currentTimeMillis()
      // save to hdfs. see what it is
      drmAt.writeDRM("hdfs://localhost:11010/tmp/BigTranspose")

      printf("Save complete in %d ms.\n", System.currentTimeMillis() - t)


    } finally {
      sc.stop()
    }

  }

  test("t squared slim sparse") {

    //    implicit val sc = mahoutSparkContext("spark://localhost:7077", "DrmOpsTests", buildJars)
    implicit val sc = mahoutSparkContext("local", "DrmOpsTests", buildJars)

    val rnd = new Random(1234L)
    val icA = new SparseMatrix(50, 50) := ((r, c, v) => if (rnd.nextDouble() > 0.7) rnd.nextDouble() else 0.0)

    val icAtAactual = icA.t %*% icA

    val drmA = drmParallelize(icA)

    val icAtA = drmA.t_sq_slim()

    val normResidual = (icAtA - icAtAactual).norm

    printf("A=\n%s\n", icA)

    printf("AtA=\n%s\n",icAtA)
    printf("AtA actual = \n%s\n",icAtAactual)

    assert(normResidual < 1e-10, "norm residual high: %.2f".format(normResidual))


  }

  test("t squared slim dense") {

    //    implicit val sc = mahoutSparkContext("spark://localhost:7077", "DrmOpsTests", buildJars)
    implicit val sc = mahoutSparkContext("local", "DrmOpsTests", buildJars)

    val rnd = new Random(1234L)
    val icA = new DenseMatrix(50, 50) := ((r, c, v) => rnd.nextDouble())

    val icAtAactual = icA.t %*% icA

    val drmA = drmParallelize(icA)

    val icAtA = drmA.t_sq_slim()

    val normResidual = (icAtA - icAtAactual).norm

    printf("A=\n%s\n", icA)

    printf("AtA=\n%s\n",icAtA)
    printf("AtA actual = \n%s\n",icAtAactual)

    assert(normResidual < 1e-10, "norm residual high: %.2f".format(normResidual))


  }

}
