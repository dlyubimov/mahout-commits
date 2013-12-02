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

import org.apache.log4j.Logger
import org.apache.spark.bagel.Bagel
import org.apache.mahout.math.scalabindings._
import org.apache.mahout.math.scalabindings.RLikeOps._
import org.apache.mahout.sparkbindings.drm._
import collection.mutable
import scala.collection.JavaConversions._
import org.apache.spark.storage.StorageLevel
import collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.rdd.RDD
import math._
import org.apache.mahout.math.{RandomAccessSparseVector, DenseVector, SparseColumnMatrix}

/**
 *
 * This is ALS-WR with confidence. See distributed-als-with-confidence.pdf for details.
 *
 * Inputs are P and C DRM matrices.
 *
 */
object WeightedALSWR {

  private val s_log = Logger.getLogger(WeightedALSWR.getClass)

  /**
   * Weighted ALS-WR
   * @param P preference matrix (1 or 0's)
   * @param C confidence matrix C' = C-C_0 where C_0 is matrix of base confidence for no observation
   * @param k desired decomposition rank
   * @param lambda L2 regularization rate
   * @param niter maximum number of iterations
   * @return (U,V) as drm matrices.
   */
  def walswr(P: DRM[Int], C: DRM[Int], k: Int, lambda: Double = 1e-6, niter: Int = 10):
  (DRM[Int], DRM[Int]) = {

    // Make sure P and C cached, since we will have multiple passes over same input to calculate
    // errors.

    P.cached
    C.cached

    val nusers = P.nrow.toInt
    val nitems = P.ncol

    if (nusers != P.nrow)
      throw new IllegalArgumentException("Can't handle input with more than MAXINT (~2bln) columns")

    var uverts = initFactors(P, C, k)
    var vverts = initFactors(P.t, C.t, k)

    val iterRmse = new ArrayBuffer[Double]()

    for (i <- 0 until niter) {

      s_log.info("Weighted ALS-WR Iteration %d.".format(i))

      uverts = alswrIter(uverts.map(t => {
        t._2.active = true
        t
      }), nusers, vverts, nitems, k, lambda)

      //      printf("iter %d U=\n%s\n",i, verts2DRM(uverts,nusers,k).collect)


      vverts = alswrIter(vverts.map(t => {
        t._2.active = true
        t
      }), nitems, uverts, nusers, k, lambda)

      iterRmse += rmse(vverts)

    }

    printf("rmse=%s\n", iterRmse)

    (verts2DRM(uverts, nusers, k), verts2DRM(vverts, nitems, k))

  }

  private def verts2DRM(verts: RDD[(Int, ALSWRCVertex)], nrow: Int, k: Int): DRM[Int] =
    new BaseDRM[Int](verts.map(t => (t._1, t._2.factorVec)), _nrow = nrow, _ncol = k)


  /**
   * This assumes that every user has a fairly sparse usage history.
   * If the matrix becomes fairly close to dense, this ALS-WR approach will becomes obviously fairly
   * inefficient (approaching dense matrix multiplication behavior).
   *
   * Therefore it is recommended to purge user histories for abnormally active history. It is likely
   * it is not typical user anyway, plus it might hamper computations.
   *
   * @param uverts
   * @param nurows
   * @param vverts
   * @param nvrows
   * @param k
   * @param lambda
   * @return
   */
  private def alswrIter(uverts: RDD[(Int, ALSWRCVertex)],
      nurows: Int,
      vverts: RDD[(Int, ALSWRCVertex)],
      nvrows: Int,
      k: Int,
      lambda: Double):
  RDD[(Int, ALSWRCVertex)] = {

    implicit val sc = uverts.context
    val drmV = verts2DRM(vverts, nvrows, k)

    println("V(%d,%d)".format(drmV.nrow, drmV.ncol))

    val inCoreVtV = drmV.t_sq_slim()

    // Look at the rank of V'V product. If we don't have full rank here, the system is singular and
    // we would have trouble with our fitting.
    if (!inCoreVtV.isFullRank)
      throw new IllegalArgumentException(
        "The input doesn't have enough information for k-rank ALS. " +
            "Try to reduce k for more meaningful results.")

    // Double-check, really in theory not needed, since we already checked for rank, but i just want
    // to make sure Cholesky does what it needs to, here. If everything ok i may remove this double-
    // verification later.
    val l = chol(inCoreVtV).getL
    val good = (for (row <- 0 until l.nrow; col <- 0 to row) yield l(row, col)).forall(abs(_) > 1e-13)


    if (!good) {
      if (s_log.isDebugEnabled) {
        // compute control in-core
        val icV = drmV.collect
        val icVtV = icV.t %*% icV
        val normResidual = (icVtV - inCoreVtV).norm
        s_log.debug("norm residual:%.2f".format(normResidual))
        s_log.debug("computed V'V=\n%s\n".format(inCoreVtV))
        s_log.debug("actual V'V=\n%s\n".format(icVtV))
        s_log.debug("V=\n%s\n".format(icV))

      }
      throw new IllegalArgumentException(
        "V'V computation produces non-positive definite? Cholesky undetermined:L=\n%s\n".format(l))
    }

    // broadcast new V'V to all nodes.
    // this should be ok since V'V is just k x k
    val vtv = sc.broadcast(inCoreVtV)

    val msgs = vverts.flatMap(t => {
      val outMsgs = new mutable.HashMap[Int, ALSVectorMsg]()
      val srcId = t._1
      val vert = t._2

      /* populate out msgs to include rows mentioned for confidence */
      vert.c.nonZeroes().foreach(el => outMsgs += (el.index ->
          new ALSVectorMsg(srcId, el.index(), vert.factorVec)))

      /* populate out msgs to include rows mentioned for preferences */
      vert.p.nonZeroes().foreach(el => if (!outMsgs.contains(el.index))
        outMsgs += (el.index -> new ALSVectorMsg(srcId, el.index(), vert.factorVec))
      )

      outMsgs.toTraversable
    })

    val numPartitions = uverts.partitions.size

    val newUverts = Bagel.run(sc, uverts, msgs, numPartitions, StorageLevel.MEMORY_AND_DISK_SER)((vert, msgs, step) => {


      val newU = if (vert.p.getNumNondefaultElements > 0) {
        // see if we have enough confidence to comprise non-signular VtDV
        val n_u = vert.c.getNumNondefaultElements

        val icVtV = vtv.value

        // ideally, we might want to form vBlock directly as a full-cardinality SparseColMatrix.
        // unfortunately this will not work very well with the way currently SparseCol/RowMatrices
        // are structured in Mahout. I am submitting a few patches that should lift that concern
        // and make this code much more accessible thru use of sparse matrix blocking rather than
        // dense blocking with a level of index indirection.

        val msgMap = msgs.toTraversable.flatMap(_.view.map(msg => ((msg.sourceId -> msg.v)))).toMap

        val cIndices = vert.c.nonZeroes().map(_.index)
        val pIndices = vert.p.nonZeroes().map(_.index)

        val vBlockForC = if (n_u == 0) null
        else dense(cIndices.map(msgMap(_)).toSeq: _*)

        val vBlockForP = dense(pIndices.map(msgMap(_)).toSeq: _*)

        // diagonal for D
        val d = if (n_u == 0) null else dvec(vert.c.nonZeroes().view.map(_.get))

        /*
          this computes V'V + V'DV + n_u*lambda*I, and then takes Cholesky.

          if n_u = 0 then the only thing here to have is V'V

          Note that we sparsify V'DV operation by picking only row vectors from V that have corresponding
          nonzero diagonal element in D (and thus we throw zero diagonal elements from D as well).
         */

        val cholArg =
          if (n_u > 0)
            icVtV + (vBlockForC.t %*%: diagv(d)) %*% vBlockForC + diag(n_u * lambda, k)
          else icVtV

        val ch = chol(cholArg)


        //c_0 * I + D'
        val dForP = dvec(pIndices.map(i => vert.c(i) + 1))

        val p = dvec(pIndices.map(i => vert.p(i)))

        // b <- V' * (c_0 I + D(u)) * P(u)
        // we do this over dense blocks of course skipping irrelevant rows and elements
        val b = (vBlockForP.t %*%: diagv(dForP)) %*% dense(p).t

        // solve linear system, result is U(V)-row-vector
        val urow = ch.solveRight(eye(k)) %*% ch.solveLeft(b)

        if (s_log.isDebugEnabled) {
          // Generally, this is not supposed to be happening for full-rank matrices. We've asserted
          // full rank vor V'V before broadcasting it, so with a very great probability Cholesky argument
          // is full rank here. Unless we screwed with computations, in which case it also may cause
          // non-positive definite input as well.
          val err = (cholArg %*% urow - b).norm
          assert(err <= 1e-10,
            "Cholesky linear system solution failed, residual norm = %.4f.".format(err))
        }

        // Residual computation

        // We assume that that data we *know* is the one identified by non-zero indices of confidences
        // vert.c. For each such indices we compute dot-product of newU and corresponding v-row vector.
        // We can see that these values are represented by values of the vector (vBlockForC %*% newU).
        // Therefore (vert.p remapped to cIndices -  (vBlockForC %*% newU)) will represent a vector
        // of residuals.

        vert.residual = if (n_u > 0) {
          val pForC = dvec(cIndices.map(vert.p(_)))

          val residVec = pForC - (vBlockForC %*% urow)(::, 0)

          // Square each item in the vector of residuals to obtain partial mse. We don't explicitly
          // save num of items because they could be obtained as n_u = vert.c.getNumNondefaultElements
          // per above at any given moment.
          (residVec *= residVec).sum

        } else 0.0

        urow

      } else {
        vert.residual = 0.0
        new SparseColumnMatrix(k, 1)
      }

      assert(newU.ncol == 1)
      assert(newU.nrow == k)

      vert.factorVec = new DenseVector(k) := newU(::, 0)

      vert.active = false

      (vert, Array())
    })

    newUverts

  }

  private def rmse(verts: RDD[(Int, ALSWRCVertex)]) = {

    // RMSE is computed by taking residuals for existing observations only (which are represented
    // by non-zero element of confidence vector c^*). Store the tuple (sum(residuals), sum(numNonZeroC^*entries))
    // of every vertex  to t:
    val t = verts.map(t => (t._2.c.getNumNondefaultElements.toLong, t._2.residual))
        .fold(0L, 0.0)((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2))

    // If number of tests >0 return sqrt(sum(resid)/numTestCases) otherwise return 0 (no tests). The
    // 0 case really means we don't have observations at all so it should not really occur for any
    // non-degenerate problem.
    if (t._1 == 0L) 0.0 else sqrt(t._2 / t._1)
  }

  private def initFactors(P: DRM[Int], C: DRM[Int], k: Int): RDD[(Int, ALSWRCVertex)] = {

    val ncol = P.ncol
    val sc = P.getRDD.context
    // important: fix the seed in the front-end and then re-distribute
    // to ensure idempotency of recomputations.
    val seed = new Random().nextLong()


    val verts = P.getRDD.mapWith(part => new Random(seed + part))((t, rnd) => {
      val factorVec = new DenseVector(k)
      for (i <- 0 until k) factorVec.setQuick(i, rnd.nextDouble / 100)
      val p = t._2
      val id = t._1

      (id, new ALSWRCVertex(id, factorVec, p, new RandomAccessSparseVector(ncol)))
    })

    val msgs = C.getRDD.map(t => (t._1, new VectorMessage[Int](t._1, t._2)))


    Bagel.run(sc, verts, msgs, P.getRDD.partitions.size)((vert, msgs, step) => {

      msgs.foreach(_.view.foreach(msg => vert.c := msg.v))

      vert.active = false

      (vert, Array())

    })
  }

}
