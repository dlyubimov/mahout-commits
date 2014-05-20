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

package org.apache.mahout.math

import scala.reflect.ClassTag
import org.apache.mahout.math.drm.decompositions.{DSPCA, DSSVD, DQR}

package object drm {

  /** Drm row-wise tuple */
  type DrmTuple[K] = (K, Vector)

  /** Drm block-wise tuple: Array of row keys and the matrix block. */
  type BlockifiedDrmTuple[K] = (Array[K], _ <: Matrix)


  /** Block-map func */
  type BlockMapFunc[S, R] = BlockifiedDrmTuple[S] => BlockifiedDrmTuple[R]

  /** CacheHint type */
  //  type CacheHint = CacheHint.CacheHint

  def safeToNonNegInt(x: Long): Int = {
    assert(x == x << -31 >>> -31, "transformation from long to Int is losing signficant bits, or is a negative number")
    x.toInt
  }

  def drmBroadcast(m:Matrix)(implicit ctx:DistributedContext):BCast[Matrix] = ctx.broadcast(m)

  def drmBroadcast(v:Vector)(implicit ctx:DistributedContext):BCast[Vector] = ctx.broadcast(v)

  implicit def bcast2val[T](bcast:BCast[T]):T = bcast.value

  implicit def v2Writable(v: Vector): VectorWritable = new VectorWritable(v)

  implicit def m2Writable(m: Matrix): MatrixWritable = new MatrixWritable(m)

  implicit def vw2v(vw: VectorWritable): Vector = vw.get()

  implicit def mw2m(mw: MatrixWritable): Matrix = mw.get()

  private[drm] implicit def m2c(m: DrmLike[_]): DistributedContext = m.context

  private[drm] implicit def m2e(m: DrmLike[_]): DistributedEngine = m.context.engine

  private[drm] implicit def c2e(c: DistributedContext): DistributedEngine = c.engine

  implicit def drm2drmCpOps[K: ClassTag](drm: CheckpointedDrm[K]): CheckpointedOps[K] =
    new CheckpointedOps[K](drm)

  implicit def drm2Checkpointed[K](drm: DrmLike[K]): CheckpointedDrm[K] = drm.checkpoint()

  // ============== Decompositions ===================

  /**
   * Distributed _thin_ QR. A'A must fit in a memory, i.e. if A is m x n, then n should be pretty
   * controlled (<5000 or so). <P>
   *
   * It is recommended to checkpoint A since it does two passes over it. <P>
   *
   * It also guarantees that Q is partitioned exactly the same way (and in same key-order) as A, so
   * their RDD should be able to zip successfully.
   */
  def dqrThin[K: ClassTag](A: DrmLike[K], checkRankDeficiency: Boolean = true): (DrmLike[K], Matrix) =
    DQR.dqrThin(A, checkRankDeficiency)

  /**
   * Distributed Stochastic Singular Value decomposition algorithm.
   *
   * @param A input matrix A
   * @param k request SSVD rank
   * @param p oversampling parameter
   * @param q number of power iterations
   * @return (U,V,s). Note that U, V are non-checkpointed matrices (i.e. one needs to actually use them
   *         e.g. save them to hdfs in order to trigger their computation.
   */
  def dssvd[K: ClassTag](A: DrmLike[K], k: Int, p: Int = 15, q: Int = 0):
  (DrmLike[K], DrmLike[Int], Vector) = DSSVD.dssvd(A, k, p, q)

  /**
   * Distributed Stochastic PCA decomposition algorithm. A logical reflow of the "SSVD-PCA options.pdf"
   * document of the MAHOUT-817.
   *
   * @param A input matrix A
   * @param k request SSVD rank
   * @param p oversampling parameter
   * @param q number of power iterations (hint: use either 0 or 1)
   * @return (U,V,s). Note that U, V are non-checkpointed matrices (i.e. one needs to actually use them
   *         e.g. save them to hdfs in order to trigger their computation.
   */
  def dspca[K: ClassTag](A: DrmLike[K], k: Int, p: Int = 15, q: Int = 0):
  (DrmLike[K], DrmLike[Int], Vector) = DSPCA.dspca(A, k, p, q)


}
