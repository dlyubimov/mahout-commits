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

import org.apache.mahout.math._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import mahout.math._
import mahout.math.RLikeOps._
import collection.JavaConversions._
import org.apache.hadoop.io.Writable

/**
 * Additional experimental operations over BaseDRM implementation. I will possibly move them up to
 * the DRMBase once they stabilize.
 *
 */
class ExtendedDRMOps[K <% Writable : ClassManifest](val drm: BaseDRM[K]) {

  /**
   * Reorganize every partition into a single in-core matrix
   * @return
   */
  def blockify: RDD[(Array[K], Matrix)] = {
    val blockncol = drm.ncol

    drm.getRDD.mapPartitions(iter => {

      if (!iter.hasNext) Iterator.empty
      else {

        val data = iter.toIterable
        val keys = data.map(t => t._1).toArray
        val vectors = data.map(t => t._2).toArray

        val block = if (vectors(0).isDense) {
          dense(vectors)
        } else {
          val mtx = new SparseMatrix(vectors.size, blockncol)
          for (i <- 0 until vectors.size) mtx(i, ::) := vectors(i)
          mtx
        }

        Iterator(keys -> block)
      }
    })
  }

  /**
   * Computes A' * A for tall but skinny A matrices. Comes up a lot in SSVD and ALS flavors alike.
   * @param maxInMemNCol maximum ncol of A during which in-memory accumulation is possible.
   * @return
   */
  def t_sq_slim(maxInMemNCol: Int = 2000): Matrix = {

    val ncol = drm.ncol

    if (ncol > maxInMemNCol) throw new UnsupportedOperationException(
      "wider matrix operation A'A not implemented yet.")

    val resSym = drm.getRDD.mapPartitions(iter => {

      val ut = new UpperTriangular(ncol)

      // strategy is to add to an outer product
      // of each row to the upper triangular accumulator.
      iter.foreach(t => {
        val v = t._2
        if (v.isDense) {

          for (row <- 0 until v.length; col <- row until v.length)
            ut(row, col) = ut(row, col) + v(row) * v(col)
        } else
          v.nonZeroes().foreach(elrow => {
            v.nonZeroes().toIterator.filter(_.index >= elrow.index).foreach(elcol => {
              val row = elrow.index
              val col = elcol.index
              ut(row, col) = ut(row, col) + elrow.get() * elcol.get()
            })
          })
      })

      val ba = new DenseVector(ut.getData).toByteArray
      Iterator(ba)

    }).collect().map(ba =>
      DRMVectorOps.fromByteArray(ba)
    ).reduce(_ += _)

    new DenseSymmetricMatrix(resSym)
  }

}

