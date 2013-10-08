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

import mahout.math.RLikeOps._
import org.apache.spark.rdd.RDD
import org.apache.mahout.math.{DenseVector, RandomAccessSparseVector, Vector}
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import org.apache.spark.bagel.{Bagel, Combiner}
import org.apache.spark.SparkContext._

/**
 *
 * Transposed DRM.<P/>
 *
 * We experiment with a few things there, mostly running on Bagel. So I try to keep things easily
 * pluggable land replaceable which is guided by <code>computed</code> property. <P/>
 *
 * TODO: add algorithm that uses blockify + sparse vector messages instead of element messages. I
 * expect it to be quite faster than element-based messages.
 *
 */
private[sparkbindings] class TransposedDRM(val transposee: BaseDRM[Int], withCombiners: Boolean = false) extends BaseDRM[Int] {

  // transposeAlg2 (with combiners) which, cpu-wise, seems to run longer,
  // most likely because of lack of much of aggregation effect and serialization overhead,
  // but might be faster i/o wise in some situations(?)
  private lazy val computed: RDD[(Int, Vector)] = if (withCombiners) transposeAlg2 else transposeAlg1

  override def getRDD: RDD[(Int, Vector)] = computed

  override protected def computeNRow = transposee.ncol

  override protected def computeNCol = {
    val tncol: Int = transposee.nrow.toInt
    if (tncol != transposee.nrow)
      throw new IllegalArgumentException("matrix too tall, " +
        "transposition not possible (nrow > Integer.MAX_VALUE).")
    tncol
  }

  /**
   * transpose1: do bagel thing without combiner
   * @return  transposed RDD
   */
  private def transposeAlg1: RDD[(Int, Vector)] = {
    val newncol = ncol
    implicit val sc = transposee.getRDD.context

    val result: RDD[(Int, VectorVertex)] = drmParallelizeEmpty(nrow.toInt, ncol,
      numPartitions = transposee.getRDD.partitions.size).getRDD
      .map(t => (t._1, new VectorVertex()))


    val messages = transposee.getRDD.flatMap(t => {
      val newCol = t._1
      val vec = t._2
      vec.nonZeroes().map(el => {
        val newRow = el.index()
        (newRow, new ElementMessage[Int](newRow, newCol, el.get()))
      })
    })

    // TODO: add combiners. probably this will help.
    // TODO: for superhuge matrices we may have to do a few runs
    // to split message vectors in a few super steps.
    Bagel.run(sc, result, messages, numPartitions = transposee.getRDD.partitions.size)((vert, msgs, step) => {
      val ms = msgs.flatten
      val cnt = ms.size

      if (cnt > 0) {
        vert.v = if (newncol / cnt > 2.0) {
          s_log.debug("using sparse vector for a new transposed row")
          new RandomAccessSparseVector(newncol)
        }
        else {
          s_log.debug("using dense vector for new transposed row")
          new DenseVector(newncol)
        }

        ms.foreach(msg => vert.v.setQuick(msg.index, msg.value))
      }

      vert._active = false
      (vert, Array())
    })
      // remap into DRM-compliant RDD
      .map(t => (t._1, if (t._2.v==null) new RandomAccessSparseVector(newncol) else t._2.v ))
  }

  /**
   * a version of transpose algorithm but using Bagel with combiners.
   * @return
   */
  private def transposeAlg2: RDD[(Int, Vector)] = {

    val newncol = ncol
    implicit val sc = transposee.getRDD.context

    val combiner = new TransposeCombiner(newncol)

    val result: RDD[(Int, VectorVertex)] = drmParallelizeEmpty(nrow.toInt, ncol,
      numPartitions = transposee.getRDD.partitions.size).getRDD
      .map(t => (t._1, new VectorVertex()))

    val messages = transposee.getRDD.flatMap(t => {
      val newCol = t._1
      val vec = t._2
      vec.nonZeroes().map(el => {
        val newRow = el.index()
        (newRow, new ElementMessage[Int](newRow, newCol, el.get()))
      })
    })

    Bagel.run(sc, result, messages, combiner,
      numPartitions = transposee.getRDD.partitions.size)((vert, combined, step) => {

      combined.foreach(m => vert.v = m.v)

      vert._active = false
      (vert, Array())

    })
      // remove empty vectors from rowset
      .filter(_._2.v != null)
      // remap into DRM-compliant RDD
      .map(t => (t._1, t._2.v))

  }
}

private object TransposedDRM {
  final val s_log = Logger.getLogger(classOf[TransposedDRM])
}

private class TransposeCombiner(newncol: Int) extends Combiner[ElementMessage[Int], VectorMessage[Int]]
with Serializable {

  override def createCombiner(msg: ElementMessage[Int]): VectorMessage[Int] = {
    val v = new RandomAccessSparseVector(newncol)
    v.set(msg.index, msg.value)

    new VectorMessage[Int](msg.targetId, v)
  }

  override def mergeMsg(combiner: VectorMessage[Int], msg: ElementMessage[Int]): VectorMessage[Int] = {
    combiner.v(msg.index) = msg.value

    checkForDense(combiner)
  }

  override def mergeCombiners(a: VectorMessage[Int], b: VectorMessage[Int]): VectorMessage[Int] = {
    a.v += b.v
    checkForDense(a)
  }

  def checkForDense(combiner: VectorMessage[Int]): VectorMessage[Int] = {
    val v = combiner.v
    if (!v.isDense && v.getNumNonZeroElements * 2 > v.size()) {
      val densev = new DenseVector(v.size())
      densev := v
      combiner.v = densev
    }

    combiner
  }
}
