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

package org.apache.mahout.sparkbindings

import org.apache.mahout.math._
import org.apache.spark.SparkContext
import scala.collection.JavaConversions._
import org.apache.hadoop.io.{LongWritable, Text, IntWritable, Writable}
import org.apache.log4j.Logger
import java.lang.Math
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.mahout.math.scalabindings._
import RLikeOps._
import SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.mahout.math.scalabindings.drm.{CheckpointedOps, DrmLike, BlockifiedDrmTuple, DrmTuple}
import org.apache.mahout.math.drm.decompositions.{DSSVD, DSPCA, DQR}


package object drm {

  private[drm] final val log = Logger.getLogger("org.apache.mahout.sparkbindings");

  /** Row-wise organized DRM rdd type */
  type DrmRdd[K] = RDD[DrmTuple[K]]

  /**
   * Blockifed DRM rdd (keys of original DRM are grouped into array corresponding to rows of Matrix
   * object value
   */
  type BlockifiedDrmRdd[K] = RDD[BlockifiedDrmTuple[K]]


  implicit def input2drmRdd[K](input: DrmRddInput[K]): DrmRdd[K] = input.toDrmRdd()

  implicit def input2blockifiedDrmRdd[K](input: DrmRddInput[K]): BlockifiedDrmRdd[K] = input.toBlockifiedDrmRdd()

  implicit def cpDrm2DrmRddInput[K: ClassTag](cp: CheckpointedDrm[K]): DrmRddInput[K] =
    new DrmRddInput(rowWiseSrc = Some(cp.ncol -> cp.rdd))

  implicit def drm2drmOps[K <% Writable : ClassTag](drm: CheckpointedDrmSpark[K]): CheckpointedOps[K] =
    new CheckpointedOps[K](drm)


  implicit def drmLike2Checkpointed[K](drm: DrmLike[K]): CheckpointedDrm[K] = drm.checkpoint()

  implicit def bcast2Matrix(bcast: Broadcast[_ <: Matrix]): Matrix = bcast.value

  implicit def bcast2Vector(bcast: Broadcast[_ <: Vector]): Vector = bcast.value


  /**
   * Load DRM from hdfs (as in Mahout DRM format)
   *
   * @param path
   * @param sc spark context (wanted to make that implicit, doesn't work in current version of
   *           scala with the type bounds, sorry)
   *
   * @return DRM[Any] where Any is automatically translated to value type
   */
  def drmFromHDFS (path: String)(implicit sc: SparkContext): CheckpointedDrmSpark[_] = {
    val rdd = sc.sequenceFile(path, classOf[Writable], classOf[VectorWritable]).map(t => (t._1, t._2.get()))

    val key = rdd.map(_._1).take(1)(0)
    val keyWClass = key.getClass.asSubclass(classOf[Writable])

    val key2val = key match {
      case xx: IntWritable => (v: AnyRef) => v.asInstanceOf[IntWritable].get
      case xx: Text => (v: AnyRef) => v.asInstanceOf[Text].toString
      case xx: LongWritable => (v: AnyRef) => v.asInstanceOf[LongWritable].get
      case xx: Writable => (v: AnyRef) => v
    }

    val val2key = key match {
      case xx: IntWritable => (x: Any) => new IntWritable(x.asInstanceOf[Int])
      case xx: Text => (x: Any) => new Text(x.toString)
      case xx: LongWritable => (x: Any) => new LongWritable(x.asInstanceOf[Int])
      case xx: Writable => (x: Any) => x.asInstanceOf[Writable]
    }

    val  km = key match {
      case xx: IntWritable => implicitly[ClassTag[Int]]
      case xx: Text => implicitly[ClassTag[String]]
      case xx: LongWritable => implicitly[ClassTag[Long]]
      case xx: Writable => ClassTag(classOf[Writable])
    }

    {
      implicit def getWritable(x: Any): Writable = val2key()
      new CheckpointedDrmSpark(rdd.map(t => (key2val(t._1), t._2)))(km.asInstanceOf[ClassTag[Any]])
    }
  }

  /** Shortcut to parallelizing matrices with indices, ignore row labels. */
  def drmParallelize(m: Matrix, numPartitions: Int = 1)
      (implicit sc: SparkContext) =
    drmParallelizeWithRowIndices(m, numPartitions)(sc)

  /** Parallelize in-core matrix as spark distributed matrix, using row ordinal indices as data set keys. */
  def drmParallelizeWithRowIndices(m: Matrix, numPartitions: Int = 1)
      (implicit sc: SparkContext)
  : CheckpointedDrm[Int] = {

    new CheckpointedDrmSpark(parallelizeInCore(m, numPartitions))
  }

  private[sparkbindings] def parallelizeInCore(m: Matrix, numPartitions: Int = 1)
      (implicit sc: SparkContext): DrmRdd[Int] = {

    val p = (0 until m.nrow).map(i => i -> m(i, ::))
    sc.parallelize(p, numPartitions)

  }

  /** Parallelize in-core matrix as spark distributed matrix, using row labels as a data set keys. */
  def drmParallelizeWithRowLabels(m: Matrix, numPartitions: Int = 1)
      (implicit sc: SparkContext)
  : CheckpointedDrmSpark[String] = {


    // In spark 0.8, I have patched ability to parallelize kryo objects directly, so no need to
    // wrap that into byte array anymore
    val rb = m.getRowLabelBindings
    val p = for (i: String <- rb.keySet().toIndexedSeq) yield i -> m(rb(i), ::)


    new CheckpointedDrmSpark(sc.parallelize(p, numPartitions))
  }

  /** This creates an empty DRM with specified number of partitions and cardinality. */
  def drmParallelizeEmpty(nrow: Int, ncol: Int, numPartitions: Int = 10)
      (implicit sc: SparkContext): CheckpointedDrm[Int] = {
    val rdd = sc.parallelize(0 to numPartitions, numPartitions).flatMap(part => {
      val partNRow = (nrow - 1) / numPartitions + 1
      val partStart = partNRow * part
      val partEnd = Math.min(partStart + partNRow, nrow)

      for (i <- partStart until partEnd) yield (i, new RandomAccessSparseVector(ncol): Vector)
    })
    new CheckpointedDrmSpark[Int](rdd, nrow, ncol)
  }

  def drmParallelizeEmptyLong(nrow: Long, ncol: Int, numPartitions: Int = 10)
      (implicit sc: SparkContext): CheckpointedDrmSpark[Long] = {
    val rdd = sc.parallelize(0 to numPartitions, numPartitions).flatMap(part => {
      val partNRow = (nrow - 1) / numPartitions + 1
      val partStart = partNRow * part
      val partEnd = Math.min(partStart + partNRow, nrow)

      for (i <- partStart until partEnd) yield (i, new RandomAccessSparseVector(ncol): Vector)
    })
    new CheckpointedDrmSpark[Long](rdd, nrow, ncol)
  }

  def drmWrap[K : ClassTag](
      rdd: DrmRdd[K],
      nrow: Int = -1,
      ncol: Int = -1
      ): CheckpointedDrm[K] =
    new CheckpointedDrmSpark[K](
      rdd = rdd,
      _nrow = nrow,
      _ncol = ncol
    )


  /** Broadcast vector (Mahout vectors are not closure-friendly, use this instead. */
  def drmBroadcast(x: Vector)(implicit sc: SparkContext): Broadcast[Vector] = sc.broadcast(x)

  /** Broadcast in-core Mahout matrix. Use this instead of closure. */
  def drmBroadcast(m: Matrix)(implicit sc: SparkContext): Broadcast[Matrix] = sc.broadcast(m)

  def blockify[K: ClassTag](rdd: DrmRdd[K], blockncol: Int): BlockifiedDrmRdd[K] = {

    rdd.mapPartitions(iter => {

      if (!iter.hasNext) Iterator.empty
      else {

        val data = iter.toIterable
        val keys = data.map(t => t._1).toArray[K]
        val vectors = data.map(t => t._2).toArray

        val block = new SparseRowMatrix(vectors.size, blockncol, vectors)

        Iterator(keys -> block)
      }
    })
  }

  def deblockify[K: ClassTag](rdd: BlockifiedDrmRdd[K]): DrmRdd[K] =

  // Just flat-map rows, connect with the keys
    rdd.flatMap({
      case (blockKeys: Array[K], block: Matrix) =>

        blockKeys.ensuring(blockKeys.size == block.nrow)
        blockKeys.view.zipWithIndex.map({
          case (key, idx) =>
            var v = block(idx, ::)

            // If a view rather than a concrete vector, clone into a concrete vector in order not to
            // attempt to serialize outer matrix when we save it (Although maybe most often this
            // copying is excessive?)
            // if (v.isInstanceOf[MatrixVectorView]) v = v.cloned
            key -> v
        })

    })



}
