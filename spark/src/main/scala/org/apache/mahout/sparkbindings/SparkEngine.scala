package org.apache.mahout.sparkbindings

import org.apache.mahout.math.scalabindings.drm.{CacheHint, DrmLike, CheckpointedDrm, DistributedEngine}
import org.apache.mahout.sparkbindings.drm.{CheckpointedDrmSpark, DrmRddInput, BlockifiedDrmRdd}
import org.apache.mahout.math.{DenseVector, Vector}
import scala.reflect.ClassTag
import org.apache.mahout.math.scalabindings.drm.logical._
import org.apache.spark.storage.StorageLevel
import org.apache.mahout.sparkbindings.blas._
import org.apache.mahout.math.scalabindings.drm.logical.OpAtAnyKey
import org.apache.mahout.math.scalabindings.drm.logical.OpAx
import org.apache.mahout.math.scalabindings.drm.logical.OpAtx
import org.apache.mahout.math.scalabindings.drm.logical.OpRowRange
import org.apache.mahout.math.scalabindings.drm.logical.OpAt
import org.apache.mahout.math.scalabindings.drm.logical.OpAtB
import org.apache.mahout.math.scalabindings.drm.logical.OpAewScalar
import org.apache.mahout.math.scalabindings.drm.logical.OpTimesRightMatrix
import org.apache.mahout.math.scalabindings.drm.logical.OpAewB
import org.apache.mahout.math.scalabindings.drm.logical.OpAtA
import org.apache.mahout.math.scalabindings.drm.logical.OpABt
import SparkEngine._

object SparkEngine extends DistributedEngine {


  /**
   * Reorganize every partition into a single in-core matrix
   * @return
   */
  def blockify[K](drm:CheckpointedDrm[K]): BlockifiedDrmRdd[K] =
    org.apache.mahout.sparkbindings.drm.blockify(rdd = drm.rdd, blockncol = drm.ncol)

  def colSums[K](drm: CheckpointedDrm[K]): Vector = {
    val n = drm.ncol

    drm.rdd
        // Throw away keys
        .map(_._2)
        // Fold() doesn't work with kryo still. So work around it.
        .mapPartitions(iter => {
      val acc = ((new DenseVector(n): Vector) /: iter)((acc, v) => acc += v)
      Iterator(acc)
    })
        // Since we preallocated new accumulator vector per partition, this must not cause any side
        // effects now.
        .reduce(_ += _)
  }

  /** Engine-specific colMeans implementation based on a checkpoint. */
  def colMeans[K](drm: CheckpointedDrm[K]): Vector = if (drm.nrow == 0) drm.colSums() else drm.colSums() /= drm.nrow

  /**
   * Perform default expression rewrite. Return physical plan that we can pass to exec(). <P>
   *
   * A particular physical engine implementation may choose to either use or not use these rewrites
   * as a useful basic rewriting rule.<P>
   */
  override def optimizerRewrite[K: ClassTag](action: DrmLike[K]): DrmLike[K] = super.optimizerRewrite(action)

  /** Translate logical pipeline into physical engine plan. */
  def toPhysical[K: ClassTag](plan: CheckpointAction[K], ch: CacheHint.CacheHint): CheckpointedDrm[K] = {

    // Spark-specific Physical Plan translation.
    val rdd = translate2Physical(plan)

    val newcp = new CheckpointedDrmSpark(
      rdd = rdd,
      _nrow = nrow,
      _ncol = ncol,
      _cacheStorageLevel = cacheHint2Spark(cacheHint),
      partitioningTag = plan.partitioningTag
    )
    cp = Some(newcp)
    newcp.cache()
  }

  private def cacheHint2Spark(cacheHint: CacheHint.CacheHint): StorageLevel = cacheHint match {
    case CacheHint.NONE => StorageLevel.NONE
    case CacheHint.DISK_ONLY => StorageLevel.DISK_ONLY
    case CacheHint.DISK_ONLY_2 => StorageLevel.DISK_ONLY_2
    case CacheHint.MEMORY_ONLY => StorageLevel.MEMORY_ONLY
    case CacheHint.MEMORY_ONLY_2 => StorageLevel.MEMORY_ONLY_2
    case CacheHint.MEMORY_ONLY_SER => StorageLevel.MEMORY_ONLY_SER
    case CacheHint.MEMORY_ONLY_SER_2 => StorageLevel.MEMORY_ONLY_SER_2
    case CacheHint.MEMORY_AND_DISK => StorageLevel.MEMORY_AND_DISK
    case CacheHint.MEMORY_AND_DISK_2 => StorageLevel.MEMORY_AND_DISK_2
    case CacheHint.MEMORY_AND_DISK_SER => StorageLevel.MEMORY_AND_DISK_SER
    case CacheHint.MEMORY_AND_DISK_SER_2 => StorageLevel.MEMORY_AND_DISK_SER_2
  }

  /** Translate previously optimized physical plan */
  private def translate2Physical[K: ClassTag](oper: DrmLike[K]): DrmRddInput[K] = {
    // I do explicit evidence propagation here since matching via case classes seems to be loosing
    // it and subsequently may cause something like DrmRddInput[Any] instead of [Int] or [String].
    // Hence you see explicit evidence attached to all recursive exec() calls.
    oper match {
      // If there are any such cases, they must go away in pass1. If they were not, then it wasn't
      // the A'A case but actual transposition intent which should be removed from consideration
      // (we cannot do actual flip for non-int-keyed arguments)
      case OpAtAnyKey(_) =>
        throw new IllegalArgumentException("\"A\" must be Int-keyed in this A.t expression.")
      case op@OpAt(a) => At.at(op, exec(a)(op.classTagA))
      case op@OpABt(a, b) => ABt.abt(op, exec(a)(op.classTagA), exec(b)(op.classTagB))
      case op@OpAtB(a, b) => AtB.atb_nograph(op, exec(a)(op.classTagA), exec(b)(op.classTagB),
        zippable = a.partitioningTag == b.partitioningTag)
      case op@OpAtA(a) => AtA.at_a(op, exec(a)(op.classTagA))
      case op@OpAx(a, x) => Ax.ax_with_broadcast(op, exec(a)(op.classTagA))
      case op@OpAtx(a, x) => Ax.atx_with_broadcast(op, exec(a)(op.classTagA))
      case op@OpAewB(a, b, '+') => AewB.a_plus_b(op, exec(a)(op.classTagA), exec(b)(op.classTagB))
      case op@OpAewB(a, b, '-') => AewB.a_minus_b(op, exec(a)(op.classTagA), exec(b)(op.classTagB))
      case op@OpAewB(a, b, '*') => AewB.a_hadamard_b(op, exec(a)(op.classTagA), exec(b)(op.classTagB))
      case op@OpAewB(a, b, '/') => AewB.a_eldiv_b(op, exec(a)(op.classTagA), exec(b)(op.classTagB))
      case op@OpAewScalar(a, s, "+") => AewB.a_plus_scalar(op, exec(a)(op.classTagA), s)
      case op@OpAewScalar(a, s, "-") => AewB.a_minus_scalar(op, exec(a)(op.classTagA), s)
      case op@OpAewScalar(a, s, "-:") => AewB.scalar_minus_a(op, exec(a)(op.classTagA), s)
      case op@OpAewScalar(a, s, "*") => AewB.a_times_scalar(op, exec(a)(op.classTagA), s)
      case op@OpAewScalar(a, s, "/") => AewB.a_div_scalar(op, exec(a)(op.classTagA), s)
      case op@OpAewScalar(a, s, "/:") => AewB.scalar_div_a(op, exec(a)(op.classTagA), s)
      case op@OpRowRange(a, _) => Slicing.rowRange(op, exec(a)(op.classTagA))
      case op@OpTimesRightMatrix(a, _) => AinCoreB.rightMultiply(op, exec(a)(op.classTagA))
      // Custom operators, we just execute them
      case blockOp: OpMapBlock[K, _] => blockOp.exec(src = exec(blockOp.A)(blockOp.classTagA))
      case cp: CheckpointedDrm[K] => new DrmRddInput[K](rowWiseSrc = Some((cp.ncol, cp.rdd)))
      case _ => throw new IllegalArgumentException("Internal:Optimizer has no exec policy for operator %s."
          .format(oper))

    }
  }


}

