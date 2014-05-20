package org.apache.mahout.math.drm

import scala.reflect.ClassTag
import logical._
import org.apache.mahout.math.{Matrix, Vector}
import DistributedEngine._
import org.apache.mahout.math.scalabindings._
import RLikeOps._

/** Abstraction of optimizer/distributed engine */
trait DistributedEngine {

  /**
   * First optimization pass. Return physical plan that we can pass to exec(). This rewrite may
   * introduce logical constructs (including engine-specific ones) that user DSL cannot even produce
   * per se.
   * <P>
   *   
   * A particular physical engine implementation may choose to either use the default rewrites or
   * build its own rewriting rules.
   * <P>
   */
  def optimizerRewrite[K: ClassTag](action: DrmLike[K]): DrmLike[K] = pass3(pass2(pass1(action)))

  /** Second optimizer pass. Translate previously rewritten logical pipeline into physical engine plan. */
  def toPhysical[K: ClassTag](plan: DrmLike[K], ch: CacheHint.CacheHint): CheckpointedDrm[K]

  /** Engine-specific colSums implementation based on a checkpoint. */
  def colSums[K:ClassTag](drm:CheckpointedDrm[K]):Vector

  /** Engine-specific colMeans implementation based on a checkpoint. */
  def colMeans[K:ClassTag](drm:CheckpointedDrm[K]):Vector

  /** Broadcast support */
  def drmBroadcast(v: Vector)(implicit dc: DistributedContext): BCast[Vector]

  /** Broadcast support */
  def drmBroadcast(m: Matrix)(implicit dc: DistributedContext): BCast[Matrix]

}

object DistributedEngine {

  /** This is mostly multiplication operations rewrites */
  private def pass1[K: ClassTag](action: DrmLike[K]): DrmLike[K] = {

    action match {
      case OpAB(OpAt(a), b) if (a == b) => OpAtA(pass1(a))
      case OpABAnyKey(OpAtAnyKey(a), b) if (a == b) => OpAtA(pass1(a))

      // For now, rewrite left-multiply via transpositions, i.e.
      // inCoreA %*% B = (B' %*% inCoreA')'
      case op@OpTimesLeftMatrix(a, b) =>
        OpAt(OpTimesRightMatrix(A = OpAt(pass1(b)), right = a.t))

      // Stop at checkpoints
      case cd: CheckpointedDrm[_] => action

      // For everything else we just pass-thru the operator arguments to optimizer
      case uop: AbstractUnaryOp[_, K] =>
        uop.A = pass1(uop.A)(uop.classTagA)
        uop
      case bop: AbstractBinaryOp[_, _, K] =>
        bop.A = pass1(bop.A)(bop.classTagA)
        bop.B = pass1(bop.B)(bop.classTagB)
        bop
    }
  }

  /** This would remove stuff like A.t.t that previous step may have created */
  private def pass2[K: ClassTag](action: DrmLike[K]): DrmLike[K] = {
    action match {
      // A.t.t => A
      case OpAt(top@OpAt(a)) => pass2(a)(top.classTagA)

      // Stop at checkpoints
      case cd: CheckpointedDrm[_] => action

      // For everything else we just pass-thru the operator arguments to optimizer
      case uop: AbstractUnaryOp[_, K] =>
        uop.A = pass2(uop.A)(uop.classTagA)
        uop
      case bop: AbstractBinaryOp[_, _, K] =>
        bop.A = pass2(bop.A)(bop.classTagA)
        bop.B = pass2(bop.B)(bop.classTagB)
        bop
    }
  }

  /** Some further rewrites that are conditioned on A.t.t removal */
  private def pass3[K: ClassTag](action: DrmLike[K]): DrmLike[K] = {
    action match {

      // matrix products.
      case OpAB(a, OpAt(b)) => OpABt(pass3(a), pass3(b))

      // AtB cases that make sense.
      case OpAB(OpAt(a), b) if (a.partitioningTag == b.partitioningTag) => OpAtB(pass3(a), pass3(b))
      case OpABAnyKey(OpAtAnyKey(a), b) => OpAtB(pass3(a), pass3(b))

      // Need some cost to choose between the following.

      case OpAB(OpAt(a), b) => OpAtB(pass3(a), pass3(b))
      //      case OpAB(OpAt(a), b) => OpAt(OpABt(OpAt(pass1(b)), pass1(a)))
      case OpAB(a, b) => OpABt(pass3(a), OpAt(pass3(b)))
      // Rewrite A'x
      case op@OpAx(op1@OpAt(a), x) => OpAtx(pass3(a)(op1.classTagA), x)

      // Stop at checkpoints
      case cd: CheckpointedDrm[_] => action

      // For everything else we just pass-thru the operator arguments to optimizer
      case uop: AbstractUnaryOp[_, K] =>
        uop.A = pass3(uop.A)(uop.classTagA)
        uop
      case bop: AbstractBinaryOp[_, _, K] =>
        bop.A = pass3(bop.A)(bop.classTagA)
        bop.B = pass3(bop.B)(bop.classTagB)
        bop
    }
  }

}