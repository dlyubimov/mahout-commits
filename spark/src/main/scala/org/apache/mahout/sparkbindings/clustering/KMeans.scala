package org.apache.mahout.sparkbindings.clustering

import org.apache.mahout.sparkbindings.drm._
import RLikeDrmOps._
import org.apache.mahout.math.scalabindings._
import RLikeOps._

import org.apache.mahout.math.{Vector, Matrix}
import org.apache.mahout.math.distances._
import org.apache.spark.storage.StorageLevel
import scala.reflect.ClassTag

/**
 * @author dmitriy
 */
object KMeans {


  /**
   *
   * @param drmData
   * @param inCoreC -- matrix with columns representing current cluster centers
   * @return updated centroid matrix
   */
  private def lLoydIteration[K: ClassTag](drmData: DrmLike[K], inCoreC: Matrix, dfunc: DistFunc = eucl):
  DrmLike[Int] = {

    // TODO: this is still tightly coupled to Spark
    implicit val sc = drmData.rdd.sparkContext

    val n = drmData.ncol
    val numCentroids = inCoreC.ncol
    val bcastC = drmBroadcast(inCoreC)

    val drmCentroids = drmData

        // Output assigned centroidi as a key for each row point.
        .mapBlock(ncol = n + 1) {

      case (keys, block) =>

        val assignedCentroids = for (r <- 0 until block.nrow) yield {
          val row = block(r, ::)
          val inCoreC = bcastC: Matrix

          // Find closest centroid -- TODO: this needs triangle inequality in case of euclidean
          // distance
          (0 until numCentroids).view.map(ci => dfunc(row, inCoreC(::, ci))).minValueIndex()
        }

        // Grow block with vector of 1.0 on the left -- this will serve as denominator of new 
        // centroid data to accumulate counts
        val newBlock = block.like(block.nrow, n + 1)
        newBlock(::, 0) := 1.0
        newBlock(::, 1 to n) := block

        assignedCentroids.toArray -> block
    }

        // Transposition aggregates vectors by centroid keys
        .t(::, 0 until numCentroids)

        .checkpoint()

    // collect centroid sizes into a vector and re-broadcast
    val vCSizes = drmBroadcast(drmCentroids(0 to 0, ::).collect(0, ::))

    // Finalize centroid matrix. First, slice off the counter row
    drmCentroids(1 to n, ::)

        // Then divide each centroid sum by its count, thus getting center.
        .mapBlock() {
      case (keys, block) =>
        for (col <- 0 until numCentroids) block(::, col) /= vCSizes.value(col)
        keys -> block
    }

  }

}
