package org.apache.mahout.sparkbindings

import org.apache.mahout.math.drm.{DistributedEngine, BCast, DistributedContext}
import org.apache.spark.SparkContext

class SparkDistributedContext(val sc: SparkContext) extends DistributedContext {

  val engine: DistributedEngine = SparkEngine

  def close() {
    sc.stop()
  }
}
