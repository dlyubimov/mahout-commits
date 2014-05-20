package org.apache.mahout.sparkbindings.drm

import org.apache.mahout.math.drm.BCast
import org.apache.spark.broadcast.Broadcast

class SparkBCast[T](val sbcast: Broadcast[T]) extends BCast[T] with Serializable {
  def value: T = sbcast.value
}
