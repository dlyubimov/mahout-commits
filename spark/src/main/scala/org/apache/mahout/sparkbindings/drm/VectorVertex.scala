package org.apache.mahout.sparkbindings.drm

import org.apache.mahout.math.Vector
import org.apache.spark.bagel.Vertex

class VectorVertex(vec: Vector = null, var _active: Boolean = true) extends Vertex {
  var v: Vector = vec

  def active: Boolean = _active

}
