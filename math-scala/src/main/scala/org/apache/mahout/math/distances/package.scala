package org.apache.mahout.math

/**
 * @author dmitriy
 */
package object distances {

  import org.apache.mahout.math.scalabindings._
  import RLikeOps._
  import math._

  type DistFunc = (Vector, Vector) => Double

  val euclSq: DistFunc = (v1, v2) => {
    val v = v1 - v2
    v dot v
  }

  val eucl: DistFunc = (v1, v2) => sqrt(euclSq(v1, v2))

}
