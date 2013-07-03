package mahout.math

import org.apache.mahout.math.Vector

/**
 * Syntactic sugar for mahout vectors
 * @param v Mahout vector
 */
class VectorOps(val v: Vector) {

  def apply(i: Int) = v.get(i)

  def sum = v.zSum()

  def :=(that: Vector) = v.assign(that)

  def length = v.size()

  def cloned: Vector = {
    val vnew = v.like()
    vnew := v
    vnew
  }

}
