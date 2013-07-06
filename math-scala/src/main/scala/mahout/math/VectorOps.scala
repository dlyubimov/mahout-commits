package mahout.math

import org.apache.mahout.math.Vector
import scala.collection.JavaConversions._

/**
 * Syntactic sugar for mahout vectors
 * @param v Mahout vector
 */
class VectorOps(val v: Vector) {

  def apply(i: Int) = v.get(i)

  def apply(r: Range) = v.viewPart(r.start, r.length)

  def sum = v.zSum()

  def :=(that: Vector): Vector = {

    // assign op in Mahout requires same
    // cardinality between vectors .
    // we want to relax it here and require
    // v to have _at least_ as large cardinality
    // as "that".
    if (that.length == v.size())
      v.assign(that)
    else {
      that.nonZeroes().foreach(t=>v.setQuick(t.index,t.get))
      v
    }
  }

  def length = v.size()

  def cloned: Vector = {
    val vnew = v.like()
    vnew := v
    vnew
  }

}
