package mahout.math

import org.apache.mahout.math.{Vector, Matrix}

/**
 * Created with IntelliJ IDEA.
 * User: dmitriy
 * Date: 6/21/13
 * Time: 10:27 PM
 * To change this template use File | Settings | File Templates.
 */
class MatrixOps(val m:Matrix) {

  def %*% (that:Matrix) = m.times(that)
  def %*% (that:Double) = m.times(that)
  def %*% (that:Vector) = m.times(that)


}
