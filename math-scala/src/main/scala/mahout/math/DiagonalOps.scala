package mahout.math

import org.apache.mahout.math.{Matrix, DiagonalMatrix}

/**
 * Created with IntelliJ IDEA.
 * User: dmitriy
 * Date: 7/17/13
 * Time: 11:01 PM
 * To change this template use File | Settings | File Templates.
 */
class DiagonalOps(m: DiagonalMatrix) extends MatrixOps(m) {

  def :%*%(that: Matrix) = m.rightMult(that)

  def %*%:(that: Matrix) = m.leftMult(that)


}
