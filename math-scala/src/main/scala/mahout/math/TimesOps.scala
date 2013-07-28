package mahout.math

import org.apache.mahout.math.{MatrixTimesOps, Matrix, DiagonalMatrix}

/**
 * Created with IntelliJ IDEA.
 * User: dmitriy
 * Date: 7/17/13
 * Time: 11:01 PM
 * To change this template use File | Settings | File Templates.
 */
class TimesOps(m: MatrixTimesOps) {

  def :%*%(that: Matrix) = m.timesRight(that)

  def %*%:(that: Matrix) = m.timesLeft(that)


}
