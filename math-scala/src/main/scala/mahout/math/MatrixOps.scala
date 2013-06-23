package mahout.math

import org.apache.mahout.math.{Vector, Matrix}
import scala.collection.JavaConversions._

/**
 * Created with IntelliJ IDEA.
 * User: dmitriy
 * Date: 6/21/13
 * Time: 10:27 PM
 * To change this template use File | Settings | File Templates.
 */
class MatrixOps(val m: Matrix) {

  def rows = m.rowSize()
  def cols = m.columnSize()

  /**
   * matrix-matrix multiplication
   * @param that
   * @return
   */
  def %*%(that: Matrix) = m.times(that)

  /**
   * matrix-scalar multiplication
   * @param that
   * @return
   */
  def %*%(that: Double) = m.times(that)

  /**
   * matrix-vector multiplication
   * @param that
   * @return
   */
  def %*%(that: Vector) = m.times(that)

  def +(that: Matrix) = m.plus(that)

  /**
   * Hadamard product
   *
   * @param that
   * @return
   */
  def *(that: Matrix) = {
    val m1 = m.like()
    m.iterateAll().foreach(slice => {
      val r = slice.index()
      slice.nonZeroes().foreach(el => {
        val c = el.index()
        val v = el.get() * that.get(r, c)
        m1.setQuick(r, c, v)
      })
    })
    m1
  }

  /**
   * in-place Hadamard product
   * @param that
   */
  def *=(that: Matrix): Unit = {
    m.iterateAll().foreach(slice => {
      val r = slice.index()
      slice.nonZeroes().foreach(el => {
        val c = el.index()
        val v = el.get() * that.get(r, c)
        m.setQuick(r, c, v)
      })
    })
  }

  def apply(row:Int,col:Int) = m.get(row,col)

  def t = m.transpose()

  def det = m.determinant()

  def := (that:Matrix) = m.assign(that)

}
