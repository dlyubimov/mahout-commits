package mahout.math

import org.apache.mahout.math.{Vector, Matrix}
import scala.collection.JavaConversions._
import org.apache.mahout.math.function.{DoubleFunction, Functions}

/**
 * Created with IntelliJ IDEA.
 * User: dmitriy
 * Date: 6/21/13
 * Time: 10:27 PM
 * To change this template use File | Settings | File Templates.
 */
class MatrixOps(val m: Matrix) {

  def nrow = m.rowSize()

  def ncol = m.columnSize()

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

  def +=(that: Matrix) = m.assign(that, Functions.PLUS)
  def +=(that: Double) = m.assign(new DoubleFunction {
    def apply(x: Double): Double = that + x
  })

  def +(that: Matrix) = cloned += that
  def +(that: Double) = cloned += that

  /**
   * Hadamard product
   *
   * @param that
   * @return
   */
  def *(that:Matrix) = cloned *= that

  /**
   * in-place Hadamard product
   * @param that
   */
  def *=(that: Matrix): Matrix = {
    m.iterateAll().foreach(slice => {
      val r = slice.index()
      slice.nonZeroes().foreach(el => {
        val c = el.index()
        val v = el.get() * that.get(r, c)
        m.setQuick(r, c, v)
      })
    })
    m
  }


  def apply(row: Int, col: Int) = m.get(row, col)

  def apply(rowRange: Range, colRange: Range): Matrix = {

    if (rowRange.length == 0 &&
      colRange.length == 0) return m

    val rr = if (rowRange.length == 0) (0 until m.nrow)
    else rowRange
    val cr = if (colRange.length == 0) (0 until m.ncol)
    else colRange

    return m.viewPart(rr.start, rr.length, cr.start, cr.length)

  }

  def apply(row: Int, colRange: Range): Vector = {
    var r = m.viewRow(row)
    if (colRange.length > 0) r = r.viewPart(colRange.start, colRange.length)
    r
  }

  def apply(rowRange: Range, col: Int): Vector = {
    var c = m.viewColumn(col)
    if (rowRange.length > 0) c = c.viewPart(rowRange.start, rowRange.length)
    c
  }

  def t = m.transpose()

  def det = m.determinant()

  def sum = m.zSum()

  def :=(that: Matrix) = m.assign(that)

  def cloned = m.like := m


  /**
   * Assigning from a row-wise collection of vectors
   * @param that
   */
  def :=(that: TraversableOnce[Vector]) = {
    var row = 0
    that.foreach(v => {
      m.assignRow(row, v)
      row += 1
    })
  }
}

object MatrixOps {

}
