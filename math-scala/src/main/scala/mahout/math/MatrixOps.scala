package mahout.math

import org.apache.mahout.math.{Vector, Matrix}
import scala.collection.JavaConversions._
import org.apache.mahout.math.function.{DoubleFunction, Functions}
import scala.math._

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

  def -=(that: Matrix) = m.assign(that, Functions.MINUS)

  def +=(that: Double) = m.assign(new DoubleFunction {
    def apply(x: Double): Double = x + that
  })

  def -=(that: Double) = +=(-that)

  def +(that: Matrix) = cloned += that

  def -(that: Matrix) = cloned -= that

  // m.plus(that)?

  def +(that: Double) = cloned += that

  def -(that: Double) = cloned -= that

  /**
   * Hadamard product
   *
   * @param that
   * @return
   */

  def *(that: Matrix) = cloned *= that

  def *(that: Double) = cloned *= that

  /**
   * in-place Hadamard product. We probably don't want to use assign
   * to optimize for sparse operations, in case of Hadamard product
   * it really can be done
   * @param that
   */
  def *=(that: Matrix) = {
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

  def *=(that: Double) = {
    m.iterateAll().foreach(slice => {
      val r = slice.index()
      slice.nonZeroes().foreach(el => {
        val c = el.index()
        val v = el.get() * that
        m.setQuick(r, c, v)
      })
    })
    m
  }

  def norm = sqrt(m.aggregate(Functions.PLUS, Functions.SQUARE))

  def pnorm(p: Int) = pow(m.aggregate(Functions.PLUS, Functions.chain(Functions.ABS, Functions.pow(p))), 1.0 / p)

  def apply(row: Int, col: Int) = m.get(row, col)

  def update(row: Int, col: Int, v: Double): Matrix = {
    m.setQuick(row, col, v);
    m
  }

  def update(rowRange: Range, colRange: Range, that: Matrix) = apply(rowRange, colRange) := that

  def update(row: Int, colRange: Range, that: Vector) = apply(row, colRange) := that

  def update(rowRange: Range, col: Int, that: Vector) = apply(rowRange, col) := that

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

  def := (f: (Int, Int, Double) => Double): Matrix = {
    for (r <- 0 until nrow; c <- 0 until ncol) m(r, c) = f(r, c, m(r, c))
    m
  }

  def cloned = m.like := m

  /**
   * Ideally, we would probably want to override equals(). But that is not
   * possible without modifying AbstractMatrix implementation in Mahout
   * which would require discussion at Mahout team.
   * @param that
   * @return
   */
  def equiv(that: Matrix) =
    that != null &&
      nrow == that.nrow &&
      m.view.zip(that).forall(t => {
        t._1.equiv(t._2)
      })

  def nequiv(that: Matrix) = !equiv(that)

}

object MatrixOps {

}
