package mahout

import org.apache.mahout.math._
import scala.Tuple9
import scala.Tuple16
import scala.Tuple17
import scala.Tuple18
import scala.Tuple5
import scala.Tuple19
import scala.Tuple6
import scala.Tuple11
import scala.Tuple20
import scala.Tuple7
import scala.Tuple12
import scala.Tuple21
import scala.Tuple8
import scala.Tuple13
import scala.Tuple22
import scala.Tuple14
import scala.Tuple1
import scala.Tuple2
import scala.Tuple15
import scala.Tuple3
import scala.Tuple4
import scala.Tuple10

/**
 * Mahout matrices and vectors' scala syntactic sugar
 */
package object math {

  implicit def vector2vectorOps(v: Vector) = new VectorOps(v)

  implicit def matrix2matrixOps(m: Matrix) = new MatrixOps(m)

  implicit def seq2Vector(s: Seq[Double]) = new DenseVector(s.toArray)

  implicit def prod2Vector(s: Product) = new DenseVector(s.productIterator.
    map(_.asInstanceOf[Number].doubleValue()).toArray)

  def diag(v: Vector) = new DiagonalMatrix(v)

  def diag(v: Double, size: Int) = new DiagonalMatrix(v, size)

  /**
   * Create dense matrix out of inline arguments -- rows -- which can be tuples,
   * iterables of Double, or just single Number (for columnar vectors)
   * @param rows
   * @tparam R
   * @return
   */
  def dense[R](rows: R*): DenseMatrix = {
    val data = for (r <- rows) yield {
      r match {
        case n: Number => Array(n.doubleValue())
        case t: Product => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Array[Double] => t
        case t: Iterable[Double] => t.toArray
        case t: Array[Array[Double]] => if (rows.size == 1)
          return new DenseMatrix(t)
        else
          throw new IllegalArgumentException(
            "double[][] data parameter can be the only argumentn for dense()")
        case _ => throw new IllegalArgumentException("unsupported type in the inline Matrix initializer")
      }
    }
    new DenseMatrix(data.toArray)
  }

  def chol(m: Matrix) = new CholeskyDecomposition(m)

  def svd(m: Matrix) = {
    val svdObj = new SingularValueDecomposition(m)
    (svdObj.getU, svdObj.getV, new DenseVector(svdObj.getSingularValues))
  }


  def ::() = Range(0, 0)

}
