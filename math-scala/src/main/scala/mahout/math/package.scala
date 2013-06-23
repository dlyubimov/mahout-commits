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

  def diag(v: Vector) = new DiagonalMatrix(v)

  def diag(v: Double, size: Int) = new DiagonalMatrix(v, size)

  def dense[R](rows: R*): DenseMatrix = {
    val data = for (r <- rows) yield {
      r match {
        case n: Number => Array(n.doubleValue())
        case t: Tuple1[_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple2[_, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple3[_, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple4[_, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple5[_, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple6[_, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple7[_, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple8[_, _, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple9[_, _, _, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple10[_, _, _, _, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple11[_, _, _, _, _, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple12[_, _, _, _, _, _, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple13[_, _, _, _, _, _, _, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple14[_, _, _, _, _, _, _, _, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Array[Double] => t
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

}
