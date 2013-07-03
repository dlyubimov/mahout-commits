package mahout

import org.apache.mahout.math._
import scala.Tuple2

/**
 * Mahout matrices and vectors' scala syntactic sugar
 */
package object math {

  implicit def vector2vectorOps(v: Vector) = new VectorOps(v)

  implicit def matrix2matrixOps(m: Matrix) = new MatrixOps(m)

  implicit def seq2Vector(s: Seq[Double]) = new DenseVector(s.toArray)

  implicit def prod2Vector(s: Product) = new DenseVector(s.productIterator.
    map(_.asInstanceOf[Number].doubleValue()).toArray)

  implicit def tuple2TravOnce2svec[V <: AnyVal](sdata: List[Tuple2[Int, V]]) = svec(sdata)

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

  /**
   * Default initializes are always row-wise.
   * create a sparse,
   * e.g.
   * m = sparse(
   *    (0,5)::(9,3)::Nil,
   *    (2,3.5)::(7,8)::Nil
   * )
   *
   * @param rows
   * @return
   */

  def sparse(rows: Vector*): SparseRowMatrix = {
    val nrow = rows.size
    val ncol = rows.map(_.size()).max
    val m = new SparseRowMatrix(nrow, ncol)
    m := rows
    m

  }

  /**
   * create a sparse vector out of list of tuple2's
   * @param sdata
   * @return
   */
  def svec(sdata: TraversableOnce[Tuple2[Int, AnyVal]]) = {
    val cardinality = sdata.map(_._1).max + 1
    val initialCapacity = sdata.size
    val sv = new RandomAccessSparseVector(cardinality, initialCapacity)
    sdata.foreach(t => sv.setQuick(t._1, t._2.asInstanceOf[Number].doubleValue()))
    sv
  }

  def chol(m: Matrix) = new CholeskyDecomposition(m)

  def svd(m: Matrix) = {
    val svdObj = new SingularValueDecomposition(m)
    (svdObj.getU, svdObj.getV, new DenseVector(svdObj.getSingularValues))
  }


  def ::() = Range(0, 0)

}
