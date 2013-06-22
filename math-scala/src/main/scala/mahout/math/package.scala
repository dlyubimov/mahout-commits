package mahout

import org.apache.mahout.math.{DenseMatrix, Matrix, Vector}

package object math {

  implicit def vector2vectorOps(v:Vector) = new VectorOps(v)

  implicit def matrix2matrixOps(m:Matrix) = new MatrixOps(m)

  object Matrix {
    def apply(rows:List[AnyVal]*) =  new DenseMatrix(rows.map(_.map(_.asInstanceOf[Number].doubleValue()).toArray).toArray)
  }

}
