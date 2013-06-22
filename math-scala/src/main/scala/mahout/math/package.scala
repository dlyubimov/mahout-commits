package mahout

import org.apache.mahout.math.{DenseMatrix, Matrix, Vector}

package object math {

  implicit def vector2vectorOps(v:Vector) = new VectorOps(v)

  implicit def matrix2matrixOps(m:Matrix) = new MatrixOps(m)

  object Matrix {
    def apply[R](rows:R*) =  new DenseMatrix(rows.map(_.toArray).toArray)
  }

}
