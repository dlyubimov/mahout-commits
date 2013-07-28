package org.apache.mahout.math;

/**
 * Optional interface for optimized matrix multiplications.
 * Some concrete Matrix implementations may mix this in.
 */
public interface MatrixTimesOps {
  /**
   * computes matrix product of (this * that)
   */
  Matrix timesRight(Matrix that);

  /**
   * Computes matrix product of (that * this)
   */
  Matrix timesLeft(Matrix that);

}
