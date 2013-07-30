package org.apache.mahout.math;

import org.apache.mahout.math.function.IntIntFunction;

/**
 * Matrix View backed by an {@link IntIntFunction}
 */
class FunctionalMatrixView extends AbstractMatrix {

  /**
   * view generator function
   */
  private IntIntFunction gf;
  private boolean denseLike;

  public FunctionalMatrixView(int rows, int columns, IntIntFunction gf) {
    this(rows, columns, gf, false);
  }

  /**
   * @param gf        generator function
   * @param denseLike whether like() should create Dense or Sparse matrix.
   */
  public FunctionalMatrixView(int rows, int columns, IntIntFunction gf, boolean denseLike) {
    super(rows, columns);
    this.gf = gf;
    this.denseLike = denseLike;
  }

  @Override
  public Matrix assignColumn(int column, Vector other) {
    throw new UnsupportedOperationException("Assignment to a matrix not supported");
  }

  @Override
  public Matrix assignRow(int row, Vector other) {
    throw new UnsupportedOperationException("Assignment to a matrix view not supported");
  }

  @Override
  public double getQuick(int row, int column) {
    return gf.apply(row, column);
  }

  @Override
  public Matrix like() {
    return like(rows, columns);
  }

  @Override
  public Matrix like(int rows, int columns) {
    if (denseLike)
      return new DenseMatrix(rows, columns);
    else
      return new SparseMatrix(rows, columns);
  }

  @Override
  public void setQuick(int row, int column, double value) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix viewPart(int[] offset, int[] size) {
    throw new UnsupportedOperationException("Assignment to a matrix view not supported");
  }
}
