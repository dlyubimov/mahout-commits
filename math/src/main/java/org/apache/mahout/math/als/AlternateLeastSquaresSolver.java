/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.math.als;

import com.google.common.base.Preconditions;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.QRDecomposition;
import org.apache.mahout.math.Vector;

import java.util.Iterator;
import java.util.List;

/**
 * See <a href="http://www.hpl.hp.com/personal/Robert_Schreiber/papers/2008%20AAIM%20Netflix/netflix_aaim08(submitted).pdf">
 * this paper.</a>
 */
public class AlternateLeastSquaresSolver {

  public Vector solve(List<Vector> featureVectors, Vector ratingVector, double lambda, int numFeatures) {

    Preconditions.checkNotNull(featureVectors, "Feature vectors cannot be null");
    Preconditions.checkArgument(!featureVectors.isEmpty());
    Preconditions.checkNotNull(ratingVector, "Rating vector cannot be null");
    Preconditions.checkArgument(featureVectors.size() == ratingVector.getNumNondefaultElements());

    int nui = ratingVector.getNumNondefaultElements();

    Matrix MiIi = createMiIi(featureVectors, numFeatures);
    Matrix RiIiMaybeTransposed = createRiIiMaybeTransposed(ratingVector);

    /* compute Ai = MiIi * t(MiIi) + lambda * nui * E */
    Matrix Ai = addLambdaTimesNuiTimesE(MiIi.times(MiIi.transpose()), lambda, nui);
    /* compute Vi = MIi * t(R(i,Ii)) */
    Matrix Vi = MiIi.times(RiIiMaybeTransposed);
    /* compute ui = inverse(Ai) * Vi */
    return solve(Ai, Vi);
  }

  Vector solve(Matrix Ai, Matrix Vi) {
    return new QRDecomposition(Ai).solve(Vi).viewColumn(0);
  }

  protected Matrix addLambdaTimesNuiTimesE(Matrix matrix, double lambda, int nui) {
    Preconditions.checkArgument(matrix.numCols() == matrix.numRows());
    double lambdaTimesNui = lambda * nui;
    for (int n = 0; n < matrix.numCols(); n++) {
      matrix.setQuick(n, n, matrix.getQuick(n, n) + lambdaTimesNui);
    }
    return matrix;
  }

  protected Matrix createMiIi(List<Vector> featureVectors, int numFeatures) {
    Matrix MiIi = new DenseMatrix(numFeatures, featureVectors.size());
    for (int n = 0; n < featureVectors.size(); n++) {
      Vector featureVector = featureVectors.get(n);
      for (int m = 0; m < numFeatures; m++) {
        MiIi.setQuick(m, n, featureVector.get(m));
      }
    }
    return MiIi;
  }

  protected Matrix createRiIiMaybeTransposed(Vector ratingVector) {
    Preconditions.checkArgument(ratingVector.isSequentialAccess());
    Matrix RiIiMaybeTransposed = new DenseMatrix(ratingVector.getNumNondefaultElements(), 1);
    Iterator<Vector.Element> ratingsIterator = ratingVector.iterateNonZero();
    int index = 0;
    while (ratingsIterator.hasNext()) {
      Vector.Element elem = ratingsIterator.next();
      RiIiMaybeTransposed.setQuick(index++, 0, elem.get());
    }
    return RiIiMaybeTransposed;
  }
}
