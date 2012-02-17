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
package org.apache.mahout.clustering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.mahout.clustering.dirichlet.UncommonDistributions;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class DirichletClusteringPolicy implements ClusteringPolicy {
  
  public DirichletClusteringPolicy() {
    super();
  }

  public DirichletClusteringPolicy(int k, double alpha0) {
    this.alpha0 = alpha0;
    this.mixture = UncommonDistributions.rDirichlet(new DenseVector(k), alpha0);
  }
  
  // The mixture is the Dirichlet distribution of the total Cluster counts over
  // all iterations
  private Vector mixture;
  
  // Alpha_0 primes the Dirichlet distribution
  private double alpha0;
  
  /* (non-Javadoc)
   * @see org.apache.mahout.clustering.ClusteringPolicy#select(org.apache.mahout.math.Vector)
   */
  @Override
  public Vector select(Vector probabilities) {
    int rMultinom = UncommonDistributions.rMultinom(probabilities.times(mixture));
    Vector weights = new SequentialAccessSparseVector(probabilities.size());
    weights.set(rMultinom, 1.0);
    return weights;
  }
  
  // update the total counts and then the mixture
  /* (non-Javadoc)
   * @see org.apache.mahout.clustering.ClusteringPolicy#update(org.apache.mahout.clustering.ClusterClassifier)
   */
  @Override
  public void update(ClusterClassifier prior) {
    Vector totalCounts = new DenseVector(prior.getModels().size());
    for (int i = 0; i < prior.getModels().size(); i++) {
      totalCounts.set(i, prior.getModels().get(i).getTotalObservations());
    }
    mixture = UncommonDistributions.rDirichlet(totalCounts, alpha0);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(alpha0);
    VectorWritable.writeVector(out, mixture);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.alpha0 = in.readDouble();
    this.mixture = VectorWritable.readVector(in);
  }
}
