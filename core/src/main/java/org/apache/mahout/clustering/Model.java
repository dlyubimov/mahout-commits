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

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.VectorWritable;

/**
 * A model is a probability distribution over observed data points and allows
 * the probability of any data point to be computed. All Models have a
 * persistent representation and extend
 * WritablesampleFromPosterior(Model<VectorWritable>[])
 */
public interface Model<O> extends Writable {
  
  /**
   * Return the probability that the observation is described by this model
   * 
   * @param x
   *          an Observation from the posterior
   * @return the probability that x is in the receiver
   */
  double pdf(O x);
  
  /**
   * Observe the given observation, retaining information about it
   * 
   * @param x
   *          an Observation from the posterior
   */
  void observe(O x);
  
  /**
   * Observe the given observation, retaining information about it
   * 
   * @param x
   *          an Observation from the posterior
   * @param weight
   *          a double weighting factor
   */
  void observe(O x, double weight);

  /**
   * Compute a new set of posterior parameters based upon the Observations that
   * have been observed since my creation
   */
  void computeParameters();
  
  /**
   * Return the number of observations that have been observed by this model
   * 
   * @return an int
   */
  long count();
  
  /**
   * @return a sample of my posterior model
   */
  Model<VectorWritable> sampleFromPosterior();
  
}
