/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.mahout.clustering.kmeans;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.clustering.ClusterObservations;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.distance.DistanceMeasure;

public class KMeansReducer extends Reducer<Text, ClusterObservations, Text, Kluster> {

  private Map<String, Kluster> clusterMap;
  private double convergenceDelta;
  private KMeansClusterer clusterer;

  @Override
  protected void reduce(Text key, Iterable<ClusterObservations> values, Context context)
    throws IOException, InterruptedException {
    Kluster cluster = clusterMap.get(key.toString());
    for (ClusterObservations delta : values) {
      cluster.observe(delta);
    }
    // force convergence calculation
    boolean converged = clusterer.computeConvergence(cluster, convergenceDelta);
    if (converged) {
      context.getCounter("Clustering", "Converged Clusters").increment(1);
    }
    cluster.computeParameters();
    context.write(new Text(cluster.getIdentifier()), cluster);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    DistanceMeasure measure =
        ClassUtils.instantiateAs(conf.get(KMeansConfigKeys.DISTANCE_MEASURE_KEY), DistanceMeasure.class);
    measure.configure(conf);

    this.convergenceDelta = Double.parseDouble(conf.get(KMeansConfigKeys.CLUSTER_CONVERGENCE_KEY));
    this.clusterer = new KMeansClusterer(measure);
    this.clusterMap = Maps.newHashMap();

    String path = conf.get(KMeansConfigKeys.CLUSTER_PATH_KEY);
    if (!path.isEmpty()) {
      Collection<Kluster> clusters = Lists.newArrayList();
      KMeansUtil.configureWithClusterInfo(conf, new Path(path), clusters);
      setClusterMap(clusters);
      if (clusterMap.isEmpty()) {
        throw new IllegalStateException("Cluster is empty!");
      }
    }
  }

  private void setClusterMap(Collection<Kluster> clusters) {
    clusterMap = Maps.newHashMap();
    for (Kluster cluster : clusters) {
      clusterMap.put(cluster.getIdentifier(), cluster);
    }
    clusters.clear();
  }

  public void setup(Collection<Kluster> clusters, DistanceMeasure measure) {
    setClusterMap(clusters);
    this.clusterer = new KMeansClusterer(measure);
  }

}
