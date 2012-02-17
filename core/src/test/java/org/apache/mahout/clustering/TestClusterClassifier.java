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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.canopy.Canopy;
import org.apache.mahout.clustering.dirichlet.models.GaussianCluster;
import org.apache.mahout.clustering.fuzzykmeans.SoftCluster;
import org.apache.mahout.clustering.kmeans.TestKmeansClustering;
import org.apache.mahout.clustering.meanshift.MeanShiftCanopy;
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Test;

import com.google.common.collect.Lists;

public final class TestClusterClassifier extends MahoutTestCase {
  
  private static ClusterClassifier newDMClassifier() {
    List<Cluster> models = Lists.newArrayList();
    DistanceMeasure measure = new ManhattanDistanceMeasure();
    models.add(new DistanceMeasureCluster(new DenseVector(2).assign(1), 0, measure));
    models.add(new DistanceMeasureCluster(new DenseVector(2), 1, measure));
    models.add(new DistanceMeasureCluster(new DenseVector(2).assign(-1), 2, measure));
    return new ClusterClassifier(models);
  }
  
  private static ClusterClassifier newClusterClassifier() {
    List<Cluster> models = Lists.newArrayList();
    DistanceMeasure measure = new ManhattanDistanceMeasure();
    models.add(new org.apache.mahout.clustering.kmeans.Kluster(new DenseVector(2).assign(1), 0, measure));
    models.add(new org.apache.mahout.clustering.kmeans.Kluster(new DenseVector(2), 1, measure));
    models.add(new org.apache.mahout.clustering.kmeans.Kluster(new DenseVector(2).assign(-1), 2, measure));
    return new ClusterClassifier(models);
  }
  
  private static ClusterClassifier newSoftClusterClassifier() {
    List<Cluster> models = Lists.newArrayList();
    DistanceMeasure measure = new ManhattanDistanceMeasure();
    models.add(new SoftCluster(new DenseVector(2).assign(1), 0, measure));
    models.add(new SoftCluster(new DenseVector(2), 1, measure));
    models.add(new SoftCluster(new DenseVector(2).assign(-1), 2, measure));
    return new ClusterClassifier(models);
  }
  
  private static ClusterClassifier newGaussianClassifier() {
    List<Cluster> models = Lists.newArrayList();
    models.add(new GaussianCluster(new DenseVector(2).assign(1), new DenseVector(2).assign(1), 0));
    models.add(new GaussianCluster(new DenseVector(2), new DenseVector(2).assign(1), 1));
    models.add(new GaussianCluster(new DenseVector(2).assign(-1), new DenseVector(2).assign(1), 2));
    return new ClusterClassifier(models);
  }
  
  private ClusterClassifier writeAndRead(ClusterClassifier classifier) throws IOException {
    Path path = new Path(getTestTempDirPath(), "output");
    ClusterIterator.writeClassifier(classifier, path);
    return ClusterIterator.readClassifier(path);
  }
  
  @Test
  public void testDMClusterClassification() {
    ClusterClassifier classifier = newDMClassifier();
    Vector pdf = classifier.classify(new DenseVector(2));
    assertEquals("[0,0]", "[0.200, 0.600, 0.200]", AbstractCluster.formatVector(pdf, null));
    pdf = classifier.classify(new DenseVector(2).assign(2));
    assertEquals("[2,2]", "[0.493, 0.296, 0.211]", AbstractCluster.formatVector(pdf, null));
  }
  
  @Test
  public void testCanopyClassification() {
    List<Cluster> models = Lists.newArrayList();
    DistanceMeasure measure = new ManhattanDistanceMeasure();
    models.add(new Canopy(new DenseVector(2).assign(1), 0, measure));
    models.add(new Canopy(new DenseVector(2), 1, measure));
    models.add(new Canopy(new DenseVector(2).assign(-1), 2, measure));
    ClusterClassifier classifier = new ClusterClassifier(models);
    Vector pdf = classifier.classify(new DenseVector(2));
    assertEquals("[0,0]", "[0.200, 0.600, 0.200]", AbstractCluster.formatVector(pdf, null));
    pdf = classifier.classify(new DenseVector(2).assign(2));
    assertEquals("[2,2]", "[0.493, 0.296, 0.211]", AbstractCluster.formatVector(pdf, null));
  }
  
  @Test
  public void testClusterClassification() {
    ClusterClassifier classifier = newClusterClassifier();
    Vector pdf = classifier.classify(new DenseVector(2));
    assertEquals("[0,0]", "[0.200, 0.600, 0.200]", AbstractCluster.formatVector(pdf, null));
    pdf = classifier.classify(new DenseVector(2).assign(2));
    assertEquals("[2,2]", "[0.493, 0.296, 0.211]", AbstractCluster.formatVector(pdf, null));
  }
  
  @Test(expected = UnsupportedOperationException.class)
  public void testMSCanopyClassification() {
    List<Cluster> models = Lists.newArrayList();
    DistanceMeasure measure = new ManhattanDistanceMeasure();
    models.add(new MeanShiftCanopy(new DenseVector(2).assign(1), 0, measure));
    models.add(new MeanShiftCanopy(new DenseVector(2), 1, measure));
    models.add(new MeanShiftCanopy(new DenseVector(2).assign(-1), 2, measure));
    ClusterClassifier classifier = new ClusterClassifier(models);
    classifier.classify(new DenseVector(2));
  }
  
  @Test
  public void testSoftClusterClassification() {
    ClusterClassifier classifier = newSoftClusterClassifier();
    Vector pdf = classifier.classify(new DenseVector(2));
    assertEquals("[0,0]", "[0.000, 1.000, 0.000]", AbstractCluster.formatVector(pdf, null));
    pdf = classifier.classify(new DenseVector(2).assign(2));
    assertEquals("[2,2]", "[0.735, 0.184, 0.082]", AbstractCluster.formatVector(pdf, null));
  }
  
  @Test
  public void testGaussianClusterClassification() {
    ClusterClassifier classifier = newGaussianClassifier();
    Vector pdf = classifier.classify(new DenseVector(2));
    assertEquals("[0,0]", "[0.212, 0.576, 0.212]", AbstractCluster.formatVector(pdf, null));
    pdf = classifier.classify(new DenseVector(2).assign(2));
    assertEquals("[2,2]", "[0.952, 0.047, 0.000]", AbstractCluster.formatVector(pdf, null));
  }
  
  @Test
  public void testDMClassifierSerialization() throws Exception {
    ClusterClassifier classifier = newDMClassifier();
    ClusterClassifier classifierOut = writeAndRead(classifier);
    assertEquals(classifier.getModels().size(), classifierOut.getModels().size());
    assertEquals(classifier.getModels().get(0).getClass().getName(), classifierOut.getModels().get(0).getClass()
        .getName());
  }
  
  @Test
  public void testClusterClassifierSerialization() throws Exception {
    ClusterClassifier classifier = newClusterClassifier();
    ClusterClassifier classifierOut = writeAndRead(classifier);
    assertEquals(classifier.getModels().size(), classifierOut.getModels().size());
    assertEquals(classifier.getModels().get(0).getClass().getName(), classifierOut.getModels().get(0).getClass()
        .getName());
  }
  
  @Test
  public void testSoftClusterClassifierSerialization() throws Exception {
    ClusterClassifier classifier = newSoftClusterClassifier();
    ClusterClassifier classifierOut = writeAndRead(classifier);
    assertEquals(classifier.getModels().size(), classifierOut.getModels().size());
    assertEquals(classifier.getModels().get(0).getClass().getName(), classifierOut.getModels().get(0).getClass()
        .getName());
  }
  
  @Test
  public void testGaussianClassifierSerialization() throws Exception {
    ClusterClassifier classifier = newGaussianClassifier();
    ClusterClassifier classifierOut = writeAndRead(classifier);
    assertEquals(classifier.getModels().size(), classifierOut.getModels().size());
    assertEquals(classifier.getModels().get(0).getClass().getName(), classifierOut.getModels().get(0).getClass()
        .getName());
  }
  
  @Test
  public void testClusterIteratorKMeans() {
    List<Vector> data = TestKmeansClustering.getPoints(TestKmeansClustering.REFERENCE);
    ClusteringPolicy policy = new KMeansClusteringPolicy();
    ClusterClassifier prior = newClusterClassifier();
    ClusterIterator iterator = new ClusterIterator(policy);
    ClusterClassifier posterior = iterator.iterate(data, prior, 5);
    assertEquals(3, posterior.getModels().size());
    for (Cluster cluster : posterior.getModels()) {
      System.out.println(cluster.asFormatString(null));
    }
  }
  
  @Test
  public void testClusterIteratorDirichlet() {
    List<Vector> data = TestKmeansClustering.getPoints(TestKmeansClustering.REFERENCE);
    ClusteringPolicy policy = new DirichletClusteringPolicy(3, 1);
    ClusterClassifier prior = newClusterClassifier();
    ClusterIterator iterator = new ClusterIterator(policy);
    ClusterClassifier posterior = iterator.iterate(data, prior, 5);
    assertEquals(3, posterior.getModels().size());
    for (Cluster cluster : posterior.getModels()) {
      System.out.println(cluster.asFormatString(null));
    }
  }
  
  @Test
  public void testSeqFileClusterIteratorKMeans() throws IOException {
    Path pointsPath = getTestTempDirPath("points");
    Path priorPath = getTestTempDirPath("prior");
    Path outPath = getTestTempDirPath("output");
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(pointsPath.toUri(), conf);
    List<VectorWritable> points = TestKmeansClustering.getPointsWritable(TestKmeansClustering.REFERENCE);
    ClusteringTestUtils.writePointsToFile(points, new Path(pointsPath, "file1"), fs, conf);
    Path path = new Path(priorPath, "priorClassifier");
    ClusterClassifier prior = newClusterClassifier();
    ClusterIterator.writeClassifier(prior, path);
    assertEquals(3, prior.getModels().size());
    System.out.println("Prior");
    for (Cluster cluster : prior.getModels()) {
      System.out.println(cluster.asFormatString(null));
    }
    ClusteringPolicy policy = new KMeansClusteringPolicy();
    ClusterIterator iterator = new ClusterIterator(policy);
    iterator.iterateSeq(pointsPath, path, outPath, 5);
    
    for (int i = 1; i <= 5; i++) {
      System.out.println("Classifier-" + i);
      ClusterClassifier posterior = ClusterIterator.readClassifier(new Path(outPath, "classifier-" + i));
      assertEquals(3, posterior.getModels().size());
      for (Cluster cluster : posterior.getModels()) {
        System.out.println(cluster.asFormatString(null));
      }
      
    }
  }
  
  @Test
  public void testMRFileClusterIteratorKMeans() throws IOException, InterruptedException, ClassNotFoundException {
    Path pointsPath = getTestTempDirPath("points");
    Path priorPath = getTestTempDirPath("prior");
    Path outPath = getTestTempDirPath("output");
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(pointsPath.toUri(), conf);
    List<VectorWritable> points = TestKmeansClustering.getPointsWritable(TestKmeansClustering.REFERENCE);
    ClusteringTestUtils.writePointsToFile(points, new Path(pointsPath, "file1"), fs, conf);
    Path path = new Path(priorPath, "priorClassifier");
    ClusterClassifier prior = newClusterClassifier();
    ClusterIterator.writeClassifier(prior, path);
    assertEquals(3, prior.getModels().size());
    System.out.println("Prior");
    for (Cluster cluster : prior.getModels()) {
      System.out.println(cluster.asFormatString(null));
    }
    ClusteringPolicy policy = new KMeansClusteringPolicy();
    ClusterIterator iterator = new ClusterIterator(policy);
    iterator.iterateMR(pointsPath, path, outPath, 5);
    
    for (int i = 1; i <= 5; i++) {
      System.out.println("Classifier-" + i);
      ClusterClassifier posterior = ClusterIterator.readClassifier(new Path(outPath, "clusters-" + i));
      assertEquals(3, posterior.getModels().size());
      for (Cluster cluster : posterior.getModels()) {
        System.out.println(cluster.asFormatString(null));
      }
      
    }
  }
}
