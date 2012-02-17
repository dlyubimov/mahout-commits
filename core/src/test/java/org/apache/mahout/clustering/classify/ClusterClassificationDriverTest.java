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

package org.apache.mahout.clustering.classify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.ClusteringTestUtils;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ClusterClassificationDriverTest extends MahoutTestCase{
  
  private static final double[][] REFERENCE = { {1, 1}, {2, 1}, {1, 2}, {4, 4}, {5, 4}, {4, 5}, {5, 5}, {9, 9}, {8, 8}};
  
  private FileSystem fs;
  
  private Path clusteringOutputPath;
  
  private Configuration conf;

  private Path pointsPath;

  private Path classifiedOutputPath;

  private List<Vector> firstCluster;
  
  private List<Vector> secondCluster;
  
  private List<Vector> thirdCluster;
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    Configuration conf = new Configuration();
    fs = FileSystem.get(conf);
    firstCluster = new ArrayList<Vector>();
    secondCluster = new ArrayList<Vector>();
    thirdCluster = new ArrayList<Vector>();
    
  }
  
  private static List<VectorWritable> getPointsWritable(double[][] raw) {
    List<VectorWritable> points = Lists.newArrayList();
    for (double[] fr : raw) {
      Vector vec = new RandomAccessSparseVector(fr.length);
      vec.assign(fr);
      points.add(new VectorWritable(vec));
    }
    return points;
  }
  
  @Test
  public void testVectorClassificationWithoutOutlierRemoval() throws Exception {
    List<VectorWritable> points = getPointsWritable(REFERENCE);
    
    pointsPath = getTestTempDirPath("points");
    clusteringOutputPath = getTestTempDirPath("output");
    classifiedOutputPath = getTestTempDirPath("classify");

    conf = new Configuration();
    
    ClusteringTestUtils.writePointsToFile(points, new Path(pointsPath, "file1"), fs, conf);
    runClustering(pointsPath, conf);
    runClassificationWithoutOutlierRemoval(conf);
    collectVectorsForAssertion();
    assertVectorsWithoutOutlierRemoval();
  }
  
  @Test
  public void testVectorClassificationWithOutlierRemoval() throws Exception {
    List<VectorWritable> points = getPointsWritable(REFERENCE);
    
    pointsPath = getTestTempDirPath("points");
    clusteringOutputPath = getTestTempDirPath("output");
    classifiedOutputPath = getTestTempDirPath("classify");

    conf = new Configuration();
    
    ClusteringTestUtils.writePointsToFile(points, new Path(pointsPath, "file1"), fs, conf);
    runClustering(pointsPath, conf);
    runClassificationWithOutlierRemoval(conf);
    collectVectorsForAssertion();
    assertVectorsWithOutlierRemoval();
  }
  
  private void runClustering(Path pointsPath, Configuration conf) throws IOException,
  InterruptedException,
  ClassNotFoundException {
    CanopyDriver.run(conf, pointsPath, clusteringOutputPath, new ManhattanDistanceMeasure(), 3.1, 2.1, false, true);
  }
  
  private void runClassificationWithoutOutlierRemoval(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
    ClusterClassificationDriver.run(pointsPath, clusteringOutputPath, classifiedOutputPath, 0.0, true);
  }
  
  private void runClassificationWithOutlierRemoval(Configuration conf2) throws IOException, InterruptedException, ClassNotFoundException {
    ClusterClassificationDriver.run(pointsPath, clusteringOutputPath, classifiedOutputPath, 0.73, true);
  }

  private void collectVectorsForAssertion() throws IOException {
    Path[] partFilePaths = FileUtil.stat2Paths(fs.globStatus(classifiedOutputPath));
    FileStatus[] listStatus = fs.listStatus(partFilePaths);
    for (FileStatus partFile : listStatus) {
      SequenceFile.Reader classifiedVectors = new SequenceFile.Reader(fs, partFile.getPath(), conf);
      Writable clusterIdAsKey = new IntWritable();
      VectorWritable point = new VectorWritable();
      while (classifiedVectors.next(clusterIdAsKey, point)) {
        collectVector(clusterIdAsKey.toString(), point.get());
      }
    }
  }
  
  private void collectVector(String clusterId, Vector vector) {
    if(clusterId.equals("0")) {
      firstCluster.add(vector);
    }
    else if(clusterId.equals("1")) {
      secondCluster.add(vector);
    }
    else if(clusterId.equals("2")) {
      thirdCluster.add(vector);
    }
  }
  
  private void assertVectorsWithOutlierRemoval() {
    assertFirstClusterWithOutlierRemoval();
    assertSecondClusterWithOutlierRemoval();
    assertThirdClusterWithOutlierRemoval();
  }

  private void assertVectorsWithoutOutlierRemoval() {
    assertFirstClusterWithoutOutlierRemoval();
    assertSecondClusterWithoutOutlierRemoval();
    assertThirdClusterWithoutOutlierRemoval();
  }

  private void assertThirdClusterWithoutOutlierRemoval() {
    Assert.assertEquals(2, thirdCluster.size());
    for (Vector vector : thirdCluster) {
      Assert.assertTrue(ArrayUtils.contains(new String[] {"{1:9.0,0:9.0}", "{1:8.0,0:8.0}"}, vector.asFormatString()));
    }
  }

  private void assertSecondClusterWithoutOutlierRemoval() {
    Assert.assertEquals(4, secondCluster.size());
    for (Vector vector : secondCluster) {
    Assert.assertTrue(ArrayUtils.contains(new String[] {"{1:4.0,0:4.0}", "{1:4.0,0:5.0}", "{1:5.0,0:4.0}",
    "{1:5.0,0:5.0}"}, vector.asFormatString()));
    }
  }

  private void assertFirstClusterWithoutOutlierRemoval() {
    Assert.assertEquals(3, firstCluster.size());
    for (Vector vector : firstCluster) {
      Assert.assertTrue(ArrayUtils.contains(new String[] {"{1:1.0,0:1.0}","{1:1.0,0:2.0}", "{1:2.0,0:1.0}"}, vector.asFormatString()));
    }
  }
  

  private void assertThirdClusterWithOutlierRemoval() {
    Assert.assertEquals(1, thirdCluster.size());
    for (Vector vector : thirdCluster) {
      Assert.assertTrue(ArrayUtils.contains(new String[] {"{1:9.0,0:9.0}"}, vector.asFormatString()));
    }
  }

  private void assertSecondClusterWithOutlierRemoval() {
    Assert.assertEquals(0, secondCluster.size());
  }

  private void assertFirstClusterWithOutlierRemoval() {
    Assert.assertEquals(1, firstCluster.size());
    for (Vector vector : firstCluster) {
      Assert.assertTrue(ArrayUtils.contains(new String[] {"{1:1.0,0:1.0}"}, vector.asFormatString()));
    }
  }

  
}
