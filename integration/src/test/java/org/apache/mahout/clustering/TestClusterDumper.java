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
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.dirichlet.DirichletDriver;
import org.apache.mahout.clustering.dirichlet.models.DistanceMeasureClusterDistribution;
import org.apache.mahout.clustering.dirichlet.models.DistributionDescription;
import org.apache.mahout.clustering.dirichlet.models.GaussianClusterDistribution;
import org.apache.mahout.clustering.fuzzykmeans.FuzzyKMeansDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.meanshift.MeanShiftCanopyDriver;
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileValueIterable;
import org.apache.mahout.common.kernel.IKernelProfile;
import org.apache.mahout.common.kernel.TriangularKernelProfile;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.hadoop.decomposer.DistributedLanczosSolver;
import org.apache.mahout.math.hadoop.decomposer.EigenVerificationJob;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.apache.mahout.utils.vectors.TermEntry;
import org.apache.mahout.utils.vectors.TermInfo;
import org.apache.mahout.utils.vectors.lucene.CachedTermInfo;
import org.apache.mahout.utils.vectors.lucene.LuceneIterable;
import org.apache.mahout.utils.vectors.lucene.TFDFMapper;
import org.apache.mahout.utils.vectors.lucene.VectorMapper;
import org.apache.mahout.vectorizer.TFIDF;
import org.apache.mahout.vectorizer.Weight;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Closeables;

public final class TestClusterDumper extends MahoutTestCase {
  
  private static final String[] DOCS = {
      "The quick red fox jumped over the lazy brown dogs.",
      "The quick brown fox jumped over the lazy red dogs.",
      "The quick red cat jumped over the lazy brown dogs.",
      "The quick brown cat jumped over the lazy red dogs.",
      "Mary had a little lamb whose fleece was white as snow.",
      "Mary had a little goat whose fleece was white as snow.",
      "Mary had a little lamb whose fleece was black as tar.",
      "Dick had a little goat whose fleece was white as snow.",
      "Moby Dick is a story of a whale and a man obsessed.",
      "Moby Bob is a story of a walrus and a man obsessed.",
      "Moby Dick is a story of a whale and a crazy man.",
      "The robber wore a black fleece jacket and a baseball cap.",
      "The robber wore a red fleece jacket and a baseball cap.",
      "The robber wore a white fleece jacket and a baseball cap.",
      "The English Springer Spaniel is the best of all dogs."};
  
  private List<VectorWritable> sampleData;
  
  private String[] termDictionary;
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    // Create test data
    getSampleData(DOCS);
    ClusteringTestUtils.writePointsToFile(sampleData, true,
        getTestTempFilePath("testdata/file1"), fs, conf);
  }
  
  private void getSampleData(String[] docs2) throws IOException {
    sampleData = Lists.newArrayList();
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(
        Version.LUCENE_31), true, IndexWriter.MaxFieldLength.UNLIMITED);
    try {
      for (int i = 0; i < docs2.length; i++) {
        Document doc = new Document();
        Fieldable id = new Field("id", "doc_" + i, Field.Store.YES,
            Field.Index.NOT_ANALYZED_NO_NORMS);
        doc.add(id);
        // Store both position and offset information
        Fieldable text = new Field("content", docs2[i], Field.Store.NO,
            Field.Index.ANALYZED, Field.TermVector.YES);
        doc.add(text);
        writer.addDocument(doc);
      }
    } finally {
      Closeables.closeQuietly(writer);
    }
    IndexReader reader = IndexReader.open(directory, true);
    Weight weight = new TFIDF();
    TermInfo termInfo = new CachedTermInfo(reader, "content", 1, 100);
    
    int numTerms = 0;
    for (Iterator<TermEntry> it = termInfo.getAllEntries(); it.hasNext();) {
      it.next();
      numTerms++;
    }
    termDictionary = new String[numTerms];
    int i = 0;
    for (Iterator<TermEntry> it = termInfo.getAllEntries(); it.hasNext();) {
      String term = it.next().getTerm();
      termDictionary[i] = term;
      System.out.println(i + " " + term);
      i++;
    }
    VectorMapper mapper = new TFDFMapper(reader, weight, termInfo);
    Iterable<Vector> iterable = new LuceneIterable(reader, "id", "content",
        mapper);
    
    i = 0;
    for (Vector vector : iterable) {
      assertNotNull(vector);
      NamedVector namedVector;
      if (vector instanceof NamedVector) {
        // rename it for testing purposes
        namedVector = new NamedVector(((NamedVector) vector).getDelegate(),
            "P(" + i + ')');
        
      } else {
        namedVector = new NamedVector(vector, "P(" + i + ')');
      }
      System.out.println(AbstractCluster.formatVector(namedVector,
          termDictionary));
      sampleData.add(new VectorWritable(namedVector));
      i++;
    }
  }
  
  /**
   * Return the path to the final iteration's clusters
   */
  private static Path finalClusterPath(Configuration conf, Path output,
      int maxIterations) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    for (int i = maxIterations; i >= 0; i--) {
      Path clusters = new Path(output, "clusters-" + i);
      if (fs.exists(clusters)) {
        return clusters;
      }
    }
    return null;
  }
  
  @Test
  public void testCanopy() throws Exception { // now run the Job
    DistanceMeasure measure = new EuclideanDistanceMeasure();
    
    Path output = getTestTempDirPath("output");
    CanopyDriver.run(new Configuration(), getTestTempDirPath("testdata"),
        output, measure, 8, 4, true, true);
    // run ClusterDumper
    ClusterDumper clusterDumper = new ClusterDumper(new Path(output,
        "clusters-0"), new Path(output, "clusteredPoints"));
    clusterDumper.printClusters(termDictionary);
    assertTrue(true);
  }
  
  @Test
  public void testKmeans() throws Exception {
    DistanceMeasure measure = new EuclideanDistanceMeasure();
    // now run the Canopy job to prime kMeans canopies
    Path output = getTestTempDirPath("output");
    Configuration conf = new Configuration();
    CanopyDriver.run(conf, getTestTempDirPath("testdata"), output, measure, 8,
        4, false, true);
    // now run the KMeans job
    KMeansDriver.run(conf, getTestTempDirPath("testdata"), new Path(output,
        "clusters-0"), output, measure, 0.001, 10, true, false);
    // run ClusterDumper
    ClusterDumper clusterDumper = new ClusterDumper(finalClusterPath(conf,
        output, 10), new Path(output, "clusteredPoints"));
    clusterDumper.printClusters(termDictionary);
  }
  
  @Test
  public void testFuzzyKmeans() throws Exception {
    DistanceMeasure measure = new EuclideanDistanceMeasure();
    // now run the Canopy job to prime kMeans canopies
    Path output = getTestTempDirPath("output");
    Configuration conf = new Configuration();
    CanopyDriver.run(conf, getTestTempDirPath("testdata"), output, measure, 8,
        4, false, true);
    // now run the Fuzzy KMeans job
    FuzzyKMeansDriver.run(conf, getTestTempDirPath("testdata"), new Path(
        output, "clusters-0"), output, measure, 0.001, 10, ((float) 1.1), true,
        true, 0, true);
    // run ClusterDumper
    ClusterDumper clusterDumper = new ClusterDumper(finalClusterPath(conf,
        output, 10), new Path(output, "clusteredPoints"));
    clusterDumper.printClusters(termDictionary);
  }
  
  @Test
  public void testMeanShift() throws Exception {
    DistanceMeasure measure = new CosineDistanceMeasure();
    IKernelProfile kernelProfile = new TriangularKernelProfile();
    Path output = getTestTempDirPath("output");
    Configuration conf = new Configuration();
    MeanShiftCanopyDriver.run(conf, getTestTempDirPath("testdata"), output,
        measure, kernelProfile, 0.5, 0.01, 0.05, 10, false, true, true);
    // run ClusterDumper
    ClusterDumper clusterDumper = new ClusterDumper(finalClusterPath(conf,
        output, 10), new Path(output, "clusteredPoints"));
    clusterDumper.printClusters(termDictionary);
  }
  
  @Test
  public void testDirichlet2() throws Exception {
    Path output = getTestTempDirPath("output");
    NamedVector prototype = (NamedVector) sampleData.get(0).get();
    DistributionDescription description = new DistributionDescription(
        GaussianClusterDistribution.class.getName(),
        RandomAccessSparseVector.class.getName(), null, prototype.getDelegate()
            .size());
    Configuration conf = new Configuration();
    DirichletDriver.run(conf, getTestTempDirPath("testdata"), output,
        description, 15, 10, 1.0, true, true, 0, true);
    // run ClusterDumper
    ClusterDumper clusterDumper = new ClusterDumper(finalClusterPath(conf,
        output, 10), new Path(output, "clusteredPoints"));
    clusterDumper.printClusters(termDictionary);
  }
  
  @Test
  public void testDirichlet3() throws Exception {
    Path output = getTestTempDirPath("output");
    NamedVector prototype = (NamedVector) sampleData.get(0).get();
    DistributionDescription description = new DistributionDescription(
        DistanceMeasureClusterDistribution.class.getName(),
        RandomAccessSparseVector.class.getName(),
        ManhattanDistanceMeasure.class.getName(), prototype.getDelegate()
            .size());
    Configuration conf = new Configuration();
    DirichletDriver.run(conf, getTestTempDirPath("testdata"), output,
        description, 15, 10, 1.0, true, true, 0, true);
    // run ClusterDumper
    ClusterDumper clusterDumper = new ClusterDumper(finalClusterPath(conf,
        output, 10), new Path(output, "clusteredPoints"));
    clusterDumper.printClusters(termDictionary);
  }
  
  // @Test
  public void testKmeansSVD() throws Exception {
    DistanceMeasure measure = new EuclideanDistanceMeasure();
    Path output = getTestTempDirPath("output");
    Path tmp = getTestTempDirPath("tmp");
    DistributedLanczosSolver solver = new DistributedLanczosSolver();
    Configuration conf = new Configuration();
    solver.setConf(conf);
    Path testData = getTestTempDirPath("testdata");
    int sampleDimension = sampleData.get(0).get().size();
    int desiredRank = 15;
    solver.run(testData, output, tmp, null, sampleData.size(), sampleDimension,
        false, desiredRank, 0.5, 0.0, true);
    Path cleanEigenvectors = new Path(output,
        EigenVerificationJob.CLEAN_EIGENVECTORS);
    
    // build in-memory data matrix A
    Matrix a = new DenseMatrix(sampleData.size(), sampleDimension);
    int i = 0;
    for (VectorWritable vw : sampleData) {
      a.assignRow(i++, vw.get());
    }
    // extract the eigenvectors into P
    Matrix p = new DenseMatrix(39, desiredRank - 1);
    FileSystem fs = FileSystem.get(cleanEigenvectors.toUri(), conf);
    
    i = 0;
    for (VectorWritable value : new SequenceFileValueIterable<VectorWritable>(
        cleanEigenvectors, conf)) {
      Vector v = value.get();
      p.assignColumn(i, v);
      i++;
    }
    // sData = A P
    Matrix sData = a.times(p);
    
    // now write sData back to file system so clustering can run against it
    Path svdData = new Path(output, "svddata");
    SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, svdData,
        IntWritable.class, VectorWritable.class);
    try {
      IntWritable key = new IntWritable();
      VectorWritable value = new VectorWritable();
      
      for (int row = 0; row < sData.numRows(); row++) {
        key.set(row);
        value.set(sData.viewRow(row));
        writer.append(key, value);
      }
    } finally {
      Closeables.closeQuietly(writer);
    }
    // now run the Canopy job to prime kMeans canopies
    CanopyDriver.run(conf, svdData, output, measure, 8, 4, false, true);
    // now run the KMeans job
    KMeansDriver.run(svdData, new Path(output, "clusters-0"), output, measure,
        0.001, 10, true, true);
    // run ClusterDumper
    ClusterDumper clusterDumper = new ClusterDumper(finalClusterPath(conf,
        output, 10), new Path(output, "clusteredPoints"));
    clusterDumper.printClusters(termDictionary);
  }
  
  // @Test
  public void testKmeansDSVD() throws Exception {
    DistanceMeasure measure = new EuclideanDistanceMeasure();
    Path output = getTestTempDirPath("output");
    Path tmp = getTestTempDirPath("tmp");
    DistributedLanczosSolver solver = new DistributedLanczosSolver();
    Configuration config = new Configuration();
    solver.setConf(config);
    Path testData = getTestTempDirPath("testdata");
    int sampleDimension = sampleData.get(0).get().size();
    // Run EigenVerificationJob from within DistributedLanczosSolver.run(...)
    int desiredRank = 13;
    solver.run(testData, output, tmp, null, sampleData.size(), sampleDimension,
        false, desiredRank, 0.5, 0.0, false);
    
    Path cleanEigenvectors = new Path(output,
        EigenVerificationJob.CLEAN_EIGENVECTORS);
    
    // now multiply the testdata matrix and the eigenvector matrix
    DistributedRowMatrix svdT = new DistributedRowMatrix(cleanEigenvectors,
        tmp, desiredRank, sampleDimension);
    Configuration conf = new Configuration(config);
    svdT.setConf(conf);
    DistributedRowMatrix a = new DistributedRowMatrix(testData, tmp,
        sampleData.size(), sampleDimension);
    a.setConf(conf);
    DistributedRowMatrix sData = a.transpose().times(svdT.transpose());
    sData.setConf(conf);
    
    // now run the Canopy job to prime kMeans canopies
    CanopyDriver.run(conf, sData.getRowPath(), output, measure, 8, 4, false,
        true);
    // now run the KMeans job
    KMeansDriver.run(sData.getRowPath(), new Path(output, "clusters-0"),
        output, measure, 0.001, 10, true, true);
    // run ClusterDumper
    ClusterDumper clusterDumper = new ClusterDumper(finalClusterPath(conf,
        output, 10), new Path(output, "clusteredPoints"));
    clusterDumper.printClusters(termDictionary);
  }
  
  // @Test
  public void testKmeansDSVD2() throws Exception {
    DistanceMeasure measure = new EuclideanDistanceMeasure();
    Path output = getTestTempDirPath("output");
    Path tmp = getTestTempDirPath("tmp");
    DistributedLanczosSolver solver = new DistributedLanczosSolver();
    Configuration config = new Configuration();
    solver.setConf(config);
    Path testData = getTestTempDirPath("testdata");
    int sampleDimension = sampleData.get(0).get().size();
    // call EigenVerificationJob separately
    int desiredRank = 13;
    solver.run(testData, output, tmp, null, sampleData.size(), sampleDimension,
        false, desiredRank);
    Path rawEigenvectors = new Path(output,
        DistributedLanczosSolver.RAW_EIGENVECTORS);
    Configuration conf = new Configuration(config);
    new EigenVerificationJob().run(testData, rawEigenvectors, output, tmp, 0.5,
        0.0, true, conf);
    Path cleanEigenvectors = new Path(output,
        EigenVerificationJob.CLEAN_EIGENVECTORS);
    
    // now multiply the testdata matrix and the eigenvector matrix
    DistributedRowMatrix svdT = new DistributedRowMatrix(cleanEigenvectors,
        tmp, desiredRank, sampleDimension);
    svdT.setConf(conf);
    DistributedRowMatrix a = new DistributedRowMatrix(testData, tmp,
        sampleData.size(), sampleDimension);
    a.setConf(conf);
    DistributedRowMatrix sData = a.transpose().times(svdT.transpose());
    sData.setConf(conf);
    
    // now run the Canopy job to prime kMeans canopies
    CanopyDriver.run(conf, sData.getRowPath(), output, measure, 8, 4, false,
        true);
    // now run the KMeans job
    KMeansDriver.run(sData.getRowPath(), new Path(output, "clusters-0"),
        output, measure, 0.001, 10, true, true);
    // run ClusterDumper
    ClusterDumper clusterDumper = new ClusterDumper(finalClusterPath(conf,
        output, 10), new Path(output, "clusteredPoints"));
    clusterDumper.printClusters(termDictionary);
  }
}
