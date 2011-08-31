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
package org.apache.mahout.math.hadoop.stochasticsvd;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.IOUtils;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.PlusMult;
import org.apache.mahout.math.hadoop.stochasticsvd.QJob.QMapper;

/**
 * Computes ABt products, then first step of QR which is pushed down to the
 * reducer.
 * 
 * @author dmitriy
 * 
 */
@SuppressWarnings("deprecation")
public class ABtJob {

  public static final String PROP_BT_PATH = "ssvd.Bt.path";

  public static class ABtMapper
      extends
      Mapper<Writable, VectorWritable, SplitPartitionedWritable, VectorWritable> {

    private SplitPartitionedWritable outKey;
    private Deque<Closeable> closeables = new ArrayDeque<Closeable>();
    private SequenceFileDirIterator<IntWritable, VectorWritable> btInput;
    private Vector[] aCols;
    // private Vector[] yiRows;
    private VectorWritable outValue = new VectorWritable();
    private int aRowCount;

    @Override
    protected void map(Writable key, VectorWritable value, Context context)
      throws IOException, InterruptedException {

      Vector vec = value.get();

      int vecSize = vec.size();
      if (aCols == null)
        aCols = new Vector[vecSize];
      else if (aCols.length < vecSize)
        aCols = Arrays.copyOf(aCols, vecSize);

      if (vec.isDense()) {
        for (int i = 0; i < vecSize; i++) {
          extendAColIfNeeded(i, aRowCount);
          aCols[i].setQuick(aRowCount, vec.getQuick(i));
        }
      } else {
        for (Iterator<Vector.Element> vecIter = vec.iterateNonZero(); vecIter
          .hasNext();) {
          Vector.Element vecEl = vecIter.next();
          int i = vecEl.index();
          extendAColIfNeeded(i, aRowCount);
          aCols[i].setQuick(aRowCount, vecEl.get());
        }
      }
      aRowCount++;
    }

    private void extendAColIfNeeded(int col, int rowCount) {
      if (aCols[col] == null)
        aCols[col] =
          new RandomAccessSparseVector(rowCount < 10000 ? 10000 : rowCount);
      else if (aCols[col].size() < aRowCount) {
        RandomAccessSparseVector newVec =
          new RandomAccessSparseVector(aRowCount << 1);
        // this doesn't have sparse implementation!
        // newVec.viewPart(0, aRowCount).assign(aCols[i]);
        for (Iterator<Vector.Element> colIter = aCols[col].iterateNonZero(); colIter
          .hasNext();) {
          Vector.Element el = colIter.next();
          newVec.set(el.index(), el.get());
        }
        aCols[col] = newVec;
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException,
      InterruptedException {
      try {
        // yiRows= new Vector[aRowCount];

        for (; btInput.hasNext();) {
          Pair<IntWritable, VectorWritable> btRec = btInput.next();
          int btIndex = btRec.getFirst().get();
          Vector btVec = btRec.getSecond().get();
          Vector aCol;
          if (btIndex > aCols.length || null == (aCol = aCols[btIndex]))
            continue;
          for (Iterator<Vector.Element> aColIter = aCol.iterateNonZero(); aColIter
            .hasNext();) {
            Vector.Element aEl = aColIter.next();
            int j = aEl.index();

            outKey.setTaskRowOrdinal(j);
            outValue.set(btVec.times(aEl.get())); // assign might work better
                                                  // with memory after all.
            context.write(outKey, outValue);
          }
        }
        aCols = null;
      } finally {
        IOUtils.close(closeables);
      }
    }

    @Override
    protected void setup(Context context) throws IOException,
      InterruptedException {

      outKey = new SplitPartitionedWritable(context);
      String propBtPathStr = context.getConfiguration().get(PROP_BT_PATH);
      Validate.notNull(propBtPathStr, "Bt input is not set");
      Path btPath = new Path(propBtPathStr);

      btInput =
        new SequenceFileDirIterator<IntWritable, VectorWritable>(btPath,
                                                                 PathType.GLOB,
                                                                 null,
                                                                 null,
                                                                 true,
                                                                 context
                                                                   .getConfiguration());
      // TODO: how do i release all that stuff??
      // closeables.addFirst(btInput);

    }

  }

  public static class ABtCombiner extends
      Reducer<SplitPartitionedWritable, VectorWritable, SplitPartitionedWritable, VectorWritable> {

    protected final VectorWritable outValue = new VectorWritable();
    protected DenseVector accum;
    protected Deque<Closeable> closeables = new ArrayDeque<Closeable>();
    protected int sparseThresholdZeroCnt;
    protected RandomAccessSparseVector sparseAccum;
    protected int accumSize;

    @Override
    protected void reduce(SplitPartitionedWritable key,
                          Iterable<VectorWritable> values,
                          Context ctx) throws IOException, InterruptedException {
      Iterator<VectorWritable> vwIter = values.iterator();

      Vector vec = vwIter.next().get();
      if (accum == null || accum.size() != vec.size()) {
        accum = new DenseVector(vec);
        accumSize = accum.size();
        sparseAccum = new RandomAccessSparseVector(accumSize);
        sparseThresholdZeroCnt =
          (int) Math.ceil(BtJob.SPARSE_ZEROS_PCT_THRESHOLD * accum.size());
        // outValue.set(accum);
      } else {
        accum.assign(vec);
      }

      while (vwIter.hasNext()) {
        accum.addAll(vwIter.next().get());
      }

      // try to detect some 0s, although not terribly likely here,
      // unless there are some items that have never been rated -- even then
      // though. Frankly, i tried to run sparse matrices as sparse as 0.05%
      // (5E-4)
      // and B' rows still never come up as sparse. If there're some items that
      // are never rated, probably it will create some B' rows that are
      // completely 0'd although i am not sure.
      boolean sparsify = false;
      int zeroCnt = 0;
      for (int i = 0; i < accumSize; i++) {
        if (accum.get(i) == 0.0 && ++zeroCnt >= sparseThresholdZeroCnt) {
          sparsify = true;
          break;
        }
      }

      if (sparsify) {
        outValue.set(sparseAccum.assign(accum));
      } else {
        outValue.set(accum);
      }
      ctx.write(key, outValue);
    }
    
    @Override
    protected void cleanup(Context context) throws IOException,
      InterruptedException {

      IOUtils.close(closeables);
    }
  }
  
  /**
   * so incoming splits are coming 
   * @author dmitriy
   *
   */
  public static class QRReducer extends Reducer<SplitPartitionedWritable, VectorWritable, SplitPartitionedWritable, VectorWritable> { 
    
  }

  public static void run(Configuration conf,
                         Path[] inputAPaths,
                         Path inputBtGlob,
                         Path outputPath,
                         int aBlockRows,
                         int minSplitSize,
                         int k,
                         int p,
                         long seed,
                         int numReduceTasks) throws ClassNotFoundException,
    InterruptedException, IOException {

    JobConf oldApiJob = new JobConf(conf);

    MultipleOutputs
      .addNamedOutput(oldApiJob,
                      QJob.OUTPUT_QHAT,
                      org.apache.hadoop.mapred.SequenceFileOutputFormat.class,
                      SplitPartitionedWritable.class,
                      DenseBlockWritable.class);

    MultipleOutputs
      .addNamedOutput(oldApiJob,
                      QJob.OUTPUT_R,
                      org.apache.hadoop.mapred.SequenceFileOutputFormat.class,
                      SplitPartitionedWritable.class,
                      VectorWritable.class);

    Job job = new Job(oldApiJob);
    job.setJobName("ABt-job");
    job.setJarByClass(ABtJob.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.setInputPaths(job, inputAPaths);
    if (minSplitSize > 0) {
      FileInputFormat.setMinInputSplitSize(job, minSplitSize);
    }

    FileOutputFormat.setOutputPath(job, outputPath);

    SequenceFileOutputFormat.setOutputCompressionType(job,
                                                      CompressionType.BLOCK);

    job.setMapOutputKeyClass(SplitPartitionedWritable.class);
    job.setMapOutputValueClass(VectorWritable.class);

    job.setOutputKeyClass(SplitPartitionedWritable.class);
    job.setOutputValueClass(VectorWritable.class);

    job.setMapperClass(ABtMapper.class);
    job.setCombinerClass(ABtCombiner.class);
    job.setReducerClass(QRReducer.class);

    job.getConfiguration().setInt(QJob.PROP_AROWBLOCK_SIZE, aBlockRows);
    job.getConfiguration().setInt(QJob.PROP_K, k);
    job.getConfiguration().setInt(QJob.PROP_P, p);
    job.getConfiguration().set(PROP_BT_PATH, inputBtGlob.toString());

    // number of reduce tasks doesn't matter. we don't actually
    // send anything to reducers.

    job.setNumReduceTasks(numReduceTasks);

    job.submit();
    job.waitForCompletion(false);

    if (!job.isSuccessful()) {
      throw new IOException("ABt job unsuccessful.");
    }

  }

}
