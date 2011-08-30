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
import java.util.List;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.IOUtils;
import org.apache.mahout.common.iterator.CopyConstructorIterator;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileValueIterator;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

/**
 * Bt job. For details, see working notes in MAHOUT-376.
 * <P>
 * 
 * Uses hadoop deprecated API wherever new api has not been updated
 * (MAHOUT-593), hence @SuppressWarning("deprecation").
 * <P>
 * 
 * This job outputs either Bt in its standard output, or upper triangular
 * matrices representing BBt partial sums if that's requested . If the latter
 * mode is enabled, then we accumulate BBt outer product sums in upper
 * triangular accumulator and output it at the end of the job, thus saving space
 * and BBt job.
 * <P>
 * 
 * This job also outputs Q and Bt and optionally BBt. Bt is output to standard
 * job output (part-*) and Q and BBt use named multiple outputs.
 * 
 * <P>
 * 
 */
@SuppressWarnings("deprecation")
public final class BtJob {

  public static final String OUTPUT_Q = "Q";
  public static final String OUTPUT_BT = "part";
  public static final String OUTPUT_BBT = "bbt";
  public static final String PROP_QJOB_PATH = "ssvd.QJob.path";
  public static final String PROP_OUPTUT_BBT_PRODUCTS =
    "ssvd.BtJob.outputBBtProducts";

  private BtJob() {
  }

  public static class BtMapper extends
      Mapper<Writable, VectorWritable, IntWritable, VectorWritable> {

    private SequenceFile.Reader qInput;
    private final List<UpperTriangular> mRs = Lists.newArrayList();
    private int blockNum;
    private double[][] mQt;
    private int cnt;
    private int r;
    private MultipleOutputs outputs;
    private final IntWritable btKey = new IntWritable();
    private final VectorWritable btValue = new VectorWritable();
    private int kp;
    private final VectorWritable qRowValue = new VectorWritable();

    // private int qCount; // debug

    void loadNextQt() throws IOException {
      Writable key = new SplitPartitionedWritable();
      DenseBlockWritable v = new DenseBlockWritable();

      boolean more = qInput.next(key, v);
      assert more;

      mQt =
        GivensThinSolver
          .computeQtHat(v.getBlock(),
                        blockNum == 0 ? 0 : 1,
                        new CopyConstructorIterator<UpperTriangular>(mRs
                          .iterator()));
      r = mQt[0].length;
      kp = mQt.length;
      if (btValue.get() == null) {
        btValue.set(new DenseVector(kp));
      }
      if (qRowValue.get() == null) {
        qRowValue.set(new DenseVector(kp));
      }

    }

    @Override
    protected void cleanup(Context context) throws IOException,
      InterruptedException {
      Closeables.closeQuietly(qInput);
      if (outputs != null) {
        outputs.close();
      }
      super.cleanup(context);
    }

    @SuppressWarnings("unchecked")
    private void outputQRow(Writable key, Writable value) throws IOException {
      outputs.getCollector(OUTPUT_Q, null).collect(key, value);
    }

    @Override
    protected void map(Writable key, VectorWritable value, Context context)
      throws IOException, InterruptedException {
      if (mQt != null && cnt++ == r) {
        mQt = null;
      }
      if (mQt == null) {
        loadNextQt();
        cnt = 1;
      }

      // output Bt outer products
      Vector aRow = value.get();
      int qRowIndex = r - cnt; // because QHats are initially stored in
                               // reverse
      Vector qRow = qRowValue.get();
      for (int j = 0; j < kp; j++) {
        qRow.setQuick(j, mQt[j][qRowIndex]);
      }

      // make sure Qs are inheriting A row labels.
      outputQRow(key, qRowValue);

      Vector btRow = btValue.get();
      if (!aRow.isDense()) {
        for (Iterator<Vector.Element> iter = aRow.iterateNonZero(); iter
          .hasNext();) {
          Vector.Element el = iter.next();
          double mul = el.get();
          for (int j = 0; j < kp; j++) {
            btRow.setQuick(j, mul * qRow.getQuick(j));
          }
          btKey.set(el.index());
          context.write(btKey, btValue);
        }
      } else {
        int n = aRow.size();
        for (int i = 0; i < n; i++) {
          double mul = aRow.getQuick(i);
          for (int j = 0; j < kp; j++) {
            btRow.setQuick(j, mul * qRow.getQuick(j));
          }
          btKey.set(i);
          context.write(btKey, btValue);
        }
      }

    }

    @Override
    protected void setup(Context context) throws IOException,
      InterruptedException {
      super.setup(context);

      Path qJobPath = new Path(context.getConfiguration().get(PROP_QJOB_PATH));

      FileSystem fs = FileSystem.get(context.getConfiguration());
      // actually this is kind of dangerous
      // becuase this routine thinks we need to create file name for
      // our current job and this will use -m- so it's just serendipity we are
      // calling
      // it from the mapper too as the QJob did.
      Path qInputPath =
        new Path(qJobPath, FileOutputFormat.getUniqueFile(context,
                                                          QJob.OUTPUT_QHAT,
                                                          ""));
      qInput =
        new SequenceFile.Reader(fs, qInputPath, context.getConfiguration());

      blockNum = context.getTaskAttemptID().getTaskID().getId();

      // read all r files _in order of task ids_, i.e. partitions
      Path rPath = new Path(qJobPath, QJob.OUTPUT_R + "-*");
      FileStatus[] rFiles = fs.globStatus(rPath);

      if (rFiles == null) {
        throw new IOException("Can't find R inputs ");
      }

      Arrays.sort(rFiles, SSVDSolver.PARTITION_COMPARATOR);

      int block = 0;
      for (FileStatus fstat : rFiles) {
        SequenceFileValueIterator<VectorWritable> iterator =
          new SequenceFileValueIterator<VectorWritable>(fstat.getPath(),
                                                        true,
                                                        context
                                                          .getConfiguration());
        VectorWritable rValue;
        try {
          rValue = iterator.next();
        } finally {
          Closeables.closeQuietly(iterator);
        }
        if (block < blockNum && block > 0) {
          GivensThinSolver
            .mergeR(mRs.get(0), new UpperTriangular(rValue.get()));
        } else {
          mRs.add(new UpperTriangular(rValue.get()));
        }
        block++;
      }
      outputs = new MultipleOutputs(new JobConf(context.getConfiguration()));
    }
  }

  public static class OuterProductCombiner extends
      Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {

    protected final VectorWritable outValue = new VectorWritable();
    protected DenseVector accum;
    protected Deque<Closeable> closeables = new ArrayDeque<Closeable>();

    @Override
    protected void reduce(IntWritable key,
                          Iterable<VectorWritable> values,
                          Context ctx) throws IOException, InterruptedException {
      Iterator<VectorWritable> vwIter = values.iterator();

      Vector vec = vwIter.next().get();
      if (accum == null || accum.size() != vec.size()) {
        accum = new DenseVector(vec);
        outValue.set(accum);
      } else {
        accum.assign(vec);
      }

      while (vwIter.hasNext()) {
        accum.addAll(vwIter.next().get());
      }

      ctx.write(key, outValue);
    }

    @Override
    protected void cleanup(Context context) throws IOException,
      InterruptedException {

      IOUtils.close(closeables);
    }
  }

  public static class OuterProductReducer extends OuterProductCombiner {

    private boolean outputBBt;
    private UpperTriangular mBBt;
    private MultipleOutputs outputs;

    @Override
    protected void setup(Context context) throws IOException,
      InterruptedException {

      super.setup(context);

      outputBBt =
        context.getConfiguration().getBoolean(PROP_OUPTUT_BBT_PRODUCTS, false);

      if (outputBBt) {
        int k = context.getConfiguration().getInt(QJob.PROP_K, -1);
        int p = context.getConfiguration().getInt(QJob.PROP_P, -1);

        Validate.isTrue(k > 0, "invalid k parameter");
        Validate.isTrue(p > 0, "invalid p parameter");
        mBBt = new UpperTriangular(k + p);

        outputs = new MultipleOutputs(new JobConf(context.getConfiguration()));
        closeables
          .addFirst(new IOUtils.MultipleOutputsCloseableAdapter(outputs));

      }
    }

    @Override
    protected void reduce(IntWritable key,
                          Iterable<VectorWritable> values,
                          Context ctx) throws IOException, InterruptedException {

      super.reduce(key, values, ctx);

      // at this point, sum of rows should be in accum,
      // so we just generate outer self product of it and add to
      // BBt accumulator.

      if (outputBBt) {
        // accumulate partial BBt sum
        int kp = mBBt.numRows();
        for (int i = 0; i < kp; i++) {
          double vi = accum.get(i);
          if (vi != 0.0) {
            for (int j = i; j < kp; j++) {
              double vj = accum.get(j);
              if (vj != 0.0)
                mBBt.setQuick(i, j, mBBt.getQuick(i, j) + vi * vj);
            }
          }
        }
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException,
      InterruptedException {

      // if we output BBt instead of Bt then we need to do it.
      try {
        if (outputBBt) {

          @SuppressWarnings("unchecked")
          OutputCollector<Writable, Writable> collector =
            outputs.getCollector(OUTPUT_BBT, null);

          collector
            .collect(new IntWritable(),
                     new VectorWritable(new DenseVector(mBBt.getData())));
        }
      } finally {
        super.cleanup(context);
      }

    }

  }

  public static void run(Configuration conf,
                         Path[] inputPathA,
                         Path inputPathQJob,
                         Path outputPath,
                         int minSplitSize,
                         int k,
                         int p,
                         int numReduceTasks,
                         Class<? extends Writable> labelClass,
                         boolean outputBBtProducts)
    throws ClassNotFoundException, InterruptedException, IOException {

    JobConf oldApiJob = new JobConf(conf);

    MultipleOutputs
      .addNamedOutput(oldApiJob,
                      OUTPUT_Q,
                      org.apache.hadoop.mapred.SequenceFileOutputFormat.class,
                      labelClass,
                      VectorWritable.class);

    if (outputBBtProducts) {
      MultipleOutputs
        .addNamedOutput(oldApiJob,
                        OUTPUT_BBT,
                        org.apache.hadoop.mapred.SequenceFileOutputFormat.class,
                        IntWritable.class,
                        VectorWritable.class);
    }

    // hack: we use old api multiple outputs
    // since they are not available in the new api of
    // either 0.20.2 or 0.20.203 but wrap it into a new api
    // job so we can use new api interfaces.

    Job job = new Job(oldApiJob);
    job.setJobName("Bt-job");
    job.setJarByClass(BtJob.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, inputPathA);
    if (minSplitSize > 0) {
      FileInputFormat.setMinInputSplitSize(job, minSplitSize);
    }
    FileOutputFormat.setOutputPath(job, outputPath);

    // MultipleOutputs.addNamedOutput(job, OUTPUT_Bt,
    // SequenceFileOutputFormat.class,
    // QJobKeyWritable.class,QJobValueWritable.class);

    // Warn: tight hadoop integration here:
    job.getConfiguration().set("mapreduce.output.basename", OUTPUT_BT);
    // FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(job,
                                                      CompressionType.BLOCK);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(VectorWritable.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(VectorWritable.class);

    job.setMapperClass(BtMapper.class);
    job.setCombinerClass(OuterProductCombiner.class);
    job.setReducerClass(OuterProductReducer.class);
    // job.setPartitionerClass(QPartitioner.class);

    // job.getConfiguration().setInt(QJob.PROP_AROWBLOCK_SIZE,aBlockRows );
    // job.getConfiguration().setLong(PROP_OMEGA_SEED, seed);
    job.getConfiguration().setInt(QJob.PROP_K, k);
    job.getConfiguration().setInt(QJob.PROP_P, p);
    job.getConfiguration().set(PROP_QJOB_PATH, inputPathQJob.toString());
    job.getConfiguration().setBoolean(PROP_OUPTUT_BBT_PRODUCTS,
                                      outputBBtProducts);

    // number of reduce tasks doesn't matter. we don't actually
    // send anything to reducers. in fact, the only reason
    // we need to configure reduce step is so that combiners can fire.
    // so reduce here is purely symbolic.
    job.setNumReduceTasks(numReduceTasks);

    job.submit();
    job.waitForCompletion(false);

    if (!job.isSuccessful()) {
      throw new IOException("Bt job unsuccessful.");
    }
  }
}
