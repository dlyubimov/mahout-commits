package org.apache.mahout.math.hadoop.stochasticsvd.qr;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.mahout.common.IOUtils;
import org.apache.mahout.common.iterator.CopyConstructorIterator;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.stochasticsvd.DenseBlockWritable;
import org.apache.mahout.math.hadoop.stochasticsvd.GivensThinSolver;
import org.apache.mahout.math.hadoop.stochasticsvd.Omega;
import org.apache.mahout.math.hadoop.stochasticsvd.UpperTriangular;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

/**
 * Abstracting QR first step from MR abstractions and doing it in terms of
 * iterators etc.
 * 
 * 
 */
@SuppressWarnings("deprecation")
public class QRFirstStep implements Closeable,
    OutputCollector<Writable, VectorWritable> {

  public static final String PROP_OMEGA_SEED = "ssvd.omegaseed";
  public static final String PROP_K = "ssvd.k";
  public static final String PROP_P = "ssvd.p";
  public static final String PROP_AROWBLOCK_SIZE = "ssvd.arowblock.size";

  private int kp;
  private Omega omega;
  private List<double[]> yLookahead;
  private GivensThinSolver qSolver;
  private int blockCnt;
  private int r;
  private final DenseBlockWritable value = new DenseBlockWritable();
  private final Writable tempKey = new IntWritable();
  private MultipleOutputs outputs;
  private final Deque<Closeable> closeables = new LinkedList<Closeable>();
  private SequenceFile.Writer tempQw;
  private Path tempQPath;
  private final List<UpperTriangular> rSubseq = Lists.newArrayList();
  private Configuration jobConf;

  private OutputCollector<? super Writable, ? super DenseBlockWritable> qtHatOut;
  private OutputCollector<? super Writable, ? super VectorWritable> rHatOut;

  public QRFirstStep(Configuration jobConf,
                     OutputCollector<? super Writable, ? super DenseBlockWritable> qtHatOut,
                     OutputCollector<? super Writable, ? super VectorWritable> rHatOut) throws IOException,
    InterruptedException {
    super();
    this.jobConf = jobConf;
    this.qtHatOut = qtHatOut;
    this.rHatOut = rHatOut;
    setup();
  }

  @Override
  public void close() throws IOException {
    cleanup();
  }

  private void flushSolver() throws IOException {
    UpperTriangular r = qSolver.getRTilde();
    double[][] qt = qSolver.getThinQtTilde();

    rSubseq.add(r);

    value.setBlock(qt);
    getTempQw().append(tempKey, value);

    // this probably should be
    // a sparse row matrix,
    // but compressor should get it for disk and in memory we want it
    // dense anyway, sparse random implementations would be
    // a mostly a memory management disaster consisting of rehashes and GC
    // thrashing. (IMHO)
    value.setBlock(null);
    qSolver.reset();
  }

  // second pass to run a modified version of computeQHatSequence.
  private void flushQBlocks() throws IOException {
    if (blockCnt == 1) {
      // only one block, no temp file, no second pass. should be the default
      // mode
      // for efficiency in most cases. Sure mapper should be able to load
      // the entire split in memory -- and we don't require even that.
      value.setBlock(qSolver.getThinQtTilde());
      outputQHat(value);
      outputR(new VectorWritable(new DenseVector(qSolver.getRTilde().getData(),
                                                 true)));

    } else {
      secondPass();
    }
  }

  private void outputQHat(DenseBlockWritable value) throws IOException {
    qtHatOut.collect(NullWritable.get(), value);
  }

  private void outputR(VectorWritable value) throws IOException {
    rHatOut.collect(NullWritable.get(), value);
  }

  private void secondPass() throws IOException {
    qSolver = null; // release mem
    FileSystem localFs = FileSystem.getLocal(jobConf);
    SequenceFile.Reader tempQr =
      new SequenceFile.Reader(localFs, tempQPath, jobConf);
    closeables.addFirst(tempQr);
    int qCnt = 0;
    while (tempQr.next(tempKey, value)) {
      value
        .setBlock(GivensThinSolver.computeQtHat(value.getBlock(),
                                                qCnt,
                                                new CopyConstructorIterator<UpperTriangular>(rSubseq
                                                  .iterator())));
      if (qCnt == 1) {
        // just merge r[0] <- r[1] so it doesn't have to repeat
        // in subsequent computeQHat iterators
        GivensThinSolver.mergeR(rSubseq.get(0), rSubseq.remove(1));
      } else {
        qCnt++;
      }
      outputQHat(value);
    }

    assert rSubseq.size() == 1;

    outputR(new VectorWritable(new DenseVector(rSubseq.get(0).getData(), true)));

  }

  protected void map(Writable key, VectorWritable value) throws IOException {
    double[] yRow;
    if (yLookahead.size() == kp) {
      if (qSolver.isFull()) {

        flushSolver();
        blockCnt++;

      }
      yRow = yLookahead.remove(0);

      qSolver.appendRow(yRow);
    } else {
      yRow = new double[kp];
    }
    omega.computeYRow(value.get(), yRow);
    yLookahead.add(yRow);
  }

  protected void setup() throws IOException, InterruptedException {

    int k = Integer.parseInt(jobConf.get(PROP_K));
    int p = Integer.parseInt(jobConf.get(PROP_P));
    kp = k + p;
    long omegaSeed = Long.parseLong(jobConf.get(PROP_OMEGA_SEED));
    r = Integer.parseInt(jobConf.get(PROP_AROWBLOCK_SIZE));
    omega = new Omega(omegaSeed, k, p);
    yLookahead = Lists.newArrayListWithCapacity(kp);
    qSolver = new GivensThinSolver(r, kp);
    outputs = new MultipleOutputs(new JobConf(jobConf));
    closeables.addFirst(new Closeable() {
      @Override
      public void close() throws IOException {
        outputs.close();
      }
    });

  }

  protected void cleanup() throws IOException {
    try {
      if (qSolver == null && yLookahead.isEmpty()) {
        return;
      }
      if (qSolver == null) {
        qSolver = new GivensThinSolver(yLookahead.size(), kp);
      }
      // grow q solver up if necessary

      qSolver.adjust(qSolver.getCnt() + yLookahead.size());
      while (!yLookahead.isEmpty()) {

        qSolver.appendRow(yLookahead.remove(0));

      }
      assert qSolver.isFull();
      if (++blockCnt > 1) {
        flushSolver();
        assert tempQw != null;
        closeables.remove(tempQw);
        Closeables.closeQuietly(tempQw);
      }
      flushQBlocks();

    } finally {
      IOUtils.close(closeables);
    }

  }

  private SequenceFile.Writer getTempQw() throws IOException {
    if (tempQw == null) {
      // temporary Q output
      // hopefully will not exceed size of IO cache in which case it is only
      // good since it
      // is going to be maanged by kernel, not java GC. And if IO cache is not
      // good enough,
      // then at least it is always sequential.
      String taskTmpDir = System.getProperty("java.io.tmpdir");
      FileSystem localFs = FileSystem.getLocal(jobConf);
      tempQPath = new Path(new Path(taskTmpDir), "q-temp.seq");
      tempQw =
        SequenceFile.createWriter(localFs,
                                  jobConf,
                                  tempQPath,
                                  IntWritable.class,
                                  DenseBlockWritable.class,
                                  CompressionType.BLOCK);
      closeables.addFirst(tempQw);
      closeables.addFirst(new IOUtils.DeleteFileOnClose(new File(tempQPath
        .toString())));
    }
    return tempQw;
  }

  @Override
  public void collect(Writable key, VectorWritable vw) throws IOException {
    map(key, vw);

  }

}
