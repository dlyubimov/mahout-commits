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
import java.util.Deque;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.IOUtils;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.DistributedRowMatrixWriter;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;
import org.apache.mahout.math.ssvd.EigenSolverWrapper;

import com.google.common.collect.Lists;

/**
 * Stochastic SVD solver (API class).
 * <P>
 * 
 * Implementation details are in my working notes in MAHOUT-376
 * (https://issues.apache.org/jira/browse/MAHOUT-376).
 * <P>
 * 
 * As of the time of this writing, I don't have benchmarks for this method in
 * comparison to other methods. However, non-hadoop differentiating
 * characteristics of this method are thought to be :
 * <LI>"faster" and precision is traded off in favor of speed. However, there's
 * lever in terms of "oversampling parameter" p. Higher values of p produce
 * better precision but are trading off speed (and minimum RAM requirement).
 * This also means that this method is almost guaranteed to be less precise than
 * Lanczos unless full rank SVD decomposition is sought.
 * <LI>"more scale" -- can presumably take on larger problems than Lanczos one
 * (not confirmed by benchmark at this time)
 * <P>
 * <P>
 * 
 * Specifically in regards to this implementation, <i>I think</i> couple of
 * other differentiating points are:
 * <LI>no need to specify input matrix height or width in command line, it is
 * what it gets to be.
 * <LI>supports any Writable as DRM row keys and copies them to correspondent
 * rows of U matrix;
 * <LI>can request U or V or U<sub>&sigma;</sub>=U* &Sigma;<sup>0.5</sup> or
 * V<sub>&sigma;</sub>=V* &Sigma;<sup>0.5</sup> none of which would require pass
 * over input A and these jobs are parallel map-only jobs.
 * <P>
 * <P>
 * 
 * This class is central public API for SSVD solver. The use pattern is as
 * follows:
 * 
 * <UL>
 * <LI>create the solver using constructor and supplying computation parameters.
 * <LI>set optional parameters thru setter methods.
 * <LI>call {@link #run()}.
 * <LI> {@link #getUPath()} (if computed) returns the path to the directory
 * containing m x k U matrix file(s).
 * <LI> {@link #getVPath()} (if computed) returns the path to the directory
 * containing n x k V matrix file(s).
 * 
 * </UL>
 */
public class SSVDSolver {

  private Vector svalues;
  private boolean computeU = true;
  private boolean computeV = true;
  private String uPath;
  private String vPath;
  private int outerBlockHeight = 30000;
  private int abtBlockHeight = 200000;

  // configured stuff
  private final Configuration conf;
  private final Path[] inputPath;
  private final Path outputPath;
  private final int ablockRows;
  private final int k;
  private final int p;
  private int q;
  private final int reduceTasks;
  private int minSplitSize = -1;
  private boolean cUHalfSigma;
  private boolean cVHalfSigma;
  private boolean overwrite;
  private boolean broadcast = true;
  private Path pcaMeanPath;

  /**
   * create new SSVD solver. Required parameters are passed to constructor to
   * ensure they are set. Optional parameters can be set using setters .
   * <P>
   * 
   * @param conf
   *          hadoop configuration
   * @param inputPath
   *          Input path (should be compatible with DistributedRowMatrix as of
   *          the time of this writing).
   * @param outputPath
   *          Output path containing U, V and singular values vector files.
   * @param ablockRows
   *          The vertical hight of a q-block (bigger value require more memory
   *          in mappers+ perhaps larger {@code minSplitSize} values
   * @param k
   *          desired rank
   * @param p
   *          SSVD oversampling parameter
   * @param reduceTasks
   *          Number of reduce tasks (where applicable)
   * @throws IOException
   *           when IO condition occurs.
   */
  public SSVDSolver(Configuration conf,
                    Path[] inputPath,
                    Path outputPath,
                    int ablockRows,
                    int k,
                    int p,
                    int reduceTasks) {
    this.conf = conf;
    this.inputPath = inputPath;
    this.outputPath = outputPath;
    this.ablockRows = ablockRows;
    this.k = k;
    this.p = p;
    this.reduceTasks = reduceTasks;
  }

  public void setcUHalfSigma(boolean cUHat) {
    this.cUHalfSigma = cUHat;
  }

  public void setcVHalfSigma(boolean cVHat) {
    this.cVHalfSigma = cVHat;
  }

  public int getQ() {
    return q;
  }

  /**
   * sets q, amount of additional power iterations to increase precision
   * (0..2!). Defaults to 0.
   * 
   * @param q
   */
  public void setQ(int q) {
    this.q = q;
  }

  /**
   * The setting controlling whether to compute U matrix of low rank SSVD.
   * 
   */
  public void setComputeU(boolean val) {
    computeU = val;
  }

  /**
   * Setting controlling whether to compute V matrix of low-rank SSVD.
   * 
   * @param val
   *          true if we want to output V matrix. Default is true.
   */
  public void setComputeV(boolean val) {
    computeV = val;
  }

  /**
   * Sometimes, if requested A blocks become larger than a split, we may need to
   * use that to ensure at least k+p rows of A get into a split. This is
   * requirement necessary to obtain orthonormalized Q blocks of SSVD.
   * 
   * @param size
   *          the minimum split size to use
   */
  public void setMinSplitSize(int size) {
    minSplitSize = size;
  }

  /**
   * This contains k+p singular values resulted from the solver run.
   * 
   * @return singlular values (largest to smallest)
   */
  public Vector getSingularValues() {
    return svalues;
  }

  /**
   * returns U path (if computation were requested and successful).
   * 
   * @return U output hdfs path, or null if computation was not completed for
   *         whatever reason.
   */
  public String getUPath() {
    return uPath;
  }

  /**
   * return V path ( if computation was requested and successful ) .
   * 
   * @return V output hdfs path, or null if computation was not completed for
   *         whatever reason.
   */
  public String getVPath() {
    return vPath;
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  /**
   * if true, driver to clean output folder first if exists.
   * 
   * @param overwrite
   */
  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  public int getOuterBlockHeight() {
    return outerBlockHeight;
  }

  /**
   * The height of outer blocks during Q'A multiplication. Higher values allow
   * to produce less keys for combining and shuffle and sort therefore somewhat
   * improving running time; but require larger blocks to be formed in RAM (so
   * setting this too high can lead to OOM).
   * 
   * @param outerBlockHeight
   */
  public void setOuterBlockHeight(int outerBlockHeight) {
    this.outerBlockHeight = outerBlockHeight;
  }

  public int getAbtBlockHeight() {
    return abtBlockHeight;
  }

  /**
   * the block height of Y_i during power iterations. It is probably important
   * to set it higher than default 200,000 for extremely sparse inputs and when
   * more ram is available. y_i block height and ABt job would occupy approx.
   * abtBlockHeight x (k+p) x sizeof (double) (as dense).
   * 
   * @param abtBlockHeight
   */
  public void setAbtBlockHeight(int abtBlockHeight) {
    this.abtBlockHeight = abtBlockHeight;
  }

  public boolean isBroadcast() {
    return broadcast;
  }

  /**
   * If this property is true, use DestributedCache mechanism to broadcast some
   * stuff around. May improve efficiency. Default is false.
   * 
   * @param broadcast
   */
  public void setBroadcast(boolean broadcast) {
    this.broadcast = broadcast;
  }

  /**
   * Optional. Single-vector file path for a vector (aka xi in MAHOUT-817
   * working notes) to be subtracted from each row of input.
   * <P>
   * 
   * Brute force approach would force would turn input into a dense input, which
   * is often not very desirable. By supplying this offset to SSVD solver, we
   * can avoid most of that overhead due to increased input density.
   * <P>
   * 
   * The vector size for this offest is n (width of A input). In PCA and R this
   * is known as "column means", but in this case it can be any offset of row
   * vectors of course to propagate into SSVD solution.
   * <P>
   * 
   */
  public Path getPcaMeanPath() {
    return pcaMeanPath;
  }

  public void setPcaMeanPath(Path pcaMeanPath) {
    this.pcaMeanPath = pcaMeanPath;
  }

  /**
   * run all SSVD jobs.
   * 
   * @throws IOException
   *           if I/O condition occurs.
   */
  public void run() throws IOException {

    Deque<Closeable> closeables = Lists.newLinkedList();
    try {
      Class<? extends Writable> labelType =
        SSVDHelper.sniffInputLabelType(inputPath, conf);
      FileSystem fs = FileSystem.get(conf);

      Path qPath = new Path(outputPath, "Q-job");
      Path btPath = new Path(outputPath, "Bt-job");
      Path uHatPath = new Path(outputPath, "UHat");
      Path svPath = new Path(outputPath, "Sigma");
      Path uPath = new Path(outputPath, "U");
      Path vPath = new Path(outputPath, "V");

      Path pcaBasePath = new Path(outputPath, "pca");
      Path sbPath = null;
      Path sqPath = null;

      double xisquaredlen = 0;

      if (pcaMeanPath != null)
        fs.mkdirs(pcaBasePath);
      Random rnd = RandomUtils.getRandom();
      long seed = rnd.nextLong();

      if (pcaMeanPath != null) {
        /*
         * combute s_b0 if pca offset present.
         * 
         * Just in case, we treat xi path as a possible reduce or otherwise
         * multiple task output that we assume we need to sum up partial
         * components. If it is just one file, it will work too.
         */

        Vector xi = SSVDHelper.loadAndSumUpVectors(pcaMeanPath, conf);
        xisquaredlen = xi.dot(xi);
        Omega omega = new Omega(seed, k + p);
        Vector s_b0 = omega.mutlithreadedTRightMultiply(xi);

        SSVDHelper.saveVector(s_b0, sbPath =
          new Path(pcaBasePath, "somega.seq"), conf);
      }

      if (overwrite) {
        fs.delete(outputPath, true);
      }

      /*
       * if we work with pca offset, we need to precompute s_bq0 aka s_omega for
       * jobs to use.
       */

      QJob.run(conf,
               inputPath,
               sbPath,
               qPath,
               ablockRows,
               minSplitSize,
               k,
               p,
               seed,
               reduceTasks);

      /*
       * restrict number of reducers to a reasonable number so we don't have to
       * run too many additions in the frontend when reconstructing BBt for the
       * last B' and BB' computations. The user may not realize that and gives a
       * bit too many (I would be happy i that were ever the case though).
       */

      sbPath = new Path(pcaBasePath, "sb0");
      sqPath = new Path(pcaBasePath, "sq0");

      BtJob.run(conf,
                inputPath,
                qPath,
                pcaMeanPath,
                btPath,
                minSplitSize,
                k,
                p,
                outerBlockHeight,
                q <= 0 ? Math.min(1000, reduceTasks) : reduceTasks,
                broadcast,
                labelType,
                q <= 0);

      sbPath = new Path(btPath, BtJob.OUTPUT_SB + "-*");
      sqPath = new Path(btPath, BtJob.OUTPUT_SQ + "-*");

      // power iterations
      for (int i = 0; i < q; i++) {

        qPath = new Path(outputPath, String.format("ABt-job-%d", i + 1));
        Path btPathGlob = new Path(btPath, BtJob.OUTPUT_BT + "-*");
        ABtDenseOutJob.run(conf,
                           inputPath,
                           btPathGlob,
                           pcaMeanPath,
                           sqPath,
                           sbPath,
                           qPath,
                           ablockRows,
                           minSplitSize,
                           k,
                           p,
                           abtBlockHeight,
                           reduceTasks,
                           broadcast);

        btPath = new Path(outputPath, String.format("Bt-job-%d", i + 1));

        BtJob.run(conf,
                  inputPath,
                  qPath,
                  pcaMeanPath,
                  btPath,
                  minSplitSize,
                  k,
                  p,
                  outerBlockHeight,
                  i == q - 1 ? Math.min(1000, reduceTasks) : reduceTasks,
                  broadcast,
                  labelType,
                  i == q - 1);
        sbPath = new Path(btPath, BtJob.OUTPUT_SB + "-*");
        sqPath = new Path(btPath, BtJob.OUTPUT_SQ + "-*");
      }

      UpperTriangular bbtTriangular =
        SSVDHelper.loadAndSumUpperTriangularMatrices(new Path(btPath,
                                                              BtJob.OUTPUT_BBT
                                                                  + "-*"), conf);

      // convert bbt to something our eigensolver could understand
      assert bbtTriangular.columnSize() == k + p;

      /*
       * we currently use a 3rd party in-core eigensolver. So we need just a
       * dense array representation for it.
       */
      DenseMatrix bbtSquare = new DenseMatrix(k+p,k+p);

      for (int i = 0; i < k + p; i++) {
        for (int j = i; j < k + p; j++) {
          double val = bbtTriangular.getQuick(i, j);
          bbtSquare.setQuick(i, j, val);
          bbtSquare.setQuick(j, i, val);
        }
      }
      
      // MAHOUT-817
      if (pcaMeanPath != null) {
        Vector sq = SSVDHelper.loadAndSumUpVectors(sqPath, conf);
        Vector sb = SSVDHelper.loadAndSumUpVectors(sbPath, conf);
        Matrix mC = sq.cross(sb);

        bbtSquare.assign(mC, Functions.MINUS);
        bbtSquare.assign(mC.transpose(), Functions.MINUS);
        mC = null;

        Matrix outerSq = sq.cross(sq);
        outerSq.assign(Functions.mult(xisquaredlen));
        bbtSquare.assign(outerSq, Functions.PLUS);

      }

      EigenSolverWrapper eigenWrapper = new EigenSolverWrapper(SSVDHelper.extractRawData(bbtSquare));
      Matrix uHat = new DenseMatrix(eigenWrapper.getUHat());
      svalues = new DenseVector(eigenWrapper.getEigenValues());

      svalues.assign(Functions.SQRT);

      // save/redistribute UHat
      fs.mkdirs(uHatPath);
      DistributedRowMatrixWriter.write(uHatPath =
        new Path(uHatPath, "uhat.seq"), conf, uHat);

      // save sigma.
      SSVDHelper.saveVector(svalues,
                            svPath = new Path(svPath, "svalues.seq"),
                            conf);

      UJob ujob = null;
      if (computeU) {
        ujob = new UJob();
        ujob.run(conf,
                 new Path(btPath, BtJob.OUTPUT_Q + "-*"),
                 uHatPath,
                 svPath,
                 uPath,
                 k,
                 reduceTasks,
                 labelType,
                 cUHalfSigma);
        // actually this is map-only job anyway
      }

      VJob vjob = null;
      if (computeV) {
        vjob = new VJob();
        vjob.run(conf,
                 new Path(btPath, BtJob.OUTPUT_BT + "-*"),
                 pcaMeanPath,
                 sqPath,
                 uHatPath,
                 svPath,
                 vPath,
                 k,
                 reduceTasks,
                 cVHalfSigma);
      }

      if (ujob != null) {
        ujob.waitForCompletion();
        this.uPath = uPath.toString();
      }
      if (vjob != null) {
        vjob.waitForCompletion();
        this.vPath = vPath.toString();
      }

    } catch (InterruptedException exc) {
      throw new IOException("Interrupted", exc);
    } catch (ClassNotFoundException exc) {
      throw new IOException(exc);

    } finally {
      IOUtils.close(closeables);
    }

  }
}
