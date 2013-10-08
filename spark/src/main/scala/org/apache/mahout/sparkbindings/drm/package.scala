/*
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

package org.apache.mahout.sparkbindings

import org.apache.mahout.math._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import mahout.math._
import scala.collection.JavaConversions._
import org.apache.hadoop.io.{LongWritable, Text, IntWritable, Writable}
import java.io._
import collection.mutable.ArrayBuffer
import org.apache.mahout.common.IOUtils
import org.apache.log4j.Logger
import java.lang.Math

package object drm {

  final val s_log = Logger.getLogger("mahout.spark.drm");

  implicit def drm2drmOps[K <% Writable : ClassManifest](drm: BaseDRM[K]): ExtendedDRMOps[K] =
    new ExtendedDRMOps[K](drm)

  implicit def drm2IntDrmOps(drm: BaseDRM[Int]): IntIndexedRowsDRMOps = new IntIndexedRowsDRMOps(drm)

  implicit def v2drmvops(v: Vector): DRMVectorOps = new DRMVectorOps(v)

  implicit def v2Writable(v: Vector): VectorWritable = new VectorWritable(v)

  implicit def m2Writable(m: Matrix): MatrixWritable = new MatrixWritable(m)

  implicit def vw2v(vw: VectorWritable): Vector = vw.get()

  implicit def mw2m(mw: MatrixWritable): Matrix = mw.get()


  /**
   * Load DRM from hdfs (as in Mahout DRM format)
   *
   * @param path
   * @param sc spark context (wanted to make that implicit, doesn't work in current version of
   *           scala with the type bounds, sorry)
   *
   * @return DRM[Any] where Any is automatically translated to value type
   */
  def drmFromHDFS(sc: SparkContext, path: String): BaseDRM[Any] = {
    val rdd = sc.sequenceFile(path, classOf[Writable], classOf[VectorWritable]).map(t => (t._1, t._2.get()))

    val key = rdd.map(_._1).take(1)(0)
    val keyWClass = key.getClass.asSubclass(classOf[Writable])

    val key2val = key match {
      case xx: IntWritable => (v: AnyRef) => v.asInstanceOf[IntWritable].get
      case xx: Text => (v: AnyRef) => v.asInstanceOf[Text].toString
      case xx: LongWritable => (v: AnyRef) => v.asInstanceOf[LongWritable].get
      case xx: Writable => (v: AnyRef) => v
    }

    val val2key = key match {
      case xx: IntWritable => (x: Any) => new IntWritable(x.asInstanceOf[Int])
      case xx: Text => (x: Any) => new Text(x.toString)
      case xx: LongWritable => (x: Any) => new LongWritable(x.asInstanceOf[Int])
      case xx: Writable => (x: Any) => x.asInstanceOf[Writable]
    }

    {
      implicit val km: ClassManifest[AnyRef] = ClassManifest.classType(key.getClass)
      implicit def getWritable(x: Any): Writable = val2key()
      new BaseDRM[Any](rdd.map(t => (key2val(t._1), t._2)))
    }
  }

  /** Shortcut to parallelizing matrices with indices, ignore row labels. */
  def drmParallelize(m: Matrix, numPartitions: Int = 1)
      (implicit sc: SparkContext) =
    drmParallelizeWithRowIndices(m, numPartitions)(sc)

  /** Parallelize in-core matrix as spark distributed matrix, using row ordinal indices as data set keys. */
  def drmParallelizeWithRowIndices(m: Matrix, numPartitions: Int = 1)
      (implicit sc: SparkContext)
  : BaseDRM[Int] = {

    import mahout.math.RLikeOps._

    val p = (0 until m.nrow).map(i => i -> m(i, ::))
    new BaseDRM(sc.parallelize(p, numPartitions))
  }

  /** Parallelize in-core matrix as spark distributed matrix, using row labels as a data set keys. */
  def drmParallelizeWithRowLabels(m: Matrix, numPartitions: Int = 1)
      (implicit sc: SparkContext)
  : BaseDRM[String] = {

    import mahout.math.RLikeOps._

    // In spark 0.8, I have patched ability to parallelize kryo objects directly, so no need to
    // wrap that into byte array anymore
    val rb = m.getRowLabelBindings
    val p = for (i: String <- rb.keySet().toIndexedSeq) yield i -> m(rb(i), ::)


    new BaseDRM(sc.parallelize(p, numPartitions))
  }

  /** This creates an empty DRM with specified number of partitions and cardinality. */
  def drmParallelizeEmpty(nrow: Int, ncol: Int, numPartitions: Int = 10)
      (implicit sc: SparkContext): BaseDRM[Int] = {
    val rdd = sc.parallelize(0 to numPartitions, numPartitions).flatMap(part => {
      val partNRow = (nrow - 1) / numPartitions + 1
      val partStart = partNRow * part
      val partEnd = Math.min(partStart + partNRow, nrow)

      for (i <- partStart until partEnd) yield (i, new RandomAccessSparseVector(ncol): Vector)
    })
    new BaseDRM[Int](rdd, nrow, ncol)
  }

  def drmParallelizeEmptyLong(nrow: Long, ncol: Int, numPartitions: Int = 10)
      (implicit sc: SparkContext): BaseDRM[Long] = {
    val rdd = sc.parallelize(0 to numPartitions, numPartitions).flatMap(part => {
      val partNRow = (nrow - 1) / numPartitions + 1
      val partStart = partNRow * part
      val partEnd = Math.min(partStart + partNRow, nrow)

      for (i <- partStart until partEnd) yield (i, new RandomAccessSparseVector(ncol): Vector)
    })
    new BaseDRM[Long](rdd, nrow, ncol)
  }


  /**
   * Create proper spark context that includes local Mahout jars
   * @param masterUrl
   * @param appName
   * @param customJars
   * @return
   */
  def mahoutSparkContext(masterUrl: String, appName: String,
      customJars: TraversableOnce[String] = Nil): SparkContext = {
    val closeables = new java.util.ArrayDeque[Closeable]()

    try {

      var mhome = System.getenv("MAHOUT_HOME")
      if (mhome == null) mhome = System.getProperty("mahout.home")

      if (mhome == null)
        throw new IllegalArgumentException("MAHOUT_HOME is required to spawn mahout-based spark jobs.")

      // Figure Mahout classpath using $MAHOUT_HOME/mahout classpath command.

      val fmhome = new File(mhome)
      val bin = new File(fmhome, "bin")
      val exec = new File(bin, "mahout")
      if (!exec.canExecute)
        throw new IllegalArgumentException("Cannot execute %s.".format(exec.getAbsolutePath))

      val p = Runtime.getRuntime.exec(Array(exec.getAbsolutePath, "classpath"))

      closeables.addFirst(new Closeable {
        def close() {
          p.destroy()
        }
      })

      val r = new BufferedReader(new InputStreamReader(p.getInputStream))
      closeables.addFirst(r)

      val w = new StringWriter()
      closeables.addFirst(w)

      var continue = true;
      val jars = new ArrayBuffer[String]()
      do {
        val cp = r.readLine()
        if (cp == null)
          throw new IllegalArgumentException("Unable to read output from \"mahout classpath\"")

        val j = cp.split(File.pathSeparatorChar)
        if (j.size > 10) {
          // assume this is a valid classpath line
          jars ++= j
          continue = false
        }
      } while (continue)

      //      if (s_log.isDebugEnabled) {
      //        s_log.debug("Mahout jars:")
      //        jars.foreach(j => s_log.debug(j))
      //      }

      // context specific jars
      val mcjars = jars.filter(j =>
        j.matches(".*mahout-math-.*\\.jar") ||
            j.matches(".*mahout-math-scala-.*\\.jar") ||
            j.matches(".*mahout-core-.*\\.jar") ||
            j.matches(".*mahout-spark-.*\\.jar")
      ).filter(!_.matches(".*-tests.jar")) ++
          SparkContext.jarOfClass(classOf[DRMVectorOps]) ++ customJars

      if (s_log.isDebugEnabled) {
        s_log.debug("Mahout jars:")
        mcjars.foreach(j => s_log.debug(j))
      }

      System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      System.setProperty("spark.kryo.registrator", "org.apache.mahout.sparkbindings.io.MahoutKryoRegistrator")

      new SparkContext(masterUrl, appName, jars = mcjars.toSeq)

    } finally {
      IOUtils.close(closeables)
    }

  }
}
