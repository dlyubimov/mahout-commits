package org.apache.mahout.sparkbindings.drm

import mahout.math._
import mahout.math.RLikeOps._
import org.scalatest.FunSuite
import org.apache.log4j.{Level, Logger, BasicConfigurator}
import java.io.File
import org.apache.spark.SparkContext._


/**
 *
 */
class DrmOpsTests extends FunSuite {


  BasicConfigurator.resetConfiguration()
  BasicConfigurator.configure()
  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("mahout.spark").setLevel(Level.DEBUG)

  // in case we are running from ide, it won't use jars
  // for class path so we need to get maven-computed one
  // in order to pass it to the spark
  val buildJars = {
    val buildDir = new File("ceml-spark-solvers/target/")
    val ls = buildDir.list()
    if (ls == null) Nil
    else
      ls.filter(_.matches(".*\\.jar")).map(buildDir.getAbsolutePath + "/" + _).toIterable
  }

  println("pwd: " + new File(".").getAbsolutePath)
  println(buildJars.toString)

  test("DRM-io") {

    implicit val sc = mahoutSparkContext("spark://localhost:7077", "DrmOpsTests", buildJars)
    //        implicit val sc = mahoutSparkContext("local", "DrmOpsTests", buildJars)
    try {

      val inCoreA = dense((1, 2, 3), (3, 4, 5))
      val drmA = drmParallelize(inCoreA)

      drmA.writeDRM("hdfs://localhost:11010/tmp/UploadedDRM")

      println(inCoreA)

      // load back from hdfs
      val drmB = drmFromHDFS(sc, "hdfs://localhost:11010/tmp/UploadedDRM")

      // collect back into in-core
      val inCoreB = drmB.collect

      //print out to see what it is we collected:
      println(inCoreB)


    } finally {
      sc.stop()
    }
  }

  test("DRM-parallelizeEmpty") {

    //    implicit val sc = mahoutSparkContext("spark://localhost:7077", "DrmOpsTests", buildJars)
    implicit val sc = mahoutSparkContext("local", "DrmOpsTests", buildJars)
    try {

      val drmEmpty = drmParallelizeEmpty(100, 50)

      // collect back into in-core
      val inCoreEmpty = drmEmpty.collect

      //print out to see what it is we collected:
      println(inCoreEmpty)
      printf("drm nrow:%d, ncol:%d\n", drmEmpty.nrow, drmEmpty.ncol)
      printf("in core nrow:%d, ncol:%d\n", inCoreEmpty.nrow, inCoreEmpty.ncol)


    } finally {
      sc.stop()
    }

  }

  test("AtA") {
    //    implicit val sc = mahoutSparkContext("spark://localhost:7077", "DrmOpsTests", buildJars)
    implicit val sc = mahoutSparkContext("local", "DrmOpsTests", buildJars)
    try {

      val inCoreA = dense((1, 2), (2, 3))
      val drmA = drmParallelize(inCoreA)

      val inCoreAtA = drmA.t_sq()
      println(inCoreAtA)

      val expectedAtA = inCoreA.t %*% inCoreA
      println(expectedAtA)

      assert(expectedAtA equiv inCoreAtA)

    } finally {
      sc.stop()
    }
  }

}
