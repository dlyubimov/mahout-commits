package org.apache.mahout.sparkbindings.drm

import mahout.math._
import mahout.math.RLikeOps._
import org.scalatest.FunSuite
import org.apache.log4j.{Level, Logger, BasicConfigurator}
import java.io.File
import org.apache.spark.SparkContext._


/**
 *
 * @author dmitriy
 */
class DRMTests extends FunSuite {


  BasicConfigurator.resetConfiguration()
  BasicConfigurator.configure()
  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("mahout.spark").setLevel(Level.DEBUG)

  val buildJars = Traversable.empty[String]

  println("pwd: " + new File(".").getAbsolutePath)
  println(buildJars.toString)

  test("DRM DFS i/o (local)") {
    drmIOTest(
      sparkMaster = "local",
      uploadPath = "UploadDRM"
    )
  }
  test("DRM DFS i/o (standalone+hdfs)") {
    drmIOTest(
      sparkMaster = "spark://localhost:7077",
      uploadPath = "hdfs://localhost:11010/tmp/UploadDRM"
    )
  }

  def drmIOTest(sparkMaster: String, uploadPath: String) {

    //    implicit val sc = mahoutSparkContext("spark://localhost:7077", "DrmOpsTests", buildJars)
    implicit val sc = mahoutSparkContext(sparkMaster, "DrmOpsTests", buildJars)
    try {

      val inCoreA = dense((1, 2, 3), (3, 4, 5))
      val drmA = drmParallelize(inCoreA)

//      drmA.writeDRM("hdfs://localhost:11010/tmp/UploadedDRM")
      drmA.writeDRM(uploadPath)

      println(inCoreA)

      // load back from hdfs
//      val drmB = drmFromHDFS(sc, "hdfs://localhost:11010/tmp/UploadedDRM")
      val drmB = drmFromHDFS(sc, uploadPath)

      // collect back into in-core
      val inCoreB = drmB.collect

      //print out to see what it is we collected:
      println(inCoreB)


    } finally {
      sc.stop()
    }
  }

  test("DRM parallelizeEmpty") {

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

  test("AtA slim") {
    //    implicit val sc = mahoutSparkContext("spark://localhost:7077", "DrmOpsTests", buildJars)
    implicit val sc = mahoutSparkContext("local", "DrmOpsTests", buildJars)
    try {

      val inCoreA = dense((1, 2), (2, 3))
      val drmA = drmParallelize(inCoreA)

      val inCoreAtA = drmA.t_sq_slim()
      println(inCoreAtA)

      val expectedAtA = inCoreA.t %*% inCoreA
      println(expectedAtA)

      assert(expectedAtA === inCoreAtA)

    } finally {
      sc.stop()
    }
  }

}
