package mahout.math

import org.scalatest.FunSuite
import org.apache.mahout.math.{RandomAccessSparseVector, Vector}

/**
 * Created with IntelliJ IDEA.
 * User: dmitriy
 * Date: 6/21/13
 * Time: 10:26 PM
 * To change this template use File | Settings | File Templates.
 */
class VectorOpsTest extends FunSuite {

  test("CreateTest") {

    val sparseVec = svec((5, 1) :: Nil)
    println(sparseVec.toString)

    val sparseVec2: Vector = (5 -> 1.0) :: Nil
    println(sparseVec2.toString)

    val sparseVec3: Vector = new RandomAccessSparseVector(100) := (5 -> 1.0) :: Nil
    println(sparseVec3)
  }

}
