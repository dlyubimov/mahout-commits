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

    val sparseVec = svec((5 -> 1) :: (10 -> 2.0) :: Nil)
    println(sparseVec)

    val sparseVec2: Vector = (5 -> 1.0) :: (10 -> 2.0) :: Nil
    println(sparseVec2)

    val sparseVec3: Vector = new RandomAccessSparseVector(100) := (5 -> 1.0) :: Nil
    println(sparseVec3)

    val denseVec1: Vector = (1.0, 1.1, 1.2)
    println(denseVec1)

    val denseVec2 = dvec(1, 0, 1.1, 1.2)
    println(denseVec2)
  }

  test("plus-minus") {

    val a: Vector = (1, 2, 3)
    val b: Vector = (0 -> 3) :: (1 -> 4) :: (2 -> 5) :: Nil

    val c = a + b
    val d = b - a
    val e = a dot b

    assert(c ===(4, 6, 8))
    assert(d ===(2, 2, 2))

  }

  test("Hadamard") {
    val a: Vector = (1, 2, 3)
    val b = (3, 4, 5)

    val c = a * b
    println(c)
    assert(c ===(3, 8, 15))
  }

  test("dot") {

    val a: Vector = (1, 2, 3)
    val b = (3, 4, 5)

    val c = a dot b
    println(c)
    assert(c == 26)

  }


}
