package mahout.math

import org.scalatest.FunSuite

/**
 * Created with IntelliJ IDEA.
 * User: dmitriy
 * Date: 6/21/13
 * Time: 10:27 PM
 * To change this template use File | Settings | File Templates.
 */
class MatrixOpsTest extends FunSuite {


  test("MulTest") {

    val a = dense((1, 2, 3), (3, 4, 5))
    val b = dense(1, 4, 5)
    val m = a %*% b

    assert(m(0, 0) == 24)
    assert(m(1, 0) == 44)
    println(m.toString)
  }

  test("HadamardTest") {
    val a = dense((1, 2, 3), (3, 4, 5))
    val b = dense((1, 1, 2), (2, 1, 1))

    val c = a * b
    assert(c(0, 0) == 1)
    assert(c(1, 2) == 5)
    println(c.toString)

    a *= b
    assert(a(0, 0) == 1)
    assert(a(1, 2) == 5)
    println(a.toString)

  }

}
