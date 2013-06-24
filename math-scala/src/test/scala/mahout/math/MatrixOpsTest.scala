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

  test("ViewsRead") {

    val a = dense((1, 2, 3), (3, 4, 5))

    assert(a(::, 0).sum == 4)
    assert(a(1, ::).sum == 12)

    assert(a(0 to 1, 1 to 2).sum == 14)

    // assign to slice-vector
    a(0, 0 to 1) :=(3, 5)
    assert(a(0, ::).sum == 11)

    println(a.toString)

    // assign to a slice-matrix
    a(0 to 1, 0 to 1) := dense((1, 1), (2, 2.5))
    println (a.toString)
    println (a.sum)

  }

  test ("SVD") {
    val a = dense((1, 2, 3), (3, 4, 5))
    val (u,v,s)=svd(a)
    printf("U:\n%s\n",u.toString)
    printf("V:\n%s\n",v.toString)
    printf("Sigma:\n%s\n",s.toString)
  }

}
