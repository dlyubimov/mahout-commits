package mahout.math

import org.scalatest.FunSuite
import org.apache.mahout.math.Vector


class MatrixOpsTest extends FunSuite {


  test("eqTest") {
    val a = dense((1, 2, 3), (3, 4, 5))
    val b = dense((1, 2, 3), (3, 4, 5))
    val c = dense((1, 4, 3), (3, 4, 5))
    assert(a equiv b)
    assert(a nequiv c)

  }
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
    println(a.toString)
    println(a.sum)

  }

  test("SVD") {

    val a = dense((1, 2, 3), (3, 4, 5))

    val (u, v, s) = svd(a)

    printf("U:\n%s\n", u.toString)
    printf("V:\n%s\n", v.toString)
    printf("Sigma:\n%s\n", s.toString)

    val aBar = u %*% diagv(s) %*% v.t

    val amab = a - aBar

    printf("A-USV'=\n%s\n", amab.toString)

    assert(amab.norm < 1e-10)

  }

  test("sparse") {

    val a = sparse((1, 3) :: Nil,
      (0, 2) ::(1, 2.5) :: Nil
    )
    println(a.toString)
  }

  test("chol") {

    // try to solve Ax=b with cholesky:
    // this requires
    // (LL')x = B
    // L'x= (L^-1)B
    // x=(L'^-1)(L^-1)B

    val a = dense((1, 2, 3), (2, 3, 4), (3, 4, 5.5))

    // make sure it is symmetric for a valid solution
    a := a.t %*% a

    printf("A= \n%s\n", a)

    val b = dense((9, 8, 7)).t

    printf("b = \n%s\n", b)

    val ch = chol(a)

    printf("L = \n%s\n", ch.getL)

    printf("(L^-1)b =\n%s\n", ch.solveLeft(b))

    val x = ch.solveRight(diag(1, 3)) %*% ch.solveLeft(b)

    printf("x = \n%s\n", x.toString)

    val axmb = (a %*% x) - b

    printf("AX - B = \n%s\n", axmb.toString)

    assert(axmb.norm < 1e-10)

  }

}
