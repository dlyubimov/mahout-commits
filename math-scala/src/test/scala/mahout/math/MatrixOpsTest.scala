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


    val a = Matrix((1, 2, 3), (3, 4, 5))

    val b = Matrix(1, 44, 55)

    val m = a %*% b

    println(m.toString)


  }

}
