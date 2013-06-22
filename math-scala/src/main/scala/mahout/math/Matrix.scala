package mahout.math

import org.apache.mahout.math.DenseMatrix
import com.sun.javaws.exceptions.InvalidArgumentException

object Matrix {

  def apply[R](rows: R*) = {
    val data = for (r <- rows) yield {

      r match {
        case n: Number => Array(n.doubleValue())
        case t: Tuple1[_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple2[_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple3[_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple4[_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple5[_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple6[_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple7[_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple8[_,_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple9[_,_,_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple10[_,_,_,_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple11[_,_,_,_,_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple12[_,_,_,_,_,_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple13[_,_,_,_,_,_,_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple14[_,_,_,_,_,_,_,_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple15[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple16[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple17[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple18[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple19[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple20[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple21[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case t: Tuple22[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => t.productIterator.map(_.asInstanceOf[Number].doubleValue()).toArray
        case _ => throw new IllegalArgumentException("unsupported type in the inline Matrix initializer")
      }
    }
    new DenseMatrix(data.toArray)
  }

}
