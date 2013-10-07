package org.apache.mahout.sparkbindings.drm

import org.apache.mahout.math.{VectorWritable, Vector}
import java.util.Arrays
import org.apache.hadoop.io.{DataInputBuffer, DataOutputBuffer}

class DRMVectorOps(val v: Vector) {

  def toByteArray: Array[Byte] = {
    val dob = new DataOutputBuffer()
    new VectorWritable(v).write(dob)
    dob.close()
    var arr = dob.getData
    if (arr.size != dob.getLength) arr = Arrays.copyOf(arr, dob.getLength)
    arr
  }

}

object DRMVectorOps {

  def fromByteArray(arr: Array[Byte]): Vector = fromByteArray(arr, 0, arr.size)

  def fromByteArray(arr: Array[Byte], offset: Int, len: Int): Vector = {
    val dib = new DataInputBuffer()
    dib.reset(arr, offset, len)
    val vw = new VectorWritable()
    vw.readFields(dib)
    dib.close()
    vw.get()
  }
}
