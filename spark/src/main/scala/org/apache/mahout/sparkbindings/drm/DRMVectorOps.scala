/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
