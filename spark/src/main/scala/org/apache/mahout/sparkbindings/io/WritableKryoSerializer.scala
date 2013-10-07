package org.apache.mahout.sparkbindings.io

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.hadoop.io.{DataInputBuffer, DataOutputBuffer, Writable}

/**
 *
 * @author dmitriy
 */
class WritableKryoSerializer[V <% Writable, W <: Writable <% V : ClassManifest] extends Serializer[V] {

  def write(kryo: Kryo, out: Output, v: V) = {
    val dob = new DataOutputBuffer()
    v.write(dob)
    dob.close()

    out.writeInt(dob.getLength)
    out.write(dob.getData, 0, dob.getLength)
  }

  def read(kryo: Kryo, in: Input, vClazz: Class[V]): V = {
    val dib = new DataInputBuffer()
    val len = in.readInt()
    val data = new Array[Byte](len)
    in.read(data)
    dib.reset(data, len)
    val w: W = classManifest[W].erasure.newInstance().asInstanceOf[W]
    w.readFields(dib)
    w

  }
}
