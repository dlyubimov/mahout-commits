package org.apache.mahout.sparkbindings.io

import com.esotericsoftware.kryo.Kryo
import org.apache.mahout.math._
import org.apache.spark.serializer.KryoRegistrator
import org.apache.mahout.sparkbindings.drm._


/**
 *
 */
class MahoutKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) = {

    kryo.addDefaultSerializer(classOf[Vector], new WritableKryoSerializer[Vector, VectorWritable])
    kryo.addDefaultSerializer(classOf[DenseVector], new WritableKryoSerializer[Vector, VectorWritable])
    kryo.addDefaultSerializer(classOf[Matrix], new WritableKryoSerializer[Matrix, MatrixWritable])

  }
}

object MahoutKryoRegistrator {
}
