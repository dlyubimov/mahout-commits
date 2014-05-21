package org.apache.mahout.math.drm

import java.io.Closeable

trait DistributedContext extends Closeable {

  val engine:DistributedEngine

}
