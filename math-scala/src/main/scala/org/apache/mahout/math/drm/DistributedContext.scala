package org.apache.mahout.math.drm

import java.io.Closeable
import org.apache.mahout.math.drm.BCast

trait DistributedContext extends Closeable {

  val engine:DistributedEngine

}
