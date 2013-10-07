package org.apache.mahout.sparkbindings.drm

import org.apache.mahout.math.Vector
import org.apache.spark.bagel.Message

/**
 *
 * @author dmitriy
 */
private[sparkbindings] class VectorMessage[K](val targetId:K, var v:Vector) extends Message[K]
