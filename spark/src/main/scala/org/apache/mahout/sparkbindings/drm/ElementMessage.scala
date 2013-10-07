package org.apache.mahout.sparkbindings.drm

import org.apache.spark.bagel.Message


private[sparkbindings] class ElementMessage[K](val targetId:K, val index:Int, val value:Double) extends Message[K]
