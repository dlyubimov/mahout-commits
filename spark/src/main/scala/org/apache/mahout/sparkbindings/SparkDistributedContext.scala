package org.apache.mahout.sparkbindings

import org.apache.mahout.math.scalabindings.drm.DistributedContext
import org.apache.spark.SparkContext

class SparkDistributedContext(val sc: SparkContext) extends DistributedContext
