package org.apache.mahout.math.stat.distribution

/** Distribution sampler trait */
trait Sampler[T] {
  def unary_~ : T
}
