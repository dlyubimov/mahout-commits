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

import org.apache.mahout.math.{Matrix, Vector}

/**
 *
 * Basic spark DRM trait.
 *
 * Since we already call the package "sparkbindings", I will not use stem "spark" with classes in
 * this package. Spark backing is already implied.
 *
 */
trait DRM[K] {

  /** R-like syntax for number of rows. */
  def nrow: Long

  /** R-like syntax for number of columns */
  def ncol: Int

  /**
   * Make sure the computed underlying matrix will be cached with Spark.
   * @return
   */
  def cached(): DRM[K]

  /**
   * Remove current backing RDD from Spark's block cache manager. (non-blocking).
   */
  def uncache()

  /**
   * Transform rows into a new DRM.
   * @param mapfun mapper funcition
   * @return new DRM
   */
  def mapRows(mapfun: (K, Vector) => Vector): DRM[K]

  /**
   * Collect this distributed matrix into front end in-core representation. This will trigger a
   * distributed computation, if necessary.
   *
   * @return in-core representation.
   */
  def collect(): Matrix

  /**
   * Dump matrix as computed Mahout's DRM into specified (HD)FS path
   * @param path
   */
  def writeDRM(path: String)
}
