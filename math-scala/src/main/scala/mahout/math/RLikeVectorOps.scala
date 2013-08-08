/**
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

package mahout.math

import org.apache.mahout.math.Vector
import org.apache.mahout.math.function.Functions
import RLikeOps._

class RLikeVectorOps(_v: Vector) extends VectorOps(_v) {

  def *=(that: Vector) = v.assign(that, Functions.MULT)

  def /=(that: Vector) = v.assign(that, Functions.DIV)

  def *=(that: Double) = v.assign(Functions.MULT, that)

  def /=(that: Double) = v.assign(Functions.DIV, that)

  def /=:(that: Double) = v.assign(Functions.INV).assign(Functions.MULT, that)

  def /=:(that: Vector) = v.assign(Functions.INV).assign(that, Functions.MULT)

  def *(that: Vector) = cloned *= that

  def *(that: Double) = cloned *= that

  def /(that: Vector) = cloned /= that

  def /(that: Double) = cloned /= that

  def /:(that: Double) = that /=: v.cloned

  def /:(that: Vector) = that.cloned /= v


}
