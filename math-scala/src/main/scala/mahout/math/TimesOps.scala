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

package mahout.math

import org.apache.mahout.math.{MatrixTimesOps, Matrix, DiagonalMatrix}

/**
 * Created with IntelliJ IDEA.
 * User: dmitriy
 * Date: 7/17/13
 * Time: 11:01 PM
 * To change this template use File | Settings | File Templates.
 */
class TimesOps(m: MatrixTimesOps) {

  def :%*%(that: Matrix) = m.timesRight(that)

  def %*%:(that: Matrix) = m.timesLeft(that)


}
