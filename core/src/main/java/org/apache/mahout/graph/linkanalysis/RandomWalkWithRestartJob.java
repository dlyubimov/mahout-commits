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

package org.apache.mahout.graph.linkanalysis;

import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.util.List;
import java.util.Map;

/**
 * <p>Distributed computation of the proximities of vertices to a source vertex in a directed graph</p>
 *
 * <p>This job outputs text files with a vertex id and its pagerank per line.</p>
  *
 * <p>Command line arguments specific to this class are:</p>
 *
 * <ol>
 * <li>--output=(path): output path</li>
 * <li>--vertices=(path): file containing the list of vertices of the graph (one per line)</li>
 * <li>--sourceVertexIndex=(int): index of the source vertex for the random walk (line number in vertices file)</li>
 * <li>--edges=(path): directory containing edges of the graph (pair of vertex ids per line in textformat)</li>
 * <li>--numIterations=(Integer): number of numIterations, default: 10</li>
 * <li>--stayingProbability=(Double): probability not to teleport to a random vertex, default: 0.85</li>
 * </ol>
 *
 * <p>General command line options are documented in {@link AbstractJob}.</p>
 *
 * <p>Note that because of how Hadoop parses arguments, all "-D" arguments must appear before all other arguments.</p>
 */
public class RandomWalkWithRestartJob extends RandomWalk {

  private int sourceVertexIndex;

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RandomWalkWithRestartJob(), args);
  }

  @Override
  protected Vector createDampingVector(int numVertices, double stayingProbability) {
    Vector dampingVector = new RandomAccessSparseVector(numVertices, 1);
    dampingVector.set(sourceVertexIndex, 1.0 - stayingProbability);
    return dampingVector;
  }

  @Override
  protected void addSpecificOptions() {
    addOption("sourceVertexIndex", null, "index of source vertex", true);
  }

  @Override
  protected void evaluateSpecificOptions() {
    sourceVertexIndex = Integer.parseInt(getOption("sourceVertexIndex"));
  }

}