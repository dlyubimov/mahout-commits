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
package org.apache.mahout.math.hadoop.stochasticsvd;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Varint;

/**
 * a key for vectors allowing to identify them by their coordinates in original
 * split of A.
 * 
 * We assume all passes over A results in the same splits, thus, we can always
 * prepare side files that come into contact with A, sp that they are sorted and
 * partitioned same way.
 * 
 */
public class TaskRowWritable implements WritableComparable<TaskRowWritable> {

  private int taskId;
  private int taskRowOrdinal;

  public TaskRowWritable(Mapper<?, ?, ?, ?>.Context mapperContext) {
    super();
    // this is basically a split # if i understand it right
    taskId = mapperContext.getTaskAttemptID().getTaskID().getId();
  }

  public TaskRowWritable() {
    super();
  }

  public int getTaskId() {
    return taskId;
  }

  public int getTaskRowOrdinal() {
    return taskRowOrdinal;
  }

  public void incrementRowOrdinal() {
    taskRowOrdinal++;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    taskId = Varint.readUnsignedVarInt(in);
    taskRowOrdinal = Varint.readUnsignedVarInt(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Varint.writeUnsignedVarInt(taskId, out);
    Varint.writeUnsignedVarInt(taskRowOrdinal, out);
  }

  @Override
  public int compareTo(TaskRowWritable o) {
    if (taskId < o.taskId) {
      return -1;
    } else if (taskId > o.taskId) {
      return 1;
    }
    if (taskRowOrdinal < o.taskRowOrdinal) {
      return -1;
    } else if (taskRowOrdinal > o.taskRowOrdinal) {
      return 1;
    }
    return 0;
  }

}