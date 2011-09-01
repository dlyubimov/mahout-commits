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

package org.apache.mahout.common.iterator.sequencefile;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ForwardingIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.IOUtils;

/**
 * Like {@link SequenceFileValueIterator}, but iterates not just over one sequence file, but many. The input path
 * may be specified as a directory of files to read, or as a glob pattern. The set of files may be optionally
 * restricted with a {@link PathFilter}.
 */
public final class SequenceFileDirValueIterator<V extends Writable>
    extends ForwardingIterator<V> implements Closeable {

  private final Iterator<V> delegate;
  private final List<SequenceFileValueIterator<V>> iterators;

  public SequenceFileDirValueIterator(Path path,
                                      PathType pathType,
                                      PathFilter filter,
                                      Comparator<FileStatus> ordering,
                                      final boolean reuseKeyValueInstances,
                                      final Configuration conf) throws IOException {
    FileStatus[] statuses;
    FileSystem fs = path.getFileSystem(conf);
    if (filter == null) {
      statuses = pathType == PathType.GLOB ? fs.globStatus(path) : fs.listStatus(path);
    } else {
      statuses = pathType == PathType.GLOB ? fs.globStatus(path, filter) : fs.listStatus(path, filter);
    }
    if (ordering != null) {
      Arrays.sort(statuses, ordering);
    }
    Iterator<FileStatus> fileStatusIterator = Iterators.forArray(statuses);

    iterators = Lists.newArrayList();

    Iterator<Iterator<V>> fsIterators =
        Iterators.transform(fileStatusIterator,
                            new Function<FileStatus, Iterator<V>>() {
                              @Override
                              public Iterator<V> apply(FileStatus from) {
                                try {
                                  SequenceFileValueIterator<V> iterator =
                                    new SequenceFileValueIterator<V>(from.getPath(), reuseKeyValueInstances, conf);
                                  iterators.add(iterator);
                                  return iterator;
                                } catch (IOException ioe) {
                                  throw new IllegalStateException(from.getPath().toString(), ioe);
                                }
                              }
                            });

    Collections.reverse(iterators); // close later in reverse order

    delegate = Iterators.concat(fsIterators);
  }

  @Override
  protected Iterator<V> delegate() {
    return delegate;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(iterators);
    iterators.clear();
  }

}
