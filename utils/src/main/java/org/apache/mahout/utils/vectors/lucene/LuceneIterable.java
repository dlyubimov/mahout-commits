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

package org.apache.mahout.utils.vectors.lucene;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.index.IndexReader;
import org.apache.mahout.math.Vector;

/**
 * {@link Iterable} counterpart to {@link LuceneIterator}.
 */
public final class LuceneIterable implements Iterable<Vector> {

  public static final double NO_NORMALIZING = -1.0;

  private final IndexReader indexReader;
  private final String field;
  private final String idField;
  private final VectorMapper mapper;
  private final double normPower;
  private final double maxPercentErrorDocs;

  public LuceneIterable(IndexReader reader, String idField, String field, VectorMapper mapper) {
    this(reader, idField, field, mapper, NO_NORMALIZING);
  }
  
  public LuceneIterable(IndexReader indexReader, String idField, String field, VectorMapper mapper, double normPower) {
    this(indexReader, idField, field, mapper, normPower, 0);
  }
  
  /**
   * Produce a LuceneIterable that can create the Vector plus normalize it.
   * 
   * @param indexReader {@link org.apache.lucene.index.IndexReader} to read the documents from.
   * @param idField field containing the id. May be null.
   * @param field  field to use for the Vector
   * @param mapper {@link VectorMapper} for creating {@link Vector}s from Lucene's TermVectors.
   * @param normPower the normalization value. Must be nonnegative, or {@link #NO_NORMALIZING}
   */
  public LuceneIterable(IndexReader indexReader, String idField, String field, VectorMapper mapper, double normPower, double maxPercentErrorDocs) {
    this.indexReader = indexReader;
    this.idField = idField;
    this.field = field;
    this.mapper = mapper;
    this.normPower = normPower;
    this.maxPercentErrorDocs = maxPercentErrorDocs;
  }
  
  @Override
  public Iterator<Vector> iterator() {
    try {
      return new LuceneIterator(indexReader, idField, field, mapper, normPower, maxPercentErrorDocs);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
