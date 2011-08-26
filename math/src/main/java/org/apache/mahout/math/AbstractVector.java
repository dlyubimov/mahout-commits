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

package org.apache.mahout.math;

import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.function.DoubleDoubleFunction;
import org.apache.mahout.math.function.DoubleFunction;

import java.util.Iterator;

/** Implementations of generic capabilities like sum of elements and dot products */
public abstract class AbstractVector implements Vector {

  private int size;
  protected double lengthSquared = -1.0;

  protected AbstractVector(int size) {
    this.size = size;
  }

  @Override
  public double aggregate(DoubleDoubleFunction aggregator, DoubleFunction map) {
    if (size < 1) {
      throw new IllegalArgumentException("Cannot aggregate empty vector");
    }
    double result = map.apply(getQuick(0));
    for (int i = 1; i < size; i++) {
      result = aggregator.apply(result, map.apply(getQuick(i)));
    }
    return result;
  }

  @Override
  public double aggregate(Vector other, DoubleDoubleFunction aggregator, DoubleDoubleFunction combiner) {
    if (size < 1) {
      throw new IllegalArgumentException("Cannot aggregate empty vector");
    }
    double result = combiner.apply(getQuick(0), other.getQuick(0));
    for (int i = 1; i < size; i++) {
      result = aggregator.apply(result, combiner.apply(getQuick(i), other.getQuick(i)));
    }
    return result;
  }

  /**
   * Subclasses must override to return an appropriately sparse or dense result
   *
   * @param rows    the row cardinality
   * @param columns the column cardinality
   * @return a Matrix
   */
  protected abstract Matrix matrixLike(int rows, int columns);

  @Override
  public Vector viewPart(int offset, int length) {
    if (offset < 0) {
      throw new IndexException(offset, size);
    }
    if (offset + length > size) {
      throw new IndexException(offset + length, size);
    }
    return new VectorView(this, offset, length);
  }

  @Override
  public Vector clone() {
    try {
      AbstractVector r = (AbstractVector) super.clone();
      r.size = size;
      r.lengthSquared = lengthSquared;
      return r;
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException("Can't happen");
    }
  }

  @Override
  public Vector divide(double x) {
    if (x == 1.0) {
      return like().assign(this);
    }
    Vector result = like().assign(this);
    Iterator<Element> iter = result.iterateNonZero();
    while (iter.hasNext()) {
      Element element = iter.next();
      element.set(element.get() / x);
    }
    return result;
  }

  @Override
  public double dot(Vector x) {
    if (size != x.size()) {
      throw new CardinalityException(size, x.size());
    }
    if (this == x) {
      return dotSelf();
    }
    double result = 0.0;
    Iterator<Element> iter = iterateNonZero();
    while (iter.hasNext()) {
      Element element = iter.next();
      result += element.get() * x.getQuick(element.index());
    }
    return result;
  }
  
  public double dotSelf() {
    double result = 0.0;
    Iterator<Element> iter = iterateNonZero();
    while (iter.hasNext()) {
      double value = iter.next().get();
      result += value * value;
    }
    return result;
  }

  @Override
  public double get(int index) {
    if (index < 0 || index >= size) {
      throw new IndexException(index, size);
    }
    return getQuick(index);
  }

  @Override
  public Element getElement(int index) {
    return new LocalElement(index);
  }

  @Override
  public Vector minus(Vector that) {
    if (size != that.size()) {
      throw new CardinalityException(size, that.size());
    }

    // TODO: check the numNonDefault elements to further optimize
    Vector result = like().assign(this);
    Iterator<Element> iter = that.iterateNonZero();
    while (iter.hasNext()) {
      Element thatElement = iter.next();
      int index = thatElement.index();
      result.setQuick(index, this.getQuick(index) - thatElement.get());
    }
    return result;
  }

  @Override
  public Vector normalize() {
    return divide(Math.sqrt(dotSelf()));
  }

  @Override
  public Vector normalize(double power) {
    return divide(norm(power));
  }
  
  @Override
  public Vector logNormalize() {
    return logNormalize(2.0, Math.sqrt(dotSelf()));
  }
  
  @Override
  public Vector logNormalize(double power) {
    return logNormalize(power, norm(power));
  }
  
  public Vector logNormalize(double power, double normLength) {   
    // we can special case certain powers
    if (Double.isInfinite(power) || power <= 1.0) {
      throw new IllegalArgumentException("Power must be > 1 and < infinity");
    } else {
      double denominator = normLength * Math.log(power);
      Vector result = like().assign(this);
      Iterator<Element> iter = result.iterateNonZero();
      while (iter.hasNext()) {
        Element element = iter.next();
        element.set(Math.log(1 + element.get()) / denominator);
      }
      return result;
    }
  }

  @Override
  public double norm(double power) {
    if (power < 0.0) {
      throw new IllegalArgumentException("Power must be >= 0");
    }
    // we can special case certain powers
    if (Double.isInfinite(power)) {
      double val = 0.0;
      Iterator<Element> iter = this.iterateNonZero();
      while (iter.hasNext()) {
        val = Math.max(val, Math.abs(iter.next().get()));
      }
      return val;
    } else if (power == 2.0) {
      return Math.sqrt(dotSelf());
    } else if (power == 1.0) {
      double val = 0.0;
      Iterator<Element> iter = this.iterateNonZero();
      while (iter.hasNext()) {
        val += Math.abs(iter.next().get());
      }
      return val;
    } else if (power == 0.0) {
      // this is the number of non-zero elements
      double val = 0.0;
      Iterator<Element> iter = this.iterateNonZero();
      while (iter.hasNext()) {
        val += iter.next().get() == 0 ? 0 : 1;
      }
      return val;
    } else {
      double val = 0.0;
      Iterator<Element> iter = this.iterateNonZero();
      while (iter.hasNext()) {
        Element element = iter.next();
        val += Math.pow(element.get(), power);
      }
      return Math.pow(val, 1.0 / power);
    }
  }

  @Override
  public double getLengthSquared() {
    if (lengthSquared >= 0.0) {
      return lengthSquared;
    }
    return lengthSquared = dotSelf();
  }

  @Override
  public double getDistanceSquared(Vector v) {
    if (size != v.size()) {
      throw new CardinalityException(size, v.size());
    }
    // if this and v has a cached lengthSquared, dot product is quickest way to compute this.
    if (lengthSquared >= 0 && v instanceof AbstractVector && ((AbstractVector)v).lengthSquared >= 0) {
      return lengthSquared + v.getLengthSquared() - 2 * this.dot(v);
    }
    Vector randomlyAccessed;
    Iterator<Element> it;
    double d = 0.0;
    if (lengthSquared >= 0.0) {
      it = v.iterateNonZero();
      randomlyAccessed = this;
      d += lengthSquared;
    } else { // TODO: could be further optimized, figure out which one is smaller, etc
      it = iterateNonZero();
      randomlyAccessed = v;
      d += v.getLengthSquared();
    }
    while (it.hasNext()) {
      Element e = it.next();
      double value = e.get();
      d += value * (value - 2.0 * randomlyAccessed.getQuick(e.index()));
    }
    //assert d > -1.0e-9; // round-off errors should never be too far off!
    return Math.abs(d);
  }

  @Override
  public double maxValue() {
    double result = Double.NEGATIVE_INFINITY;
    int nonZeroElements = 0;
    Iterator<Element> iter = this.iterateNonZero();
    while (iter.hasNext()) {
      nonZeroElements++;
      Element element = iter.next();
      result = Math.max(result, element.get());
    }
    if (nonZeroElements < size) {
      return Math.max(result, 0.0);
    }
    return result;
  }
  
  @Override
  public int maxValueIndex() {
    int result = -1;
    double max = Double.NEGATIVE_INFINITY;
    int nonZeroElements = 0;
    Iterator<Element> iter = this.iterateNonZero();
    while (iter.hasNext()) {
      nonZeroElements++;
      Element element = iter.next();
      double tmp = element.get();
      if (tmp > max) {
        max = tmp;
        result = element.index();
      }
    }
    // if the maxElement is negative and the vector is sparse then any
    // unfilled element(0.0) could be the maxValue hence we need to
    // find one of those elements
    if (nonZeroElements < size && max < 0.0) {
      for (Element element : this) {
        if (element.get() == 0.0) {
          return element.index();
        }
      }
    }
    return result;
  }

  @Override
  public double minValue() {
    double result = Double.POSITIVE_INFINITY;
    int nonZeroElements = 0;
    Iterator<Element> iter = this.iterateNonZero();
    while (iter.hasNext()) {
      nonZeroElements++;
      Element element = iter.next();
      result = Math.min(result, element.get());
    }
    if (nonZeroElements < size) {
      return Math.min(result, 0.0);
    }
    return result;
  }

  @Override
  public int minValueIndex() {
    int result = -1;
    double min = Double.POSITIVE_INFINITY;
    int nonZeroElements = 0;
    Iterator<Element> iter = this.iterateNonZero();
    while (iter.hasNext()) {
      nonZeroElements++;
      Element element = iter.next();
      double tmp = element.get();
      if (tmp < min) {
        min = tmp;
        result = element.index();
      }
    }
    // if the maxElement is positive and the vector is sparse then any
    // unfilled element(0.0) could be the maxValue hence we need to
    // find one of those elements
    if (nonZeroElements < size && min > 0.0) {
      for (Element element : this) {
        if (element.get() == 0.0) {
          return element.index();
        }
      }
    }
    return result;
  }

  @Override
  public Vector plus(double x) {
    Vector result = like().assign(this);
    if (x == 0.0) {
      return result;
    }
    int size = result.size();
    for (int i = 0; i < size; i++) {
      result.setQuick(i, getQuick(i) + x);
    }
    return result;
  }

  @Override
  public Vector plus(Vector x) {
    if (size != x.size()) {
      throw new CardinalityException(size, x.size());
    }

    // prefer to have this be the denser than x
    if (!isDense() && (x.isDense() || x.getNumNondefaultElements() > this.getNumNondefaultElements())) {
      return x.plus(this);
    }

    Vector result = like().assign(this);
    Iterator<Element> iter = x.iterateNonZero();
    while (iter.hasNext()) {
      Element e = iter.next();
      int index = e.index();
      result.setQuick(index, this.getQuick(index) + e.get());
    }
    return result;
  }

  @Override
  public void set(int index, double value) {
    if (index < 0 || index >= size) {
      throw new IndexException(index, size);
    }
    setQuick(index, value);
  }

  @Override
  public Vector times(double x) {
    if (x == 0.0) {
      return like();
    }

    Vector result = like().assign(this);
    if (x == 1.0) {
      return result;
    }
    
    Iterator<Element> iter = result.iterateNonZero();
    while (iter.hasNext()) {
      Element element = iter.next();
      element.set(element.get() * x);
    }

    return result;
  }

  @Override
  public Vector times(Vector x) {
    if (size != x.size()) {
      throw new CardinalityException(size, x.size());
    }

    Vector to = this;
    Vector from = x;
    // Clone and edit to the sparse one; if both are sparse, edit the more sparse one (more zeroes)
    if (isDense() || (!x.isDense() && getNumNondefaultElements() > x.getNumNondefaultElements())) {
      to = x;
      from = this;
    }

    Vector result = to.like().assign(to);
    Iterator<Element> iter = result.iterateNonZero();
    while (iter.hasNext()) {
      Element element = iter.next();
      element.set(element.get() * from.getQuick(element.index()));
    }

    return result;
  }

  @Override
  public double zSum() {
    double result = 0.0;
    Iterator<Element> iter = iterateNonZero();
    while (iter.hasNext()) {
      result += iter.next().get();
    }

    return result;
  }

  @Override
  public Vector assign(double value) {
    for (int i = 0; i < size; i++) {
      setQuick(i, value);
    }
    return this;
  }

  @Override
  public Vector assign(double[] values) {
    if (size != values.length) {
      throw new CardinalityException(size, values.length);
    }
    for (int i = 0; i < size; i++) {
      setQuick(i, values[i]);
    }
    return this;
  }

  @Override
  public Vector assign(Vector other) {
    if (size != other.size()) {
      throw new CardinalityException(size, other.size());
    }
    for (int i = 0; i < size; i++) {
      setQuick(i, other.getQuick(i));
    }
    return this;
  }

  @Override
  public Vector assign(DoubleDoubleFunction f, double y) {
    Iterator<Element> it = f.apply(0, y) == 0 ? iterateNonZero() : iterator();
    while (it.hasNext()) {
      Element e = it.next();
      e.set(f.apply(e.get(), y));
    }
    return this;
  }

  @Override
  public Vector assign(DoubleFunction function) {
    Iterator<Element> it = function.apply(0) == 0 ? iterateNonZero() : iterator();
    while (it.hasNext()) {
      Element e = it.next();
      e.set(function.apply(e.get()));
    }
    return this;
  }

  @Override
  public Vector assign(Vector other, DoubleDoubleFunction function) {
    if (size != other.size()) {
      throw new CardinalityException(size, other.size());
    }
    for (int i = 0; i < size; i++) {
      setQuick(i, function.apply(getQuick(i), other.getQuick(i)));
    }
    return this;
  }

  @Override
  public Matrix cross(Vector other) {
    Matrix result = matrixLike(size, other.size());
    for (int row = 0; row < size; row++) {
      result.assignRow(row, other.times(getQuick(row)));
    }
    return result;
  }

  @Override
  public final int size() {
    return size;  
  }

  @Override
  public String asFormatString() {
    return toString();
  }

  @Override
  public int hashCode() {
    int result = size;
    Iterator<Element> iter = iterateNonZero();
    while (iter.hasNext()) {
      Element ele = iter.next();
      result += ele.index() * RandomUtils.hashDouble(ele.get());
    }
    return result;
  }

  /**
   * Determines whether this {@link Vector} represents the same logical vector as another
   * object. Two {@link Vector}s are equal (regardless of implementation) if the value at
   * each index is the same, and the cardinalities are the same.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Vector)) {
      return false;
    }
    Vector that = (Vector) o;
    if (size != that.size()) {
      return false;
    }
    for (int index = 0; index < size; index++) {
      if (getQuick(index) != that.getQuick(index)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append('{');
    for (int index = 0; index < size; index++) {
      double value = getQuick(index);
      if (value != 0.0) {
        result.append(index);
        result.append(':');
        result.append(value);
        result.append(',');
      }
    }
    if (result.length() > 1) {
      result.setCharAt(result.length() - 1, '}');
    } else {
      result.append('}');
    }
    return result.toString();
  }


  protected final class LocalElement implements Element {
    int index;

    LocalElement(int index) {
      this.index = index;
    }

    @Override
    public double get() {
      return getQuick(index);
    }

    @Override
    public int index() {
      return index;
    }

    @Override
    public void set(double value) {
      setQuick(index, value);
    }
  }
}
