package org.apache.mahout.math.hadoop.stochasticsvd;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Varint;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class SparseRowBlockWritable implements Writable {

  private long rowIndices[];
  private Vector[] rows;

  public long[] getRowIndices() {
    return rowIndices;
  }

  public void setRowIndices(long[] rowIndices) {
    this.rowIndices = rowIndices;
  }

  public Vector[] getRows() {
    return rows;
  }

  public void setRows(Vector[] rows) {
    this.rows = rows;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int len = Varint.readUnsignedVarInt(in);
    if (rows == null || rows.length != len) {
      rows = new Vector[len];
      rowIndices = new long[len];
    }
    VectorWritable vw = new VectorWritable();
    for (int i = 0; i < len; i++) {
      rowIndices[i] = Varint.readUnsignedVarLong(in);
      vw.readFields(in);
      rows[i] = vw.get().clone();
    }

  }

  @Override
  public void write(DataOutput out) throws IOException {
    Varint.writeUnsignedVarInt(rows.length, out);
    VectorWritable vw = new VectorWritable();
    for (int i = 0; i < rows.length; i++) {
      Varint.writeUnsignedVarLong(rowIndices[i], out);
      vw.set(rows[i]);
      vw.write(out);
    }
  }

}
