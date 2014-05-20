package org.apache.mahout.sparkbindings.blas

import org.apache.mahout.sparkbindings.drm.DrmRddInput

object MapBlock {

  def exec[S, R](src: DrmRddInput[S]): DrmRddInput[R] = {

    // We can't use attributes to avoid putting the whole this into closure.
    val bmf = this.bmf
    val ncol = this.ncol

    val rdd = src.toBlockifiedDrmRdd()
        .map(blockTuple => {
      val out = bmf(blockTuple)

      assert(out._2.nrow == blockTuple._2.nrow, "block mapping must return same number of rows.")
      assert(out._2.ncol == ncol, "block map must return %d number of columns.".format(ncol))

      out
    })
    new DrmRddInput(blockifiedSrc = Some(rdd))
  }

}
