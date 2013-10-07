package org.apache.mahout.sparkbindings.drm


/**
 * Some operations (like transpose) that are only applicable to DRM with Int keys.
 *
 * @author dmitriy
 */
class IntIndexedRowsDRMOps(val drm: DRM[Int]) {


  def t: DRM[Int] =
    if (drm.isInstanceOf[TransposedDRM])
      // (A')' == A
      drm.asInstanceOf[TransposedDRM].transposee
    else
      new TransposedDRM(drm)


}
