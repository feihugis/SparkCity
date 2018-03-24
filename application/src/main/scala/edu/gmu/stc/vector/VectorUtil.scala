package edu.gmu.stc.vector

import org.geotools.referencing.CRS
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform

/**
  * Created by Fei Hu on 3/24/18.
  */
object VectorUtil {

  def getCRSTransform(sourceCRSStr: String,
                      targetCRSStr: String,
                      longitudeFirst: Boolean): MathTransform = {

    val sourceCRS = CRS.decode(sourceCRSStr, longitudeFirst)
    val targetCRS = CRS.decode(targetCRSStr, longitudeFirst)
    CRS.findMathTransform(sourceCRS, targetCRS)
  }


}
