package edu.gmu.stc.vector

import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform

/**
  * Created by Fei Hu on 3/24/18.
  */
object VectorUtil {

  def getCRSTransform(sourceCRSStr: String,
                      sourceLongitudeFirst: Boolean,
                      targetCRSStr: String,
                      targetLongitudeFirst: Boolean): MathTransform = {

    val sourceCRS = CRS.decode(sourceCRSStr, sourceLongitudeFirst)
    val targetCRS = CRS.decode(targetCRSStr, targetLongitudeFirst)
    CRS.findMathTransform(sourceCRS, targetCRS)
  }

  def transformCRS(inputGeometry: Geometry,
                   sourceCRSStr: String,
                   sourceLongitudeFirst: Boolean,
                   targetCRSStr: String,
                   targetLongitudeFirst: Boolean): Geometry = {
    val transform = getCRSTransform(sourceCRSStr, sourceLongitudeFirst, targetCRSStr, targetLongitudeFirst)
    JTS.transform(inputGeometry, transform)
  }

  def transformCRS(inputGeometry: Envelope,
                   sourceCRSStr: String,
                   sourceLongitudeFirst: Boolean,
                   targetCRSStr: String,
                   targetLongitudeFirst: Boolean): Envelope = {
    val transform = getCRSTransform(sourceCRSStr, sourceLongitudeFirst, targetCRSStr, targetLongitudeFirst)
    JTS.transform(inputGeometry, transform)
  }


}
