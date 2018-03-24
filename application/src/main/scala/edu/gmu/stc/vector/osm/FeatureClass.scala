package edu.gmu.stc.vector.osm

import scala.collection.immutable.HashSet

/**
  * Created by Fei Hu on 3/23/18.
  */
class FeatureClassification {

  val landuse_RedFeatures: HashSet[String] = HashSet[String]("residential", "industrial", "commercial", "recreation_ground", "retail", "military", "quarry", "heath", "brownfield", "construction", "railway", )
  val landuse_GreenFeatures: HashSet[String] = HashSet[String]("forest", "park", "cemetery", "allotments", "meadow", "nature_reserve", "orchard", "vineyard", "scrub", "grass", "national_park", "basin", "village_green", "plant_nursery", "greenfield", "farmland", "farmyard")

  def isRedFeature(feature: String): Boolean = landuse_RedFeatures.contains(feature.toLowerCase())




}
