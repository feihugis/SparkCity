package edu.gmu.stc.vector.osm

import scala.collection.immutable.HashSet

/**
  * Created by Fei Hu on 3/23/18.
  */
object FeatureClass {

  val landuse_RedFeatures: HashSet[String] = HashSet("residential", "industrial", "commercial",
    "recreation_ground", "retail", "military", "quarry", "heath", "brownfield", "construction",
    "railway")

  val landuse_GreenFeatures: HashSet[String] = HashSet("forest", "park", "cemetery", "allotments",
    "meadow", "nature_reserve", "orchard", "vineyard", "scrub", "grass", "national_park", "basin",
    "village_green", "plant_nursery", "greenfield", "farm", "farmland", "farmyard")

  def isRedFeature(feature: String): Boolean = landuse_RedFeatures.contains(feature.toLowerCase())
}
