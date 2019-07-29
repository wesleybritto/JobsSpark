package br.com.natura.segmentation.commons.domains.application


case class ConsultantSegmentation (
  country: Int,
  company: Int,
  business_model: Int,
  person_id: String,
  subsegmentation_uid: String,
  subsegmentation_id: Int,
  subsegmentation_function: String,
  segmentation_uid: String,
  segmentation_id: Int,
  operational_cycle: Int,
  person_code: Int,
  structure_level: Int,
  structure_code: Int,
  index_code: Option[Int],
  index_value: Option[Double],
  country_code: String
)
