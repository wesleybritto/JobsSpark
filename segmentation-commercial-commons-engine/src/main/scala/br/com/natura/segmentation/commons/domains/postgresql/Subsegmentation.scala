package br.com.natura.segmentation.commons.domains.postgresql

case class Subsegmentation (
  country: Int,
  company: Int,
  business_model: Int,
  segmentation_uid: String,
  segmentation_id: Int,
  subsegmentation_uid: String,
  subsegmentation_id: Int,
  index_code: Option[Int],
  index_value: Option[Double],
  subsegmentation_function: String
)