package br.com.natura.segmentation.commons.domains.postgresql

case class PersonRelation (
  country: Int,
  company: Int,
  business_model: Int,
  structure_level: Int,
  structure_code: Int,
  person_code: Int,
  person_id: String,
  operational_cycle: Int
)