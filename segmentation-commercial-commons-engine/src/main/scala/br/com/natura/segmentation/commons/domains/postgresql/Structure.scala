package br.com.natura.segmentation.commons.domains.postgresql

case class Structure(
  country: Int,
  company: Int,
  business_model: Int,
  structure_level: Int,
  structure_code: Int,
  structure_name: String,
  operational_cycle: Int,
  parent_structure_level: Int,
  parent_structure_code: Int,
  parent_structure_name: String
)