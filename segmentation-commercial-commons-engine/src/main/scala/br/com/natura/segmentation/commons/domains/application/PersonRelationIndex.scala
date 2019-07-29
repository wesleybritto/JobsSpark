package br.com.natura.segmentation.commons.domains.application

import br.com.natura.segmentation.commons.domains.scylladb.ConsultantIndex

case class PersonRelationIndex (
  country: Int,
  company: Int,
  business_model: Int,
  structure_level: Int,
  structure_code: Int,
  person_code: Int,
  person_id: String,
  operational_cycle: Int,
  consultant_index: ConsultantIndex
)