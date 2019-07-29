package br.com.natura.segmentation.commons.domains.scylladb

import java.sql.Timestamp

case class PersonSubsegmentation (
  country: Int,
  company: Int,
  business_model: Int,
  person_id: String,
  subsegmentation_id: Int,
  status: Boolean,
  row_uid: String,
  created_at: Timestamp,
  created_by: String
)

object PersonSubsegmentation {
  def empty: PersonSubsegmentation = {
    PersonSubsegmentation (
      country = 0,
      company = 0,
      business_model = 0,
      person_id = "",
      subsegmentation_id = 0,
      status = false,
      row_uid = "",
      created_at = null,
      created_by = ""
    )
  }
}