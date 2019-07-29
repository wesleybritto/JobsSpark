package br.com.natura.segmentation.commons.domains.scylladb

import java.sql.Date

case class PersonId (
                      country: Int,
                      business_model: Option[Int],
                      person_id: String,
                      birthday: Date,
                      gender_id: Int,
                      registration_status_id: Int,
                      registration_substatus_id: Option[Int]
)