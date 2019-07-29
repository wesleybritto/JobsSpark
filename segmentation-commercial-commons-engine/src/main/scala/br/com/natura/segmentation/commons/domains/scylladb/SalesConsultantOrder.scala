package br.com.natura.segmentation.commons.domains.scylladb

import java.sql.Timestamp

case class SalesConsultantOrder (
  country_code: String,
  company_id: Int,
  business_model_id: Option[Int],
  person_id: String,
  order_number: Int,
  order_uid: String,
  order_date: Timestamp,
  order_cycle: Int,
  status_id: Option[Int]
)


object SalesConsultantOrder {
  def empty: SalesConsultantOrder = {
    SalesConsultantOrder (
      country_code = "",
      company_id = 0,
      business_model_id = Some(0),
      person_id = "",
      order_number = 0,
      order_uid = "",
      order_date =  java.sql.Timestamp.valueOf("1900-01-01 00:00:00.0"),
      order_cycle = 0,
      status_id = Some(0)
    )
  }
}