package br.com.natura.segmentation.commons.domains.scylladb

import java.sql.Timestamp

case class DebitTitles (

company_id: Int,
country_code: String,
business_model_id: Int,
person_id: String,
order_uid: String,
due_date: Option[Timestamp],
paid: Boolean
                       )


object DebitTitles {
def empty: DebitTitles = {
  DebitTitles(
    company_id = 0,
    country_code = "",
    business_model_id = 0,
    person_id = "",
    order_uid = "",
    due_date = Some(java.sql.Timestamp.valueOf("3000-01-01 00:00:00.0")),
    paid = true
  )
}
}