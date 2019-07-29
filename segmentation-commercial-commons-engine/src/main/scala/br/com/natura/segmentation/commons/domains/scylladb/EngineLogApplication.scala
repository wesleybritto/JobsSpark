package br.com.natura.segmentation.commons.domains.scylladb

import java.sql.{Timestamp, Date}

case class EngineLogApplication (
  application_id: String,
  history_date: Date,
  hour_range: Int,
  engine_id: Int,
  uuid: String,
  payload: Option[String] = None,
  process_date: Timestamp
)