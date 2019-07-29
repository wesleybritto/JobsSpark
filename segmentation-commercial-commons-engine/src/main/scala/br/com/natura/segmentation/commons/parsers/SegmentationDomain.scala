package br.com.natura.segmentation.commons.parsers

import java.sql.{ResultSet, Timestamp, Date => SQLDate}
import java.util.{Date, UUID}

import br.com.natura.segmentation.commons.domains.application._
import br.com.natura.segmentation.commons.domains.postgresql._
import br.com.natura.segmentation.commons.domains.scylladb._
import com.datastax.spark.connector.CassandraRow
import org.apache.spark.sql.Row

import scala.util.Try

object SegmentationDomain {
  def parse2PersonRelation(r: ResultSet, s: Structure): PersonRelation = {
    PersonRelation (
      country = r.getInt("country"),
      company = r.getInt("company"),
      business_model = r.getInt("business_model"),
      structure_level = r.getInt("structure_level"),
      structure_code = r.getInt("structure_code"),
      person_code = r.getInt("person_code"),
      person_id = r.getString("person_uid"),
      operational_cycle = s.operational_cycle
    )
  }

  def parse2Subsegmentation(r: ResultSet): Subsegmentation = {
    Subsegmentation (
      country = r.getInt("country"),
      company = r.getInt("company"),
      business_model = r.getInt("business_model"),
      segmentation_uid = r.getString("segmentation_uid"),
      segmentation_id = r.getInt("segmentation_id"),
      subsegmentation_uid = r.getString("subsegmentation_uid"),
      subsegmentation_id = r.getInt("subsegmentation_id"),
      index_code = Some(r.getInt("index_code")),
      index_value = Some(r.getInt("index_value")),
      subsegmentation_function = r.getString("subsegmentation_function")
    )
  }

  def parse2PersonSubsegmentation(c: Option[CassandraRow]): Option[PersonSubsegmentation] = {
    if(c.nonEmpty) {
      val r = c.get
      val person = PersonSubsegmentation (
        country = r.getInt("country"),
        company = r.getInt("company"),
        business_model = r.getInt("business_model"),
        person_id = r.getUUID("person_id").toString,
        subsegmentation_id = r.getInt("subsegmentation_id"),
        status = r.getBoolean("status"),
        row_uid = r.getUUID("row_uid").toString,
        created_at = new Timestamp(r.getDateTime("created_at").getMillis),
        created_by = r.getString("created_by")
      )

      Some(person)
    } else {
      None
    }
  }

  def parse2PersonSubsegmentation(r: Row): Option[PersonSubsegmentation] = {
    if(r == null || r != null && r.isNullAt(0)) {
      None
    } else {
      val person = PersonSubsegmentation (
        country = r.getInt(r.fieldIndex("country")),
        company = r.getInt(r.fieldIndex("company")),
        business_model = r.getInt(r.fieldIndex("business_model")),
        person_id = r.getString(r.fieldIndex("person_id")),
        subsegmentation_id = r.getInt(r.fieldIndex("subsegmentation_id")),
        status = r.getBoolean(r.fieldIndex("status")),
        row_uid = r.getString(r.fieldIndex("row_uid")),
        created_at = r.getTimestamp(r.fieldIndex("created_at")),
        created_by = r.getString(r.fieldIndex("created_by"))
      )

      Some(person)
    }
  }

  def parse2PersonSubsegmentation(c: ConsultantSegmentation, status: Boolean, created_by: String): PersonSubsegmentation = {
    PersonSubsegmentation (
      country = c.country,
      company = c.company,
      business_model = c.business_model,
      person_id = c.person_id,
      subsegmentation_id = c.subsegmentation_id,
      status = status,
      row_uid = UUID.randomUUID().toString,
      created_at = new Timestamp(new Date().getTime),
      created_by = created_by.toUpperCase()
    )
  }

  def parse2ConsultantIndex(r: CassandraRow): ConsultantIndex = {
    ConsultantIndex(
      index_code = r.getInt("index_code"),
      index_value = r.getInt("index_value"),
      operational_cycle = r.getInt("operational_cycle")
    )
  }

  def parse2ConsultantIndex(r: Row): ConsultantIndex = {
    ConsultantIndex(
      index_code = r.getInt(r.fieldIndex("index_code")),
      index_value = r.getDouble(r.fieldIndex("index_value")),
      operational_cycle = r.getInt(r.fieldIndex("operational_cycle"))
    )
  }

  def parse2PersonSubsegmentationResult(c: ConsultantSegmentation,
                                        s: Option[PersonSubsegmentation],
                                        result: Boolean): PersonSubsegmentationResult = {
    PersonSubsegmentationResult(
      consultant = c,
      currentSubsegmentation = s,
      result = result
    )
  }

  def parse2ConsultantSegmentation(r: Row): ConsultantSegmentation = {
    val index_code = Try(Some(r.getInt(r.fieldIndex("index_code")))).getOrElse(None)
    val index_value = Try(Some(r.getDouble(r.fieldIndex("index_value")))).getOrElse(None)


    ConsultantSegmentation(
      country = r.getInt(r.fieldIndex("country")),
      company = r.getInt(r.fieldIndex("company")),
      business_model = r.getInt(r.fieldIndex("business_model")),
      person_id = r.getString(r.fieldIndex("person_id")),
      subsegmentation_uid = r.getString(r.fieldIndex("subsegmentation_uid")),
      subsegmentation_id = r.getInt(r.fieldIndex("subsegmentation_id")),
      subsegmentation_function = r.getString(r.fieldIndex("subsegmentation_function")),
      segmentation_uid = r.getString(r.fieldIndex("segmentation_uid")),
      segmentation_id = r.getInt(r.fieldIndex("segmentation_id")),
      operational_cycle = r.getInt(r.fieldIndex("operational_cycle")),
      person_code = r.getInt(r.fieldIndex("person_code")),
      structure_level = r.getInt(r.fieldIndex("structure_level")),
      structure_code = r.getInt(r.fieldIndex("structure_code")),
      index_code = index_code,
      index_value = index_value,
      country_code = r.getString(r.fieldIndex("country_code"))

    )
  }

  def parse2PersonId(r: CassandraRow): PersonId = {

    val business_model_id = Try(Some(r.getInt("business_model_id"))).getOrElse(None)
    val registration_substatus_id = Try(Some(r.getInt("registration_substatus_id"))).getOrElse(None)

    PersonId(
      country = r.getInt("country_id"),
      business_model = business_model_id,//r.getInt("business_model_id"),
      person_id = r.getUUID("person_id").toString,
      birthday = new SQLDate(r.getDate("birthday").getTime),
      gender_id = r.getInt("gender_id"),
      registration_status_id = r.getInt("registration_status_id"),
      registration_substatus_id = registration_substatus_id//r.getInt("registration_substatus_id")
    )
  }

  def parse2PersonId(r: Row): PersonId = {

    val business_model_id = Try(Some(r.getInt(r.fieldIndex("business_model_id")))).getOrElse(None)
    val registration_substatus_id = Try(Some(r.getInt(r.fieldIndex("registration_substatus_id")))).getOrElse(None)


    PersonId(
      country = r.getInt(r.fieldIndex("country")),
      business_model = business_model_id,//r.getInt(r.fieldIndex("business_model")),
      person_id = r.getString(r.fieldIndex("person_id")),
      birthday = r.getDate(r.fieldIndex("birthday")),
      gender_id = r.getInt(r.fieldIndex("gender_id")),
      registration_status_id = r.getInt(r.fieldIndex("registration_status_id")),
      registration_substatus_id = registration_substatus_id//r.getInt(r.fieldIndex("registration_substatus_id"))
    )
  }

  def parse2SalesConsultantOrder(r: Option[CassandraRow]): Option[SalesConsultantOrder] = {

    if(r.nonEmpty) {
      val o = r.get

      val business_model_id = Try(Some(o.getInt("business_model_id"))).getOrElse(None)
      val status_id = Try(Some(o.getInt("status_id"))).getOrElse(None)

      Some(SalesConsultantOrder(
        country_code = o.getString("country_code"),
        company_id = o.getInt("company_id"),
        business_model_id = business_model_id,//o.getInt("business_model_id"),
        person_id = o.getUUID("person_id").toString,
        order_number = o.getInt("order_number"),
        order_uid = o.getUUID("order_uid").toString,
        order_date = new Timestamp(o.getDateTime("order_date").getMillis),
        order_cycle = o.getInt("order_cycle"),
        status_id = status_id//o.getInt("status_id")
      ))
    } else {
      None
    }
  }


  def parse2OnlySalesConsultantOrder(r: CassandraRow): SalesConsultantOrder = {

    val business_model_id = Try(Some(r.getInt("business_model_id"))).getOrElse(None)
    val status_id = Try(Some(r.getInt("status_id"))).getOrElse(None)

      SalesConsultantOrder(
        country_code = r.getString("country_code"),
        company_id = r.getInt("company_id"),
        business_model_id = business_model_id,
        person_id = r.getUUID("person_id").toString,
        order_number = r.getInt("order_number"),
        order_uid = r.getUUID("order_uid").toString,
        order_date = new Timestamp(r.getDateTime("order_date").getMillis),
        order_cycle = r.getInt("order_cycle"),
        status_id = status_id//r.getInt("status_id")
      )

  }

  def parse2SalesConsultantOrder(r: Row): SalesConsultantOrder = {
    val business_model_id = Try(Some(r.getInt(r.fieldIndex("business_model_id")))).getOrElse(None)
    val status_id = Try(Some(r.getInt(r.fieldIndex("status_id")))).getOrElse(None)

    SalesConsultantOrder(
      country_code = r.getString(r.fieldIndex("country_code")),
      company_id = r.getInt(r.fieldIndex("company_id")),
      business_model_id =  business_model_id,
      person_id = r.getString(r.fieldIndex("person_id")),
      order_number = r.getInt(r.fieldIndex("order_number")),
      order_uid = r.getString(r.fieldIndex("order_uid")),
      order_date = r.getTimestamp(r.fieldIndex("order_date")),
      order_cycle = r.getInt(r.fieldIndex("order_cycle")),
      status_id = status_id//r.getInt(r.fieldIndex("status_id"))
    )
  }

  def parse2Cycles(r: Option[CassandraRow]): Option[Cycles] = {

    if(r.nonEmpty) {
      val o = r.get

    Some(Cycles(
      last_cycles = o.getInt("last_cycles"),
      rank_id = o.getInt("rank_id")
    ))
    } else {
      None
    }
  }

 def parse2Cycles(r:Row): Cycles = {

      Cycles (
        last_cycles = r.getInt(r.fieldIndex("last_cycles")),
        rank_id = r.getInt(r.fieldIndex("rank_id"))
      )
  }


  def parse2DebitTitles(r: CassandraRow): DebitTitles = {
    val due_date = Try(Some(new Timestamp(r.getDateTime("due_date").getMillis))).getOrElse(None)//r.getInt(r.fieldIndex("due_date")))).getOrElse(None)

    DebitTitles(
      company_id = r.getInt("company_id"),
      country_code = r.getString("country_code"),
      business_model_id = r.getInt("business_model_id"),
      person_id = r.getString("person_id"),
      order_uid = r.getString("order_uid"),
      due_date = due_date,//new Timestamp(r.getDateTime("due_date").getMillis),
      paid = r.getBoolean("paid")
    )
  }


  def parse2DebitTitles(r: Option[CassandraRow]): Option[DebitTitles] = {



    if (r.nonEmpty) {
      val o = r.get

      val due_date = Try(Some(new Timestamp(o.getDateTime("due_date").getMillis))).getOrElse(None)

      Some(DebitTitles(
        company_id = o.getInt("company_id"),
        country_code = o.getString("country_code"),
        business_model_id = o.getInt("business_model_id"),
        person_id = o.getString("person_id"),
        order_uid = o.getString("order_uid"),
        due_date = due_date,//new Timestamp(o.getDateTime("due_date").getMillis),
        paid = o.getBoolean("paid")
      ))
    }
    else{
      None
    }

  }

  def parse2DebitTitles(r: Row): DebitTitles = {
    val due_date = Try(Some(r.getTimestamp(r.fieldIndex("due_date")))).getOrElse(None)

    DebitTitles(
      company_id = r.getInt(r.fieldIndex("company_id")),
      country_code = r.getString(r.fieldIndex("country_code")),
      business_model_id = r.getInt(r.fieldIndex("business_model_id")),
      person_id = r.getString(r.fieldIndex("person_id")),
      order_uid = r.getString(r.fieldIndex("order_uid")),
      due_date = due_date,//r.getTimestamp(r.fieldIndex("due_date")),
      paid = r.getBoolean(r.fieldIndex("paid"))
    )
  }


  def parse2StartCycle(r:Row): Option[FirstCycle] = {

    if (r == null || r != null && r.isNullAt(0)) {
      Some(FirstCycle.empty)
    } else {
      val startCycle = FirstCycle(
        person_code = r.getInt(r.fieldIndex("person_code")),
        first_cycle = r.getInt(r.fieldIndex("first_cycle"))
      )
      Some(startCycle)
    }
  }
}