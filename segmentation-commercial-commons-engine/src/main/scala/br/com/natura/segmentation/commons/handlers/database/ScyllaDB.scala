package br.com.natura.segmentation.commons.handlers.database

import br.com.natura.segmentation.commons.domains.scylladb.{EngineLogApplication, PersonSubsegmentation}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object ScyllaDB {
  val sparkCassandra: String = "org.apache.spark.sql.cassandra"

  val ksMasterdata = "masterdata"
  val ksPerformance = "performance"
  val ksSegmentation = "segmentation"
  val ksApplicationLog = "applicationlog"

  val tbConsultantIndex = "consultant_index"
  val tbPersonSubsegmentation = "person_subsegmentation"
  val tbPersonId = "person_id"
  val tbSalesConsultantOrder = "sales_consultant_order"
  val tbEngineLogApplication = "engine_log_application"
  val tbDebitTitles = "debit_titles"

  val ENGINELOGAPPLICATION = Map("keyspace" -> this.ksApplicationLog, "table" -> this.tbEngineLogApplication)
  val PERSONSUBSEGMENTATION = Map("keyspace" -> this.ksSegmentation, "table" -> this.tbPersonSubsegmentation)

  def writeTable[T](dataset: Dataset[T],
                    table: Map[String, String],
                    saveMode: SaveMode = SaveMode.Append): Unit = {
    dataset.write.format(sparkCassandra).options(table).mode(saveMode).save()
  }
}

class ScyllaDB(conf: SparkConf) extends Serializable {
  val conn = CassandraConnector(conf)

  def setPersonSubsegmentation(ds: Dataset[PersonSubsegmentation]): Unit = {
    ScyllaDB.writeTable(ds, ScyllaDB.PERSONSUBSEGMENTATION)
  }

  def setEngineLogApplication(ds: Dataset[EngineLogApplication]): Unit = {
    ScyllaDB.writeTable(ds, ScyllaDB.ENGINELOGAPPLICATION)
  }
}
