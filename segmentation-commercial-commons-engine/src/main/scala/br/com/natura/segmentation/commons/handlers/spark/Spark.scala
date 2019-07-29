package br.com.natura.segmentation.commons.handlers.spark

import br.com.natura.segmentation.commons.handlers.database.PostgreSQL
import br.com.natura.segmentation.commons.settings.SegmentationSettings
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Spark {
  System.setProperty("hadoop.home.dir", SegmentationSettings.winUtils)

  val session: SparkSession = this.setSession()

  def setSession(): SparkSession = {
    val conf: SparkConf = new SparkConf()
      .set("spark.cassandra.connection.host", SegmentationSettings.scyllaAddress)
      .set("spark.cassandra.auth.username", SegmentationSettings.scyllaUser)
      .set("spark.cassandra.auth.password", SegmentationSettings.scyllaPass)
      .set("spark.cassandra.input.consistency.level", SegmentationSettings.scyllaInputConsistency)
      .set("spark.cassandra.output.consistency.level", SegmentationSettings.scyllaOutputConsistency)

    SparkSession
      .builder()
      .config(conf)
      .appName(SegmentationSettings.appName)
      .getOrCreate()
  }

  def getSession: SparkSession = {
    this.session
  }

  def getContext: SparkContext = {
    this.getSession.sparkContext
  }

  def getContextConf: SparkConf = {
    this.getContext.getConf
  }

  def setLogLevel(level: Level): Unit = {
    this.getContext.setLogLevel(level.toString)
  }

  def stop(): Unit = {
    PostgreSQL.closeConnections()

    this.getContext.stop()
    this.getSession.stop()
  }
}