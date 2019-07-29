package br.com.natura.segmentation.commons.settings

import com.typesafe.config.ConfigFactory

object SegmentationSettings {
  private val config = ConfigFactory.load()

  private val weblogGen = config.getConfig("segmentation")
  private val hadoopSettings = config.getConfig("hadoop_settings")
  private val scyllaSettings = config.getConfig("scylla_settings")
  private val masterdataSettings = config.getConfig("postgres_settings_masterdata")
  private val segmentationSettings = config.getConfig("postgres_settings_segmentation")

  lazy val project: String = weblogGen.getString("project")
  lazy val appName: String = weblogGen.getString("appName")

  // Hadoop
  lazy val winUtils: String = hadoopSettings.getString("winUtils")

  //Scylla
  lazy val scyllaAddress: String = scyllaSettings.getString("server_address")
  lazy val scyllaUser: String = scyllaSettings.getString("user")
  lazy val scyllaPass: String = scyllaSettings.getString("pass")
  lazy val scyllaInputConsistency: String = scyllaSettings.getString("input_consistency")
  lazy val scyllaOutputConsistency: String = scyllaSettings.getString("output_consistency")

  //PostgreSQL Driver
  lazy val postgresql_driver_prefix: String =     masterdataSettings.getString("prefix")

  //PostgreSQL MASTERDATA
  lazy val postgresql_masterdata_host: String =     masterdataSettings.getString("host")
  lazy val postgresql_masterdata_database: String = masterdataSettings.getString("database")
  lazy val postgresql_masterdata_port: String =     masterdataSettings.getString("port")
  lazy val postgresql_masterdata_user: String =     masterdataSettings.getString("user")
  lazy val postgresql_masterdata_pass: String =     masterdataSettings.getString("pass")
  lazy val postgresql_masterdata_ssl: String =      masterdataSettings.getString("ssl")
  lazy val postgresql_masterdata_driver: String =   masterdataSettings.getString("driver")

  lazy val postgresql_masterdata_url: String =
    postgresql_driver_prefix + postgresql_masterdata_host + "/" + postgresql_masterdata_database

  //PostgreSQL SEGMENTATION
  lazy val postgresql_segmentation_host: String =     segmentationSettings.getString("host")
  lazy val postgresql_segmentation_database: String = segmentationSettings.getString("database")
  lazy val postgresql_segmentation_port: String =     segmentationSettings.getString("port")
  lazy val postgresql_segmentation_user: String =     segmentationSettings.getString("user")
  lazy val postgresql_segmentation_pass: String =     segmentationSettings.getString("pass")
  lazy val postgresql_segmentation_ssl: String =      segmentationSettings.getString("ssl")
  lazy val postgresql_segmentation_driver: String =   segmentationSettings.getString("driver")

  lazy val postgresql_segmentation_url: String =
    postgresql_driver_prefix + postgresql_segmentation_host + "/" + postgresql_segmentation_database
}