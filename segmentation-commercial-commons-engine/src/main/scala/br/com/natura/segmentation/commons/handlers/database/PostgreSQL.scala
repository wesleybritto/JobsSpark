package br.com.natura.segmentation.commons.handlers.database

import java.sql.{Connection, ResultSet}
import java.util.Properties

import br.com.natura.segmentation.commons.domains.application.{ConsultantSegmentation, PostgreSQLCredentials}
import br.com.natura.segmentation.commons.domains.postgresql.{SegmentationCategory, Structure}
import br.com.natura.segmentation.commons.enumerators.Databases
import br.com.natura.segmentation.commons.handlers.spark.Spark
import br.com.natura.segmentation.commons.settings.SegmentationSettings
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

object PostgreSQL {
  val segmentationCredentials: PostgreSQLCredentials = this.setCredentials(Databases.SEGMENTATION)
  val masterdataCredentials: PostgreSQLCredentials = this.setCredentials(Databases.MASTERDATA)

  val segmentationConnection: Connection = this.setConnection(Databases.SEGMENTATION).getConnection
  val masterdataConnection: Connection = this.setConnection(Databases.MASTERDATA).getConnection

  def setConnection(database: String): BasicDataSource = {
    val credentials: PostgreSQLCredentials = this.getCredentials(database)
    val ds = new BasicDataSource()

    ds.setDriverClassName(credentials.driver)
    ds.setUrl(credentials.url)
    ds.setUsername(credentials.user)
    ds.setPassword(credentials.pass)

    ds
  }

  def getProperties(database: String): Properties = {
    val credentials: PostgreSQLCredentials = this.getCredentials(database)

    val connectionProperties: Properties = new Properties()
    connectionProperties.put("user", credentials.user)
    connectionProperties.put("password", credentials.pass)
    connectionProperties.put("driver", credentials.driver)

    connectionProperties
  }

  def getCredentials(database: String): PostgreSQLCredentials = {
    database match {
      case Databases.SEGMENTATION => this.segmentationCredentials
      case Databases.MASTERDATA   => this.masterdataCredentials
    }
  }

  def setCredentials(database: String): PostgreSQLCredentials = {
    database match {
      case Databases.SEGMENTATION  =>
        PostgreSQLCredentials(
          SegmentationSettings.postgresql_segmentation_user,
          SegmentationSettings.postgresql_segmentation_pass,
          SegmentationSettings.postgresql_segmentation_driver,
          SegmentationSettings.postgresql_segmentation_url
        )
      case Databases.MASTERDATA    =>
        PostgreSQLCredentials(
          SegmentationSettings.postgresql_masterdata_user,
          SegmentationSettings.postgresql_masterdata_pass,
          SegmentationSettings.postgresql_masterdata_driver,
          SegmentationSettings.postgresql_masterdata_url
        )
    }
  }

  def closeConnections(): Unit = {
    this.segmentationConnection.close()
    this.masterdataConnection.close()
  }
}

class PostgreSQL() extends Serializable {
  def getStructuresWithOpenedCycle(country: Int, structureLevel: Int): DataFrame = {
    val statement: String =
      s"""
         |(
         |select		  cs.country,
         |  			    cs.company,
         |			      cs.business_model,
         |			      cs.structure_level,
         |			      cs.structure_code,
         |			      cs.structure_name,
         |			      cs.parent_structure_level,
         |			      cs.parent_structure_code,
         |			      csn.structure_name parent_structure_name,
         |			      cs.operational_cycle
         |from		    masterdata.commercial_structure cs
         |inner join	masterdata.commercial_structure csn
         |on			    cs.country = csn.country
         |and 		    cs.company = csn.company
         |and 		    cs.business_model = csn.business_model
         |and 		    cs.parent_structure_level = csn.structure_level
         |and 		    cs.parent_structure_code = csn.structure_code
         |and 		    cs.operational_cycle = csn.operational_cycle
         |where		    (cs.country = $country and cs.structure_level = $structureLevel)
         |and			    now() between cs.start_date and cs.end_date
         | ) as query
       """.stripMargin

    Spark.
      getSession.
      read.
      jdbc(SegmentationSettings.postgresql_masterdata_url, statement, PostgreSQL.getProperties(Databases.MASTERDATA))
  }

  def getStructureConsultants(structure: Structure): ResultSet = {
    val statement =
      s"""
         |select *
         |from (
         |  select rank() over(partition by br.company, br.country, br.business_model, br.brm_person_uid
         |                                                           order by br.created_at desc) rank_id,
         |                                           br.company,
         |                                           br.country,
         |                                           br.business_model,
         |                                           br.structure_level,
         |                                           br.structure_code,
         |                                           br.person_code,
         |                                           br.business_status_code,
         |                                           br."cycle",
         |                                           br."name",
         |                                           br."function",
         |                                           br."role",
         |                                           br.created_at,
         |                                           p.person_uid
         |	                                  from masterdata.business_relation br
         |                                    inner join masterdata.brm_person p
         |                                        on br.brm_person_uid = p.brm_person_uid
         |                                     where br."role" = 1
         |                                       and br."function" = 1
         |                                       and br.country = ?
         |                                       and br.company = ?
         |                                       and br.business_model = ?
         |                                       and exists (select *
         |                                                     from masterdata.business_relation rl
         |                                                    where rl.company = br.company
         |                                                      and rl.country = br.country
         |                                                      and rl.business_model = br.business_model
         |                                                      and rl.brm_person_uid = br.brm_person_uid
         |                                                      and rl."function" = br."function"
         |                                                      and rl."role" = br."role"
         |                                                      and rl.country = br.country
         |                                                      and rl.company = br.company
         |                                                      and rl.business_model = br.business_model
         |                                                      and rl.structure_level = ?
         |                                                      and rl.structure_code = ?)) pp
         |                             where pp.rank_id = 1
         |                               and pp.business_status_code not in (2, 4)
         |                               and pp.country = ?
         |                               and pp.company = ?
         |                               and pp.business_model = ?
         |                               and pp.structure_level = ?
         |                               and pp.structure_code = ?
       """.stripMargin

    val query = PostgreSQL.masterdataConnection.prepareStatement(statement)
    query.setInt(1, structure.country)
    query.setInt(2, structure.company)
    query.setInt(3, structure.business_model)
    query.setInt(4, structure.structure_level)
    query.setInt(5, structure.structure_code)
    query.setInt(6, structure.country)
    query.setInt(7, structure.company)
    query.setInt(8, structure.business_model)
    query.setInt(9, structure.structure_level)
    query.setInt(10, structure.structure_code)

    query.executeQuery()
  }

  def getSubsegmentationsBySegmentationName(country: Int,
                                            segmentationcategory_name: String,
                                            subsegmentationexecution_multiple: Boolean,
                                            subsegmentationexecution_period: Int): DataFrame = {
    val statement =
      s"""
         |(
         |select
         |	s.country,
         |	s.company,
         |	s.business_model,
         |	s.segmentation_uid,
         |	s.segmentation_id,
         |	ss.subsegmentation_uid,
         |	ss.subsegmentation_id,
         |	ssp.index_code,
         |	ssp.index_value,
         |	ssm.subsegmentation_function,
         |	sse.subsegmentationexecution_period,
         |	sse.subsegmentationexecution_multiple
         |from		    segmentation.segmentation s
         |inner join	segmentation.subsegmentation ss
         |on			s.segmentation_uid = ss.segmentation_uid
         |inner join	segmentation.subsegmentation_method ssm
         |on			ss.subsegmentation_uid = ssm.subsegmentation_uid
         |inner join	segmentation.subsegmentation_execution sse
         |on			ss.subsegmentation_uid = sse.subsegmentation_uid
         |inner join	segmentation.domain_segmentation_category sc
         |on			s.segmentationcategory_id = sc.segmentationcategory_id
         |left join	segmentation.subsegmentation_performance ssp
         |on			ss.subsegmentation_uid = ssp.subsegmentation_uid
         |where       s.country = $country
         |and			    s.segmentation_type = 1
         |and 		    sc.segmentationcategory_name = '$segmentationcategory_name'
         |and         sse.subsegmentationexecution_multiple = $subsegmentationexecution_multiple
         |${this.setPeriodFilter(subsegmentationexecution_period)}
         |) as query
       """.stripMargin

    Spark.
      getSession.
      read.
      jdbc(SegmentationSettings.postgresql_segmentation_url, statement, PostgreSQL.getProperties(Databases.SEGMENTATION))
  }

  def setPeriodFilter(period: Int): String = {
    if(period > 0) {
      s"and sse.subsegmentationexecution_period = ${period}"
    } else {
      ""
    }
  }

  def getSubsegmentation(country: Int, subsegmentation_id: Int): DataFrame = {
    val statement =
      s"""
         |(
         |select
         |      s.country,
         |      s.company,
         |      s.business_model,
         |			s.segmentation_uid,
         |     	s.segmentation_id,
         |     	ss.subsegmentation_uid,
         |    	ss.subsegmentation_id,
         |     	ssp.index_code,
         |     	ssp.index_value,
         |     	ssm.subsegmentation_function
         |from		segmentation.segmentation s
         |inner join	segmentation.subsegmentation ss
         |on			s.segmentation_uid = ss.segmentation_uid
         |inner join	segmentation.subsegmentation_method ssm
         |on			ss.subsegmentation_uid = ssm.subsegmentation_uid
         |left join	segmentation.subsegmentation_performance ssp
         |on			ss.subsegmentation_uid = ssp.subsegmentation_uid
         |where   s.country = $country
         |and			s.segmentation_type = 1
         |and 		ss.subsegmentation_id = $subsegmentation_id
         | ) as query
       """.stripMargin

    Spark.
      getSession.
      read.
      jdbc(SegmentationSettings.postgresql_segmentation_url, statement, PostgreSQL.getProperties(Databases.SEGMENTATION))
  }

  def getSegmentationCategory(segmentationCategory_name: String): DataFrame = {
    val statement =
      s"""
         |(
         |SELECT  *
         |FROM    segmentation.domain_segmentation_category
         |WHERE   segmentationcategory_name = '$segmentationCategory_name'
         | ) as query
       """.stripMargin

    Spark.
      getSession.
      read.
      jdbc(SegmentationSettings.postgresql_segmentation_url, statement, PostgreSQL.getProperties(Databases.SEGMENTATION))
  }



  def getStructureLastCycles(country: Int, structureLevel: Int, last: Int): DataFrame = {
    val statement: String =
      s"""
         |(
         |
         |select *
         |from (
         |select		  cs.country,
         |  			    cs.company,
         |			      cs.business_model,
         |			      cs.structure_level,
         |			      cs.structure_code,
         |			      cs.operational_cycle as last_cycles,
         |			      cast (rank() OVER (PARTITION BY cs.structure_code order by cs.operational_cycle desc) as INT) rank_id
         |from		    masterdata.commercial_structure cs
         |inner join	masterdata.commercial_structure csn
         |on			    cs.country = csn.country
         |and 		    cs.company = csn.company
         |and 		    cs.business_model = csn.business_model
         |and 		    cs.parent_structure_level = csn.structure_level
         |and 		    cs.parent_structure_code = csn.structure_code
         |and 		    cs.operational_cycle = csn.operational_cycle
         |where		    (cs.country = '$country' and cs.structure_level = '$structureLevel')
         |and			    now() > cs.start_date
         |) pp
         |where pp.rank_id <= '$last'
         | ) as query
       """.stripMargin

    Spark.
      getSession.
      read.
      jdbc(SegmentationSettings.postgresql_masterdata_url, statement, PostgreSQL.getProperties(Databases.MASTERDATA))
  }




  def getConsultantStartCycle (countryId: Int): DataFrame = {
    val statement: String =
      s"""
         |(
         |
         |select
         |            DISTINCT rh.person_id,
         |            rh.sales_hierarchy_cycle
         |from masterdata.recommended_sales_hierarchy rh
         |join masterdata.person p on p.person_id = rh.person_id
         |where p.country_id = '$countryId'
         | and rh.sales_hierarchy_cycle is not null
         | ) as query
       """.stripMargin

    Spark.
      getSession.
      read.
      jdbc(SegmentationSettings.postgresql_masterdata_url, statement, PostgreSQL.getProperties(Databases.MASTERDATA))
  }

  def getConsultantFirstCycle(country: Int): DataFrame = {
    val statement: String =
      s"""
         |(
         |         select
         |            a.person_code,
         |            min(a.cycle) as first_cycle
         |FROM
         |masterdata.business_relation a
         |where a.business_status_code = 3 and a.country = '$country' and a.person_code notnull
         |and not exists (
         |select 1 from masterdata.business_relation b
         |where a.brm_person_uid = b.brm_person_uid
         |and b.business_status_code = 4)
         |group by a.person_code
         | ) as query
       """.stripMargin

    Spark.
      getSession.
      read.
      jdbc(SegmentationSettings.postgresql_masterdata_url, statement, PostgreSQL.getProperties(Databases.MASTERDATA))
  }

}