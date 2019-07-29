package br.com.natura.segmentation.commons.data

import java.sql.{Date, ResultSet, Timestamp}
import java.util.UUID

import br.com.natura.segmentation.commons.domains.application._
import br.com.natura.segmentation.commons.domains.postgresql._
import br.com.natura.segmentation.commons.domains.scylladb._
import br.com.natura.segmentation.commons.enumerators.{Country, GroupCodes, LastCycles, TableFields}
import br.com.natura.segmentation.commons.handlers.database.{PostgreSQL, ScyllaDB}
import br.com.natura.segmentation.commons.handlers.spark.Spark
import br.com.natura.segmentation.commons.parsers.SegmentationDomain
import br.com.natura.segmentation.commons.settings.SegmentationSettings
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import com.datastax.spark.connector.{SomeColumns, _}
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD



class SegmentationCollector(arguments: SegmentationArguments) extends Serializable {
  val ss: SparkSession = Spark.getSession

  Spark.setLogLevel(Level.WARN)

  import ss.implicits._

  val scylladb = new ScyllaDB(Spark.getContextConf)
  val postgresql = new PostgreSQL()

  def getStructuresWithOpenedCycle(structureLevel: Int): Dataset[Structure] = {
    postgresql
      .getStructuresWithOpenedCycle(arguments.country, structureLevel)
      .as[Structure]
  }

  def getGroupsConsultants(groups: Dataset[Structure]): Dataset[PersonRelation] = {
    groups.flatMap(this.getStructureConsultants)
  }

  def getStructureConsultants(group: Structure): List[PersonRelation] = {
    val consultants: ResultSet = postgresql.getStructureConsultants(group)

    Iterator
      .continually(consultants)
      .takeWhile(_.next)
      .map(SegmentationDomain.parse2PersonRelation(_, group))
      .toList
  }

  def getSubsegmentationsByCategory(segmentationCategory_name: String): Dataset[Subsegmentation] = {
    postgresql
      .getSubsegmentationsBySegmentationName(
        arguments.country,
        segmentationCategory_name,
        arguments.multiple,
        arguments.period
      )
      .as[Subsegmentation]
  }

  def getSubsegmentation(segmentation_id: Int): Dataset[Subsegmentation] = {
    postgresql.getSubsegmentation(arguments.country, segmentation_id).as[Subsegmentation]
  }

  def getConsultants(structure_level: Int): Dataset[PersonRelation] = {
    val groups = this.getStructuresWithOpenedCycle(structure_level)

    this.getGroupsConsultants(groups)
  }

  def getConsultantsSubsegmentations(segmentationCategory_name: String): Dataset[ConsultantSegmentation] = {
    val subsegmentations = this.getSubsegmentationsByCategory(segmentationCategory_name)
    val consultants = this.getConsultants(GroupCodes.COUNTRIES(arguments.country))

    this.joinConsultantsSubsegmentations(consultants, subsegmentations)
  }

  def getConsultantSubsegmentation(subsegmentation_id: Int): Dataset[ConsultantSegmentation] = {
    val subsegmentations = this.getSubsegmentation(subsegmentation_id)
    val consultants = this.getConsultants(GroupCodes.COUNTRIES(arguments.country))

    this.joinConsultantsSubsegmentations(consultants, subsegmentations)
  }

  def joinConsultantsSubsegmentations(consultants: Dataset[PersonRelation],
                                      subsegmentations: Dataset[Subsegmentation]): Dataset[ConsultantSegmentation] = {
    consultants
      .join(subsegmentations, Seq(TableFields.COMPANY, TableFields.COUNTRY, TableFields.BUSINESSMODEL))
      .withColumn(TableFields.COUNTRYCODE, functions.lit(Country.COUNTRYCODE(arguments.country)))
      .as[ConsultantSegmentation]

  }

  def getBaseData(segmentationCategory_name: String): RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])] = {
    val consultants = this.getConsultantsSubsegmentations(segmentationCategory_name)

    val c = SomeColumns(
      TableFields.COMPANY,
      TableFields.COUNTRY,
      TableFields.PERSONID,
      TableFields.SUBSEGMENTATIONID
    )

    consultants
      .rdd
      .leftJoinWithCassandraTable(ScyllaDB.ksSegmentation, ScyllaDB.tbPersonSubsegmentation)
      .on(c)
      .map(r => (r._1, SegmentationDomain.parse2PersonSubsegmentation(r._2)))
      .reduceByKey((a, b) => {
        if(a.isEmpty && b.isEmpty) None
        else if (a.nonEmpty && b.isEmpty) a
        else if (a.isEmpty && b.nonEmpty) b
        else this.getCurrentPersonSubsegmentation(a, b)
      })

  }

  def getCurrentPersonSubsegmentation(a: Option[PersonSubsegmentation],
                                      b: Option[PersonSubsegmentation]): Option[PersonSubsegmentation] = {
    if(a.get.created_at.getTime >= b.get.created_at.getTime) {
      a
    } else {
      b
    }
  }

  def setPersonSubsegmentationMovimentation(ds: Dataset[PersonSubsegmentationResult],
                                            engine: String): Dataset[PersonSubsegmentation] = {
    ds.map(this.generatePersonSubsegmentation(_, engine)).filter(_.subsegmentation_id > 0)
  }

  def setPersonSubsegmentationMovimentationCycles(ds: Dataset[PersonSubsegmentationResult],
                                            engine: String): Dataset[PersonSubsegmentation] = {

    ds.rdd.map(r =>( (r.consultant.person_id, r.consultant.subsegmentation_uid), r))
      .reduceByKey((a,b) =>
        if (a.result) a else b)
      .map(_._2)
      .toDS()
      .as[PersonSubsegmentationResult]
      .map(this.generatePersonSubsegmentation(_, engine))
      .filter(_.subsegmentation_id > 0)
  }

  def generatePersonSubsegmentation(r: PersonSubsegmentationResult, engine: String): PersonSubsegmentation = {
    val cur = r.currentSubsegmentation

    if((cur.isEmpty && r.result) || (cur.nonEmpty && cur.get.status != r.result) ) {
      SegmentationDomain.parse2PersonSubsegmentation(r.consultant, r.result, engine)
    } else {
      PersonSubsegmentation.empty
    }
  }

  def setPersonSubsegmentation(ds: Dataset[PersonSubsegmentation]): Unit = {
    scylladb.setPersonSubsegmentation(ds)
  }


  def getConsultantsIndex(consultants: RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])]): RDD[(ConsultantSegmentation, ConsultantIndex)] = {
    val columns = SomeColumns(
      TableFields.COMPANY,
      TableFields.COUNTRY,
      TableFields.BUSINESSMODEL,
      TableFields.CONSULTANTCODE as TableFields.PERSONCODE,
      TableFields.OPERATIONALCYCLE,
      TableFields.INDEXCODE
    )

    consultants
      .filter(_._1.index_code.nonEmpty)
      .map(_._1)
      .joinWithCassandraTable(ScyllaDB.ksMasterdata, ScyllaDB.tbConsultantIndex)
      .on(columns)
      .map(r => (r._1, SegmentationDomain.parse2ConsultantIndex(r._2)))
  }

  def getHistoricConsultantsIndex(consultants: RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])]): RDD[(ConsultantSegmentation, Iterable[ConsultantIndex])] = {
    val columns = SomeColumns(
      TableFields.COMPANY,
      TableFields.COUNTRY,
      TableFields.BUSINESSMODEL,
      TableFields.CONSULTANTCODE as TableFields.PERSONCODE

    )

    consultants
      .filter(_._1.index_code.nonEmpty)
      .map(_._1)
      .joinWithCassandraTable(ScyllaDB.ksMasterdata, ScyllaDB.tbConsultantIndex)
      .on(columns)
      .map(r => (r._1, SegmentationDomain.parse2ConsultantIndex(r._2)))
      .groupByKey()

  }

  def joinPersonsubsegmentationIndex(consultants: RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])],
                                     index: RDD[(ConsultantSegmentation, ConsultantIndex)]): DataFrame = {
    consultants
      .join(index)
      .map(r => (r._1, r._2._1, r._2._2))
      .toDF(TableFields.CONSULTANT, TableFields.CURRENTSUBSEGMENTATION, TableFields.CONSULTANTINDEX)
  }

  def joinPersonsubsegmentationIndexStartCycle(consultants: RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])],
                                               personId: RDD[(ConsultantSegmentation, PersonId)],
                                     index: RDD[(ConsultantSegmentation, Iterable[ConsultantIndex])],
                                               startCycle: RDD[(ConsultantSegmentation, Option[FirstCycle])],
                                               cycles: RDD[(ConsultantSegmentation, Iterable[Cycles])]): DataFrame = {

    consultants
      .join(index)
      .join(startCycle)
      .join(cycles)
      .join(personId)
      .map(r => (r._1, r._2._1._1._1._1, r._2._2, r._2._1._1._1._2.toList, r._2._1._1._2, r._2._1._2.toList))
      .toDF(TableFields.CONSULTANT,TableFields.CURRENTSUBSEGMENTATION, TableFields.PERSON, TableFields.CONSULTANTINDEX, TableFields.STARTCYCLE, TableFields.CYCLES)

  }

  def getBaseDataWithIndex(consultants: RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])]): DataFrame = {
    val indexes = this.getConsultantsIndex(consultants)

    this.joinPersonsubsegmentationIndex(consultants, indexes)
  }

  def getHistoricBaseDataWithIndex(consultants: RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])], lastCycles: Int): DataFrame = {

    val indexes = this.getHistoricConsultantsIndex(consultants)
    val personId = this.getPersonId(consultants)
    val startCycles = this.getStartCycles(consultants)
    val cycles = this.getConsultantCycles(consultants, lastCycles)

    this.joinPersonsubsegmentationIndexStartCycle(consultants, personId, indexes, startCycles, cycles)
  }

  def getEmptyDataFrame(data: RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])]): DataFrame = {
    Seq
      .empty[(ConsultantSegmentation, PersonSubsegmentation)]
      .toDF(TableFields.CONSULTANT, TableFields.CURRENTSUBSEGMENTATION)
  }

  def getPersonId(consultants: RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])]): RDD[(ConsultantSegmentation, PersonId)] = {
    val columns = SomeColumns(
      TableFields.PERSONID
    )

    consultants
      .map(_._1)
      .joinWithCassandraTable(ScyllaDB.ksMasterdata, ScyllaDB.tbPersonId)
      .on(columns)
      .map(r => (r._1, SegmentationDomain.parse2PersonId(r._2)))
  }


  def getSalesConsultantOrders(consultants: RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])]): RDD[(ConsultantSegmentation, Iterable[Option[SalesConsultantOrder]])] = {
    val columns = SomeColumns(
      TableFields.COMPANYID as TableFields.COMPANY,
      TableFields.COUNTRYCODE,
      TableFields.PERSONID,
      TableFields.BUSINESSMODELID as TableFields.BUSINESSMODEL

    )

   consultants
      .map(_._1)
      .leftJoinWithCassandraTable(ScyllaDB.ksMasterdata, ScyllaDB.tbSalesConsultantOrder)
      .on(columns)
      .map(r => (r._1, SegmentationDomain.parse2SalesConsultantOrder(r._2)))
      .groupByKey()
  }


  def getOnlySalesConsultantOrders(consultants: RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])]): RDD[SalesConsultantOrder] = {
    val columns = SomeColumns(
      TableFields.COMPANYID as TableFields.COMPANY,
      TableFields.COUNTRYCODE,
      TableFields.PERSONID,
      TableFields.BUSINESSMODELID as TableFields.BUSINESSMODEL
    )

    consultants
      .map(_._1)
      .joinWithCassandraTable(ScyllaDB.ksMasterdata, ScyllaDB.tbSalesConsultantOrder)
      .on(columns)
      .map(r => (r._1, SegmentationDomain.parse2OnlySalesConsultantOrder(r._2)))
      .map(_._2)

  }

  def getStructureCycles(country: Int, structureLevel: Int, last: Int): Dataset[OperationalCycles] = {
    postgresql
      .getStructureLastCycles(country: Int, structureLevel: Int, last: Int).as[OperationalCycles]

  }

  def getConsultantCycles (consultants: RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])], lastCycles: Int): RDD[(ConsultantSegmentation, Iterable[Cycles])] = {

    val onlyConsultants =
      consultants
        .map(_._1)
        .toDS()

    val cycles = getStructureCycles(arguments.country, GroupCodes.COUNTRIES(arguments.country), lastCycles)

    onlyConsultants
      .join(cycles,
        Seq(TableFields.COMPANY,
          TableFields.COUNTRY,
          TableFields.BUSINESSMODEL,
          TableFields.STRUCTURECODE,
          TableFields.STRUCTURELEVEL))
      .as[ConsultantCycles].map(x => {
  (
    ConsultantSegmentation(
      x.country,
      x.company,
      x.business_model,
      x.person_id,
      x.subsegmentation_uid,
      x.subsegmentation_id,
      x.subsegmentation_function,
      x.segmentation_uid,
      x.segmentation_id,
      x.operational_cycle,
      x.person_code,
      x.structure_level,
      x.structure_code,
      x.index_code,
      x.index_value,
      x.country_code
    ),

    Cycles(x.last_cycles,
     x.rank_id
    ))})
      .rdd
      .groupByKey()

  }

  def getDebitTitles(orders: RDD[SalesConsultantOrder]): RDD[(SalesConsultantOrder, Option[DebitTitles])] = {
    val columns = SomeColumns(
      TableFields.COMPANYID,
      TableFields.COUNTRYCODE,
      TableFields.BUSINESSMODELID,
      TableFields.PERSONID,
      TableFields.ORDERUID
    )

      orders
        .filter(_.business_model_id.nonEmpty)
        .leftJoinWithCassandraTable(ScyllaDB.ksMasterdata, ScyllaDB.tbDebitTitles)
        .on(columns)
        .map(r => (r._1, SegmentationDomain.parse2DebitTitles(r._2)))

  }

  def getConsultantDetailOrder(consultants: RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])], last_cycles: Int): DataFrame = {
    val personId = this.getPersonId(consultants)
    val orders = this.getSalesConsultantOrders(consultants)
    val cycles = this.getConsultantCycles(consultants, last_cycles)
    val startCycle = this.getStartCycles(consultants)

    this.joinPersonsubsegmentationDetailOrder(consultants, orders, personId,  cycles, startCycle)
  }



  def joinPersonsubsegmentationDetailOrder(consultants: RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])],
                                                orders: RDD[(ConsultantSegmentation, Iterable[Option[SalesConsultantOrder]])],
                                                personId: RDD[(ConsultantSegmentation, PersonId)],
                                                cycles:  RDD[(ConsultantSegmentation, Iterable[Cycles])],
                                                startCycle: RDD [(ConsultantSegmentation, Option[FirstCycle])]): DataFrame = {


    consultants
      .join(personId)
      .join(orders)
      .join(cycles)
      .join(startCycle)
      .map(r => (r._1, r._2._1._1._1._1, r._2._1._1._1._2, r._2._1._1._2.toList, r._2._1._2.toList, r._2._2))
      .toDF(TableFields.CONSULTANT, TableFields.CURRENTSUBSEGMENTATION, TableFields.PERSON, TableFields.ORDERS, TableFields.CYCLES, TableFields.STARTCYCLE)
  }


  def getConsultantTitles(consultants: RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])]): DataFrame = {

    val orders = this.getOnlySalesConsultantOrders(consultants)
    val titles = this.getDebitTitles(orders)

    this.joinOrderDetailTitles(consultants, titles)
  }

  def joinOrderDetailTitles(consultants: RDD[(ConsultantSegmentation, Option[PersonSubsegmentation])],
                       titles: RDD[(SalesConsultantOrder, Option[DebitTitles])]): DataFrame = {


    val consultantsWithKey =
      consultants.map(consultants => (consultants._1.person_id, consultants))

    val titlesWithKey  = titles.filter(_._2.nonEmpty)
      .map(titles => (titles._2.get.person_id, titles._2))

       consultantsWithKey
         .leftOuterJoin(titlesWithKey)
         .map(r => (r._2._1, r._2._2))
         .groupByKey()
         .map(x => (x._1._1, x._1._2, x._2.toList))
         .toDF(TableFields.CONSULTANT, TableFields.CURRENTSUBSEGMENTATION, TableFields.TITLES)

  }


  def getStartCycles(consultants: RDD[(ConsultantSegmentation,Option[PersonSubsegmentation])]): RDD[(ConsultantSegmentation, Option[FirstCycle])] = {//RDD[(ConsultantSegmentation, Option[SalesHierarchy])] = {

    val startCycles =
      postgresql
        .getConsultantFirstCycle(arguments.country)
        .as[FirstCycle]
        .rdd
        .map(startCycles => (startCycles.person_code, startCycles))

    consultants
      .map(consultants => (consultants._1.person_code, consultants._1))
      .leftOuterJoin(startCycles)
      .map(_._2)
  }

  def getSegmentationCategory(segmentationCategory_name: String): Dataset[SegmentationCategory] = {
    postgresql.getSegmentationCategory(segmentationCategory_name).as[SegmentationCategory]
  }

  def generateEngineLogApplication(segmentationCategory_name: String): Dataset[EngineLogApplication] = {
    val currentDate = new java.util.Date().getTime
    val historyDate = new Date(currentDate)
    val segmentationCategory = this.getSegmentationCategory(segmentationCategory_name).collect()
    val segmentationCategory_id = if(segmentationCategory.nonEmpty) {
      segmentationCategory(0).segmentationcategory_id
    } else {
      0
    }

    val ela = List(
      EngineLogApplication(
        application_id = SegmentationSettings.project.toUpperCase(),
        history_date = historyDate,
        hour_range = 1,
        engine_id = segmentationCategory_id,
        uuid = UUID.randomUUID().toString,
        process_date = new Timestamp(historyDate.getTime)
      )
    )

    ela.toDS()
  }

  def setEngineLogApplication(segmentationCategory_name: String): Unit = {
    val dsEngineLogApplication = this.generateEngineLogApplication(segmentationCategory_name)

    scylladb.setEngineLogApplication(dsEngineLogApplication)
  }
}