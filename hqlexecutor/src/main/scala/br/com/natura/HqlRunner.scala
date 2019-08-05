package br.com.natura
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}

object Executor2{

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
     // .config("spark.some.config.option", "some-value")
      .enableHiveSupport()
      .getOrCreate()

    println("------ START -----")


    println(
      """
        |
        |Printando parametros
        |
        |
      """.stripMargin)

    args.foreach(println)


    val params = collection.mutable.Map[String, Any]()
    params.put("priority", args(0))
    params.put("tableName", args(1))
    params.put("loadName", args(2))
    params.put("dataMart", args(3))
    params.put("dependentTable", args(4))
    params.put("tableLog", args(5))
    params.put("status", args(6))

    executor(params:collection.mutable.Map[String, Any],spark)

  }

  def today(): String = {
    val dateFmt = "yyyy-MM-dd HH:mm:ss"
    var date = new Date
    var sdf = new SimpleDateFormat(dateFmt)
    sdf.format(date)
  }

  def executor(params:collection.mutable.Map[String, Any],spark: SparkSession): Unit ={

    var priority = params.get("priority").get
    var tableName = params.get("tableName").get
    var loadName = params.get("loadName").get
    var dataMart = params.get("dataMart").get
    var dependentTable = params.get("dependentTable").get
    var tableLog = params.get("tableLog").get
    var status = params.get("status").get


    spark
      .sqlContext
      .sql(s"select load_sql from dbconnect.datamart_parameters where table_name = '$tableName'")
      .show()


    println(
      s"""
        |
        |
        |printando resultado da Query
        |tableName: $tableName
        |
        |
        |
      """.stripMargin)


    println(
      """
        |
        |
        |printando resultado da Query
        |
        |
        |
      """.stripMargin)

    val jobStarDate = today()

    var executeQuery = spark.sqlContext.sql(s"select load_sql from dbconnect.datamart_parameters where table_name = '$tableName'").collect()(0)
    var exeQuery = executeQuery.get(0).toString
    println(exeQuery)
    //Executa a Query
    spark.sqlContext.sql(exeQuery)

   val jobFinishDate = today()

    var count = exeQuery
    count = count.replaceAll(s"insert overwrite table $dataMart.$tableName", "")
    var origin_count = spark.sql(s"$count").count()
    var dest_count = spark.sql(s"Select * from $dataMart.$tableName").count()




    spark.sql(s"insert into dbconnect.ingestion_events values('$jobStarDate', '${jobFinishDate}', '$dataMart', '$tableName', '', '$origin_count', '$dest_count', 'OK', '$exeQuery')")



    spark.stop()

  }

}
