package br.com.natura
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Date

object Executor{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark - HQLExecutor")
      .enableHiveSupport()
      .getOrCreate()

    var INSERT_LOG_EVENT_HIVE = ""
    step("------ START -----")

    step("""Printando parametros """)

    args.foreach(println)

    val params = collection.mutable.Map[String, String]()
    params.put("tableName", args(0))
    params.put("loadName", args(1))
    params.put("dataMart", args(2))
    params.put("origin_table", args(3))
    params.put("destiny_table", args(4))
    params.put("fs_location", args(5))
    params.put("tableLog", args(6))
    params.put("status", args(7))

    var exeQuery:String=null

    val jobStarDate = today()
    try {
      exeQuery=executor(params,spark).replace("'","''")

      println("------------------Inserting log...-------------------------")

    } catch {
      case e => {
        println("Erro ao inserir: " + e.getCause)
        e.printStackTrace()
        val jobFinishDate = today()
        INSERT_LOG_EVENT_HIVE = s"insert into  table ${params.get ("tableLog").get} values('EXECUTE SQL','GENERATE DW','$jobStarDate','${jobFinishDate}','${params.get ("dataMart").get}','${params.get ("origin_table").get}','${params.get ("destiny_table").get}','${params.get ("fs_location").get}','NOK','$exeQuery')"
        spark.sql (INSERT_LOG_EVENT_HIVE)
        System.exit(1)
      }
    }finally {
      val jobFinishDate = today()
        INSERT_LOG_EVENT_HIVE = s"insert into table ${params.get ("tableLog").get} values('EXECUTE SQL','GENERATE DW','$jobStarDate','${jobFinishDate}','${params.get ("dataMart").get}','${params.get ("origin_table").get}','${params.get ("destiny_table").get}','${params.get ("fs_location").get}','OK','$exeQuery')"
        spark.sql (INSERT_LOG_EVENT_HIVE)


    }

    spark.stop()
  }

  def today(): String = {
    val dateFmt = "yyyy-MM-dd HH:mm:ss"
    var date = new Date
    var sdf = new SimpleDateFormat(dateFmt)
    sdf.format(date)
  }



  def executor(params:collection.mutable.Map[String, String],spark: SparkSession): String = {

    var tableName = params.get("tableName").get
    var loadName = params.get("loadName").get
    var dataMart = params.get("dataMart").get
    var origin_table = params.get("origin_table").get
    var destiny_table = params.get("destiny_table").get
    var fs_location = params.get("fs_location").get
    var tableLog = params.get("tableLog").get
    var status = params.get("status").get

    /*spark
      .sqlContext
      .sql(s"select load_sql from bigdata_ingestion.datamart_parameters where table_name = '$tableName'")
      .show()*/

    step(s"printando resultado da Query $tableName")
    //step("printando resultado da Query")

    //var executeQuery = spark.sqlContext.sql(s"select load_sql from bigdata_ingestion.datamart_parameters where table_name = '$tableName'").collect()(0)
    //var exeQuery = executeQuery.get(0).toString

    //println(exeQuery)
    //Executa a Query
    //spark.sqlContext.sql(exeQuery)

    //val hiveDF = spark.sql(s"${loadName}")

    val hiveDF = spark.sql(loadName)

    println("before spark sql df.show")

    hiveDF.show()

    println("after spark sql df.show")

    hiveDF.write
      .option("path", fs_location)
      .mode("Overwrite")
      .format("orc")
      .saveAsTable(destiny_table)

    println("after save as table")

    return loadName
    //return exeQuery


    }



  def step(msg:String):Unit={
    println(
     s"""
        |
        |
        |${msg}
        |
        |
        |
      """.stripMargin)
  }


}





