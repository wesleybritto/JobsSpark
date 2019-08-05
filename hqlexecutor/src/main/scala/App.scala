import org.apache.spark.sql.SparkSession

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]): Unit = {
    println( "Hello World!" )

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

      val b = Seq('1','2','3')
  val haha = spark.sparkContext.parallelize(b)

    val ar = haha.collect()


    val int = 1
    var long = 1L

    println(int == long)


    println(ar(0))
  }
}
