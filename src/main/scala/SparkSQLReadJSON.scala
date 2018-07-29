import org.apache.spark.sql.SparkSession

object SparkSQLReadJSON {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder().master("local[2]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val df = spark.read.json("src/main/resources/examples/people.json")
    df.printSchema()

    df.select("Name").show();
    df.select($"Name", $"AGE" + 1).show()
    df.filter($"age">19).show()

  }

}
