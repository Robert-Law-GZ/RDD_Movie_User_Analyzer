import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

object UsersAnalyzer {
// 数据文件下载地址：http://files.grouplens.org/datasets/movielens/ml-20m.zip

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Movie Users Analyzer")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")
    val dataPath = "/usr/local/Cellar/spark/data/ml-20m/"
    val usersRDD = sc.textFile(dataPath + "tags.csv")
    //    userId,movieId,tag,timestamp
    val schemaForUsers = StructType("UserId::MovieId::tag::timestamp".split("::").map(column => StructField(column, StringType, true)))
    val usersRDDRows = usersRDD.map(_.split(",")).map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim))
    val userDataFrame = spark.createDataFrame(usersRDDRows, schemaForUsers);

    bySQL(spark,userDataFrame)

    //    val schemaForRatings=StructType("UserID::MovieID".split("::").map(column=>StructField(column,StringType,true))).add("Rating",DoubleType,true).add("Timestamp",StringType,true)
    val groups = userDataFrame.filter(s"UserId!='userId'").groupBy("tag").count()
    groups.orderBy(-groups("count")).show(10)

    //    userDataFrame.show(10)
    spark.stop()
  }

  def bySQL(spark: SparkSession, df: DataFrame): Unit = {
    df.createTempView("users");
    val sql = "select * from users where UserId<>'userId'";
    spark.sql(sql).show(10);
  }

  def byDataFrame(): Unit = {

  }

}
