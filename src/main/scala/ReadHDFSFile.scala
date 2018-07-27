import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadHDFSFile {
  def main(args: Array[String]): Unit = {
    var sc_conf = new SparkConf().setAppName("Read hdfs file").setMaster("local[2]")
    val spark = SparkSession.builder().config(sc_conf).getOrCreate()
    val text_file = spark.sparkContext.textFile("hdfs://localhost:9000/user/robert/people.txt")

    import spark.implicits._

    val lines = text_file.map(_.split(",")).map(u => User(u(1).trim, u(0).trim))

    val userDataFrame = spark.createDataset[User](lines)
    userDataFrame.show()
  }
}
