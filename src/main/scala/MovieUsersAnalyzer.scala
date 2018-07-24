import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MovieUsersAnalyzer {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Movie Users Analyzer")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("warn")
    val dataPath="/usr/local/Cellar/spark/data/ml-20m/"
    val usersRDD=sc.textFile(dataPath+"tags.csv")
    val movieRDD=sc.textFile(dataPath+"movies.csv")
    val ratingsRDD=sc.textFile(dataPath+"ratings.csv")

    val users=usersRDD.collect()

    for (a<-users.take(10)){
      val line=a.split(",")

      for (r<-line){
        print(r);print("|")
      }

      println("\n")
    }

    spark.stop()
  }

}