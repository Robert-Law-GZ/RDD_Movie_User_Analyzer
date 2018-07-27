import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RDD2Dataset {
  def main(args: Array[String]): Unit = {
    var conf = HBaseConfiguration.create();
    var sc_conf = new SparkConf().setAppName("Insert data to hbase").setMaster("local[2]")
    val spark = SparkSession.builder().config(sc_conf).getOrCreate()

    val table_name = "student"

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, table_name)

    val RDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val newRDD = RDD.map({ result =>
      val name = Bytes.toString(result._2.getValue("info".getBytes, "name".getBytes))
      val age = Bytes.toString(result._2.getValue("info".getBytes, "age".getBytes))
      val gender = Bytes.toString(result._2.getValue("info".getBytes, "gender".getBytes))
      Student(name, age, gender)
    })

    import spark.implicits._
    val dataSet = spark.createDataset[Student](newRDD)
    dataSet.show()

    spark.stop()
  }
}
