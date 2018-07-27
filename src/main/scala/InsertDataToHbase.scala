import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object InsertDataToHbase {
  def main(args: Array[String]): Unit = {
    var conf = HBaseConfiguration.create();
    var sc_conf = new SparkConf().setAppName("Insert data to hbase").setMaster("local[2]")
    val spark = SparkSession.builder().config(sc_conf).getOrCreate()

    val table_name = "student"

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, table_name)

    val conn = ConnectionFactory.createConnection(conf)
    val tn = TableName.valueOf(table_name);

    val table = conn.getTable(tn);

    //    val g = new Get("4".getBytes)
    //    val studen = table.get(g)
    //
    //    if (studen.getExists) {
    //      println("已经存在")
    //    } else {
    //      val p = new Put("4".getBytes)
    //      p.addColumn("info".getBytes, "name".getBytes, "AAAA".getBytes)
    //      p.addColumn("info".getBytes, "age".getBytes, "34".getBytes)
    //      p.addColumn("info".getBytes, "gender".getBytes, "男".getBytes)
    //      table.put(p)
    //    }

    val RDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val newRDD = RDD.map({ result =>
      val infoBt = Bytes.toBytes("info")

      val ageBt = Bytes.toBytes("age")
      val nameBt = Bytes.toBytes("name")
      val genderBt = Bytes.toBytes("gender")

      val name = Bytes.toString(result._2.getValue(infoBt, nameBt))
      val age = Bytes.toString(result._2.getValue(infoBt, ageBt))
      val gender = Bytes.toString(result._2.getValue(infoBt, genderBt))

      Student(name, age, gender)
    })

    import spark.implicits._
    val dataSet = spark.createDataset[Student](newRDD)
    dataSet.show()

    spark.stop()
  }


}
