import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
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
    val tn=TableName.valueOf(table_name);

    val table = conn.getTable(tn);

    val p = new Put(new String("4").getBytes)
    p.addColumn("info".getBytes(), "name".getBytes(), "AAAA".getBytes())
    p.addColumn("info".getBytes(), "age".getBytes(), "34".getBytes())
    p.addColumn("info".getBytes(), "gender".getBytes(), "男".getBytes())
    table.put(p)

    spark.stop()
  }

}
