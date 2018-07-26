import org.apache.hadoop.hbase.client.Result
import org.apache.spark.streaming._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf}
import org.apache.hadoop.hbase.util.Bytes

object StreamingFromRDDSample {

  def main(args: Array[String]): Unit = {
    val conf=HBaseConfiguration.create()
    val sc_conf=new SparkConf().setMaster("local[2]").setAppName("Streaming Sample")
    val ssc=new StreamingContext(sc_conf,Seconds(1))

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE,"student")
    val RDD=ssc.sparkContext.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])

    val count=RDD.count()
    println("Students RDD Count:"+count)
    RDD.cache()
    //遍历输出
    RDD.foreach({case (_,result)=>
      val key=Bytes.toString(result.getRow)
      val name=Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
      val gender=Bytes.toString(result.getValue("info".getBytes,"gender".getBytes))
      val age=Bytes.toString(result.getValue("info".getBytes,"age".getBytes))
      println("ROW:"+key+" name: "+name+" Gender: "+gender+" Age: "+age)
    })
    
  }

}
