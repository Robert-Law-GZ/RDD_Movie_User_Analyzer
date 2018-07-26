import org.apache.spark._
import org.apache.spark.streaming._

object StreamingSample {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[2]").setAppName("Streaming Sample")
    val ssc=new StreamingContext(conf,Seconds(1))
    val sc=ssc.sparkContext;

    sc.setLogLevel("warn")

    val lines=ssc.socketTextStream("127.0.0.1",9999)

    val words=lines.flatMap(_.split(" "))

    val pairs=words.map(word=>(word,1))
    val wordCounts=pairs.reduceByKey(_+_)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
