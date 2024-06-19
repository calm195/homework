import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}

import scala.collection.immutable.Seq

/**
 * @author wjx
 * @date 2024/6/14 16:27
 */
object RemoteReadHDFS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.setAppName("RemoteReadHDFS").setMaster("local");

    val sparkSession = SparkSession.builder()
      .appName("RemoteReadHDFS")
      .config(conf)
      .getOrCreate();
    // 步骤2: 设置Hadoop配置
    val laccellSignFilePath = "hdfs://bigdata01:9000/data/laccellSign/291121/23/FlumeData.1889968379171"
    val laccellSignSchema = new StructType()
      .add("imsi",StringType,false)
      .add("laccell",StringType,false)
      .add("latitude",DoubleType,false)
      .add("longitude",DoubleType,false)
      .add("procedureStartTime",LongType,false)
    val laccellSignDf = sparkSession
      .read
      .format("csv")
      .option("sep","|")
      .schema(laccellSignSchema)
      .load(laccellSignFilePath)
    laccellSignDf.createOrReplaceTempView("laccell_sign")

    val execSQL =
      """
        |	SELECT *
        |	FROM laccell_sign""".stripMargin
    val resDf = sparkSession.sql(execSQL)
    resDf.show()
  }
}
