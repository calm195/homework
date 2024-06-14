import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wjx
 * @date 2024/6/13 13:33
 */
object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.setAppName("CalUserBitmapIndex").setMaster("local");
    val sc = new SparkContext(conf);
    val spark = SparkSession.builder().config(conf).getOrCreate();
    val df = spark.createDataFrame(Seq(
      (1, 2, 3),
      (31, 32, 33),
      (4, 5, 6)
    )).toDF("id", "age", "imsi")
    df.createOrReplaceTempView("test")
    spark.sql("select * from test where id > 3").show()

  }

}
