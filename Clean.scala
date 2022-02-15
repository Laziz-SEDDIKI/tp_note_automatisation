import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Clean {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("Clean data")
      .master("local[*]")
      .getOrCreate()

    //val inputFile = args(0)
    //val outputFile = args(1)
    val inputFile="C:/Users/lounhadja/spark_stram/src/main/data/data.csv"  //TO TEST
    val outputFile="C:/Users/lounhadja/spark_stram/src/main/data/data___1.csv"  //TO TEST

    val df = spark.read.option("header", true).format("csv")
                      .csv(inputFile)
    //df.show()
    val new_df=df.withColumn("date_debut", col("date_debut").cast("Date"))
      .withColumn("year", year(col("date_debut")))
      .withColumn("month", month(col("date_debut")))
      .withColumn("day", month(col("date_debut")))

    print(new_df.printSchema())
    new_df.show()

    new_df.write.format("csv")
                .save(outputFile)
  }
}
