import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, window, when}


//package esgi.circulation

object Jointure {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("JoinOperation")
      .master("local[*]")
      .getOrCreate()

    //val inputFile = args(0)
    //val joinFile = args(1)
    //val outputFile = args(2)

    val inputFile = "C:/Users/lounhadja/spark_stram/src/main/data/data.csv"
    val joinFile = "C:/Users/lounhadja/spark_stram/src/main/data/test1.parquet"
    val outputFile = "C:/Users/lounhadja/spark_stram/src/main/data/data___2.csv"

    //TODO : lire son fichier d'input et son fichier de jointure
    val df = spark.read.parquet(joinFile)
    val joinDf = spark.read.option("header", true).format("csv")
          .csv(inputFile)
    println(df.schema)
    println(joinDf.schema)

    //Jointure
    val df_join_result=df.join(joinDf, "iu_ac")
    df_join_result.show()
    df_join_result.write.format("csv")
      .save(outputFile)

  }
}
