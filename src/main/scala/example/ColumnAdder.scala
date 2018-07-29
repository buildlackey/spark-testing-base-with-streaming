package example


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ColumnAdder {

  def withGreenPokemon()(df: DataFrame): DataFrame = {
    df.withColumn(
      "green_pokemon",
      lit("bulba bulba")
    )
  }
}

