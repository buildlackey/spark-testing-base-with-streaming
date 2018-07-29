package example

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, StreamingSuiteBase}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.scalatest.WordSpec
import org.apache.spark.sql.types.StringType

import org.scalatest.FunSpec





class ColumnAdderSpec
    extends FunSpec
    with DataFrameSuiteBase {

  describe("withGreenPokemon") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    it("appends a green_pokemon column to a DataFrame") {

      val sourceDF: DataFrame = sc.parallelize(
        List(
          "grass",
          "flower"
        )
      ).toDF()

      val expectedDF: DataFrame = sc.parallelize(
        List(
          ("grass", "bulba bulba"),
          ("flower", "bulba bulba")
        )
      ).toDF()

      val actualDF = sourceDF.transform(ColumnAdder.withGreenPokemon())
      assertDataFrameEquals(actualDF, expectedDF)
    }

  }
}
