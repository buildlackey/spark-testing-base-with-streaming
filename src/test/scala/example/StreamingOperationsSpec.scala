package example

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.scalatest.WordSpec

class StreamingOperationsSpec extends WordSpec with StreamingSuiteBase {

  val streamingOperations = new StreamingOperations

  "StreamingOperations" should {

    "remove duplicates" in {
      val inputPair = List(List((1, "the word the"), (1, "the word the")))
      val pair = List(List((1, "the word the")))

      testOperation(inputPair, streamingOperations.distinct _, pair, ordered = false)
    }

    "update stream" in {
      val inputPair = List(List((1, """{"name": "anuj"}, {"name": "raman"}""")))
      val pair = List(List((1, """{"value":[{"name": "anuj"}, {"name": "raman"}]}""")))

      testOperation(inputPair, streamingOperations.update _, pair, ordered = false)
    }
  }
}