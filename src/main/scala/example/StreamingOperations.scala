package example


import org.apache.spark.streaming.{State, StateSpec}
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream}

class StreamingOperations {

  def update(dStream: DStream[(Int, String)]): DStream[(Int, String)] = {
    dStream.map {
      case (key, value) => (key, s"""{"value":[$value]}""")
    }
  }

  def distinct(dStream: DStream[(Int, String)]): DStream[(Int, String)] = {
    val optionTupleStream: DStream[Option[(Int, String)]] = dStream.mapWithState(StateSpec.function(dedup))

    optionTupleStream .flatMap {
        case Some(value) => Seq(value)
        case _ => Seq()
      }
  }

  val dedup: (Int, Option[String], State[List[Int]]) => Option[(Int, String)] =
    (key: Int, value: Option[String], state: State[List[Int]]) => {
      (value, state.getOption()) match {
        case (Some(data), Some(keys)) if !keys.contains(key) =>
          state.update(key :: keys)
          Some(key, data)
        case (Some(data), None) =>
          state.update(List(key))
          Some(key, data)
        case _ =>
          None
      }
    }


  def allowEvens(pair: Tuple2[Int,Int] ): Option[(Int, Int)] = pair match {
    case (first, second) if first % 2 == 0 => Some(pair)
    case _ => None
    }



  def go(): Unit = {
    val listo = List( (1,2), (2,3), (3,4))
    val result = listo.map(allowEvens).flatMap {
      case Some(value) => Seq(value)
      case _ => Seq()
    }

    System.out.println("result:" + result);
  }
}



