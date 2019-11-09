package com.caimh

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.HashMap

/**
  * 1.统计session总数
  * 2.统计各个step_size的session总数
  * 3.统计各个visit_length的session总数
  *
  * Created by caimh on 2019/11/8.
  */
class SessionStatisticAccumulator extends AccumulatorV2[String, HashMap[String, Int]] {

  private val sessionCountMap: HashMap[String, Int] = new HashMap[String, Int]()

  override def isZero: Boolean = sessionCountMap.isEmpty

  override def merge(other: AccumulatorV2[String, HashMap[String, Int]]): Unit = {

    other match {
      case acc: SessionStatisticAccumulator => {
        (this.sessionCountMap /: acc.value) {
          case (map, (k, v)) => map += (k -> (v + map.getOrElse(k, 0)))
        }
      }
      case _ =>
    }
  }

  override def copy(): AccumulatorV2[String, HashMap[String, Int]] = {
    val accumulator: SessionStatisticAccumulator = new SessionStatisticAccumulator
    sessionCountMap.synchronized {
      accumulator.sessionCountMap ++= sessionCountMap
    }
    accumulator
  }

  override def value: HashMap[String, Int] = sessionCountMap

  override def add(v: String): Unit = {
    if (!sessionCountMap.contains(v)) {
      sessionCountMap += (v -> 1)
      return
    }
    sessionCountMap.update(v, sessionCountMap(v) + 1)
  }

  override def reset(): Unit = {
    sessionCountMap.clear()
  }
}
