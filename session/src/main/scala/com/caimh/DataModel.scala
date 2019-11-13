package com.caimh

/**
  *
  * @param taskid
  * @param session_count
  * @param visit_length_1s_3s_ratio
  * @param visit_length_4s_6s_ratio
  * @param visit_length_7s_9s_ratio
  * @param visit_length_10s_30s_ratio
  * @param visit_length_30s_60s_ratio
  * @param visit_length_1m_3m_ratio
  * @param visit_length_3m_10m_ratio
  * @param visit_length_10m_30m_ratio
  * @param visit_length_30m_ratio
  * @param step_size_1_3_ratio
  * @param step_size_4_6_ratio
  * @param step_size_7_9_ratio
  * @param step_size_10_30_ratio
  * @param step_size_30_60_ratio
  * @param step_size_60_ratio
  */
case class SessionAggrStatistic(
                                 taskid: String,
                                 session_count: Int,
                                 visit_length_1s_3s_ratio: Double,
                                 visit_length_4s_6s_ratio: Double,
                                 visit_length_7s_9s_ratio: Double,
                                 visit_length_10s_30s_ratio: Double,
                                 visit_length_30s_60s_ratio: Double,
                                 visit_length_1m_3m_ratio: Double,
                                 visit_length_3m_10m_ratio: Double,
                                 visit_length_10m_30m_ratio: Double,
                                 visit_length_30m_ratio: Double,
                                 step_size_1_3_ratio: Double,
                                 step_size_4_6_ratio: Double,
                                 step_size_7_9_ratio: Double,
                                 step_size_10_30_ratio: Double,
                                 step_size_30_60_ratio: Double,
                                 step_size_60_ratio: Double
                               )

/**
  * session随机抽取表
  *
  * @param taskid           当前计算批次的ID
  * @param sessionid        抽取的session的ID
  * @param startTime        session的开始时间
  * @param searchKeywords   session的查询字段
  * @param clickCategoryIds session点击的类别id集合
  */
case class SessionRandomExtract(
                                 taskid: String,
                                 sessionid: String,
                                 startTime: String,
                                 searchKeywords: String,
                                 clickCategoryIds: String
                               )

case class Top10Category(taskId: String,
                         categoryId: Long,
                         clickCount: Int,
                         orderCount: Int,
                         payCount: Int)

case class SortKey(clickCount: Int, orderCount: Int, payCount: Int) extends Ordered[SortKey] {
  //按照点击次数，下单次数，付款次数排序
  override def compare(that: SortKey): Int = {
    if (this.clickCount - that.clickCount != 0) {
      return this.clickCount - that.clickCount
    } else if (this.orderCount - that.orderCount != 0) {
      return this.orderCount - that.orderCount
    } else {
      return this.payCount - that.payCount
    }
  }
}

case class Top10ActiveSession(taskId: String,
                              categoryId: Long,
                              sessionId: String,
                              clickCount: Int)