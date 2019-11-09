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
