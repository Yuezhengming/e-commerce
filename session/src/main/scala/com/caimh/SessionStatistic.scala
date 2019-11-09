package com.caimh

import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.collection.mutable

/**
  * Created by caimh on 2019/11/8.
  */
object SessionStatistic {


  def main(args: Array[String]) {

    //读取配置文件
    val jsonStr: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParm: JSONObject = JSONObject.fromObject(jsonStr)

    //每个Spark任务（Driver）生成一个独一无二的主键（UUID）,存入数据库，查询可以得到不同的Spark任务的数据
    val taskUUID: String = UUID.randomUUID().toString

    //Spark相关
    // SparkSession
    val conf: SparkConf = new SparkConf().setAppName("SessionStatistic").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //从Hive读取数据
    //actionRDD:RDD[UserVisitAction]
    val actionRDD: RDD[UserVisitAction] = getActionRDD(spark, taskParm)

    //sessionId2ActionRDD:RDD[(String, UserVisitAction)]
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map(userVisitAction => (userVisitAction.session_id, userVisitAction))
    //sessionId2GroupRDD:RDD[(String, Iterable[UserVisitAction])]
    val sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()

    //公司跑集群需要设置
    //    spark.sparkContext.setCheckpointDir("集群HDFS路径")
    sessionId2GroupRDD.cache()
    //    sessionId2GroupRDD.checkpoint()

    //获取完整信息数据:(k,v)=(sessionId,session_id|Search_Keywords|Click_Category_Id|Visit_Length|Step_Size|Start_Time|Age|Professional|Sex|City)
    val sessionId2FullDataInfo: RDD[(String, String)] = getAggrFullDataInfo(spark, sessionId2GroupRDD)
        sessionId2FullDataInfo.foreach(println(_))

    //设置自定义累加器,实现所有数据的统计功能，注意累加器也是懒执行的
    val accumulator: SessionStatisticAccumulator = new SessionStatisticAccumulator
    // 注册自定义累加器
    sc.register(accumulator, "SessionStatisticAccumulator")

    //根据查询任务配置，过滤用户行为数据，同时在过滤过程中，对累加器中的数据进行统计
    //sessionId2FilterDataInfo是按照年龄、职业、城市范围、性别、搜索词、点击品类这些条件过滤后的最终结果
    val sessionId2FilterDataInfo: RDD[(String, String)] = filterFullDataInfo(sessionId2FullDataInfo, taskParm, accumulator)

    sessionId2FilterDataInfo.foreach(println(_))

    //对数据进行缓存
    sessionId2FilterDataInfo.cache()

    //业务功能一：统计session总数已经各个范围session占比，并写入MySQL
    caculateSessionRatio(spark, accumulator.value, taskUUID)
  }

  def caculateSessionRatio(spark: SparkSession, value: mutable.HashMap[String, Int], taskUUID: String): Unit = {
    //统计session访问时长，访问步长各个范围占比
    val sessionCount: Double = value.getOrElse(Constants.SESSION_COUNT,1).toDouble

    //各个范围访问时长visit_length统计
    val visit_length_1s_3s: Int = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s: Int = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s: Int = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s: Int = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s: Int = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m: Int = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10: Int = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m: Int = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m: Int = value.getOrElse(Constants.TIME_PERIOD_30m, 0)
    //各个范围访问步长step_size统计
    val step_size_1_3: Int = value.getOrElse(Constants.STEP_SIZE_1_3, 0)
    val step_size_4_6: Int = value.getOrElse(Constants.STEP_SIZE_4_6, 0)
    val step_size_7_9: Int = value.getOrElse(Constants.STEP_SIZE_7_9, 0)
    val step_size_10_30: Int = value.getOrElse(Constants.STEP_SIZE_10_30, 0)
    val step_size_30_60: Int = value.getOrElse(Constants.STEP_SIZE_30_60, 0)
    val step_size_60: Int = value.getOrElse(Constants.STEP_SIZE_60, 0)

    //各个范围访问时长visit_length占比统计
    val visit_length_1s_3s_ratio: Double = NumberUtils.formatDouble(visit_length_1s_3s / sessionCount, 2)
    val visit_length_4s_6s_ratio: Double = NumberUtils.formatDouble(visit_length_4s_6s / sessionCount, 2)
    val visit_length_7s_9s_ratio: Double = NumberUtils.formatDouble(visit_length_7s_9s / sessionCount, 2)
    val visit_length_10s_30s_ratio: Double = NumberUtils.formatDouble(visit_length_10s_30s / sessionCount, 2)
    val visit_length_30s_60s_ratio: Double = NumberUtils.formatDouble(visit_length_30s_60s / sessionCount, 2)
    val visit_length_1m_3m_ratio: Double = NumberUtils.formatDouble(visit_length_1m_3m / sessionCount, 2)
    val visit_length_3m_10m_ratio: Double = NumberUtils.formatDouble(visit_length_3m_10 / sessionCount, 2)
    val visit_length_10m_30m_ratio: Double = NumberUtils.formatDouble(visit_length_10m_30m / sessionCount, 2)
    val visit_length_30m_ratio: Double = NumberUtils.formatDouble(visit_length_30m / sessionCount, 2)

    //各个范围访问步长step_size占比统计
    val step_size_1_3_ratio: Double = NumberUtils.formatDouble(step_size_1_3 / sessionCount, 2)
    val step_size_4_6_ratio: Double = NumberUtils.formatDouble(step_size_4_6 / sessionCount, 2)
    val step_size_7_9_ratio: Double = NumberUtils.formatDouble(step_size_7_9 / sessionCount, 2)
    val step_size_10_30_ratio: Double = NumberUtils.formatDouble(step_size_10_30 / sessionCount, 2)
    val step_size_30_60_ratio: Double = NumberUtils.formatDouble(step_size_30_60 / sessionCount, 2)
    val step_size_60_ratio: Double = NumberUtils.formatDouble(step_size_60 / sessionCount, 2)

    //统计结果封装成Domain对象
    val sessionAggrStat: SessionAggrStatistic = SessionAggrStatistic(taskUUID,
      sessionCount.toInt,
      visit_length_1s_3s_ratio,
      visit_length_4s_6s_ratio,
      visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio,
      visit_length_30s_60s_ratio,
      visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio,
      visit_length_10m_30m_ratio,
      visit_length_30m_ratio,
      step_size_1_3_ratio,
      step_size_4_6_ratio,
      step_size_7_9_ratio,
      step_size_10_30_ratio,
      step_size_30_60_ratio,
      step_size_60_ratio)

    //插入Mysql
    import spark.implicits._
    val sessionAggrStatRDD: RDD[SessionAggrStatistic] = spark.sparkContext.makeRDD(Array(sessionAggrStat))
    sessionAggrStatRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_aggr_statistic")
      .mode(SaveMode.Append)
      .save()
  }

  def getActionRDD(spark: SparkSession, taskParm: JSONObject): RDD[UserVisitAction] = {
    val startDate: String = ParamUtils.getParam(taskParm, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParm, Constants.PARAM_END_DATE)
    val sql = "Select * from user_visit_action where date>= '" + startDate + "'and date <='" + endDate + "'"

    import spark.implicits._
    val actionRDD: RDD[UserVisitAction] = spark.sql(sql).as[UserVisitAction].rdd
    actionRDD
  }

  def getAggrFullDataInfo(spark: SparkSession, sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    //获取聚合信息:（k,v）=(userId,session_id|Search_Keywords|Click_Category_Id|Visit_Length|Step_Size|Start_Time)
    val sessionId2AggrDataInfoRDD: RDD[(Long, String)] = sessionId2GroupRDD.map {
      case (sid, iterableActon) => {
        //TODO iterableActon需要判空吗？
        var startTime: Date = null
        var endTime: Date = null

        val userId = iterableActon.head.user_id
        val searchKeywords = new StringBuilder()
        val clickCategoryIds = new StringBuilder()
        var visitLength = 0.0f
        var stepSize = 0

        for (userVisitAction <- iterableActon) {
          //startTime/endTime
          val actionTime = DateUtils.parseTime(userVisitAction.action_time)
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }
          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }

          //searchKeywords
          val search_keyword: String = userVisitAction.search_keyword
          if (StringUtils.isNotEmpty(search_keyword)
            && !searchKeywords.toString().contains(search_keyword)) {
            searchKeywords.append(search_keyword).append(",")
          }

          //clickCategoryIds
          val click_category_id: Long = userVisitAction.click_category_id
          if (click_category_id != -1l && !clickCategoryIds.toString().contains(click_category_id.toString)) {
            clickCategoryIds.append(click_category_id.toString).append(",")
          }

          //stepSize,计算访问步长
          stepSize += 1
        }

        val searchKeywords2Str: String = StringUtils.trimComma(searchKeywords.toString())
        val clickCategoryIds2Str: String = StringUtils.trimComma(clickCategoryIds.toString())

        //visitLength，计算session访问时长（秒）
        visitLength = (endTime.getTime - startTime.getTime) / 1000

        //拼接
        val aggrDataInfo = Constants.FIELD_SESSION_ID + "=" + sid + "|" + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords2Str + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds2Str + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_SIZE + "=" + stepSize + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatDate(startTime)

        (userId, aggrDataInfo)
      }
    }

    //联立userInfo,获取完整信息:(k,v)=(sessionId,session_id|Search_Keywords|Click_Category_Id|Visit_Length|Step_Size|Start_Time|Age|Professional|Sex|City)
    import spark.implicits._
    val userInfoRDD: RDD[UserInfo] = spark.sql("select * from user_info").as[UserInfo].rdd
    val userId2UserInfoRDD: RDD[(Long, UserInfo)] = userInfoRDD.map(userInfo => (userInfo.user_id, userInfo))
    sessionId2AggrDataInfoRDD.join(userId2UserInfoRDD).map {
      case (userId, (aggrDataInfo, userInfo)) => {
        val fullDataInfo = aggrDataInfo + "|" + Constants.FIELD_AGE + "=" + userInfo.age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
          Constants.FIELD_SEX + "=" + userInfo.sex + "|" +
          Constants.FIELD_CITY + "=" + userInfo.city

        val sessionId: String = StringUtils.getFieldFromConcatString(fullDataInfo, "\\|", Constants.FIELD_SESSION_ID)
        (sessionId, fullDataInfo)
      }
    }
  }

  def filterFullDataInfo(sessionId2FullDataInfo: RDD[(String, String)], taskParm: JSONObject, accumulator: SessionStatisticAccumulator) = {

    //过滤条件
    val startAge: AnyRef = taskParm.get(Constants.PARAM_START_AGE)
    val endAge: AnyRef = taskParm.get(Constants.PARAM_END_AGE)
    val professionals: AnyRef = taskParm.get(Constants.PARAM_PROFESSIONALS)
    val citys: AnyRef = taskParm.get(Constants.PARAM_CITIES)
    val sex: AnyRef = taskParm.get(Constants.PARAM_SEX)
    val keywords: AnyRef = taskParm.get(Constants.PARAM_KEYWORDS)
    val categoryIds: AnyRef = taskParm.get(Constants.PARAM_CATEGORY_IDS)

    var parameter = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (citys != null) Constants.PARAM_CITIES + "=" + citys + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds + "|" else "")

    if (StringUtils.isEmpty(parameter)) {
      true
    } else if (parameter.endsWith("\\|")) {
      parameter = parameter.substring(0, parameter.length - 1)
    }

    val sessionId2FilterDataInfo: RDD[(String, String)] = sessionId2FullDataInfo.filter {
      case (sessionId, fullDataInfo) => {
        //过滤条件
        if (!ValidUtils.between(fullDataInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          false
        } else if (!ValidUtils.in(fullDataInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
          false
        } else if (!ValidUtils.in(fullDataInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
          false
        } else if (!ValidUtils.in(fullDataInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
          false
        } else if (!ValidUtils.in(fullDataInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
          false
        } else if (!ValidUtils.in(fullDataInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
          false
        } else {
          //统计SESSION总数
          accumulator.add(Constants.SESSION_COUNT)

          def caculateVisitLengthCount(visit_length: Double) = {
            if (visit_length >= 1 && visit_length <= 3) {
              accumulator.add(Constants.TIME_PERIOD_1s_3s)
            } else if (visit_length >= 4 && visit_length <= 6) {
              accumulator.add(Constants.TIME_PERIOD_4s_6s)
            } else if (visit_length >= 7 && visit_length <= 9) {
              accumulator.add(Constants.TIME_PERIOD_7s_9s)
            } else if (visit_length >= 10 && visit_length <= 30) {
              accumulator.add(Constants.TIME_PERIOD_10s_30s)
            } else if (visit_length > 30 && visit_length <= 60) {
              accumulator.add(Constants.TIME_PERIOD_30s_60s)
            } else if (visit_length > 60 && visit_length <= 180) {
              accumulator.add(Constants.TIME_PERIOD_1m_3m)
            } else if (visit_length > 180 && visit_length <= 600) {
              accumulator.add(Constants.TIME_PERIOD_3m_10m)
            } else if (visit_length > 600 && visit_length <= 1800) {
              accumulator.add(Constants.TIME_PERIOD_10m_30m)
            } else if (visit_length > 1800) {
              accumulator.add(Constants.TIME_PERIOD_30m)
            } else {
              //TODO
            }
          }

          def caculateStepSizeCount(stepSize: Double) = {
            if (stepSize >= 1 && stepSize <= 3) {
              accumulator.add(Constants.STEP_SIZE_1_3)
            } else if (stepSize >= 4 && stepSize <= 6) {
              accumulator.add(Constants.STEP_SIZE_4_6)
            } else if (stepSize >= 7 && stepSize <= 9) {
              accumulator.add(Constants.STEP_SIZE_7_9)
            } else if (stepSize >= 10 && stepSize <= 30) {
              accumulator.add(Constants.STEP_SIZE_10_30)
            } else if (stepSize > 30 && stepSize <= 60) {
              accumulator.add(Constants.STEP_SIZE_30_60)
            } else if (stepSize > 60) {
              accumulator.add(Constants.STEP_SIZE_60)
            } else {
              //TODO
            }
          }

          //统计各个visit_length的session总数
          val visit_length: Double = StringUtils.getFieldFromConcatString(fullDataInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toDouble
          caculateVisitLengthCount(visit_length)

          //统计各个step_size的session总数
          val stepSize: Double = StringUtils.getFieldFromConcatString(fullDataInfo, "\\|", Constants.FIELD_STEP_SIZE).toDouble
          caculateStepSizeCount(stepSize)

        }
          true
      }
    }
    sessionId2FilterDataInfo
  }
}
