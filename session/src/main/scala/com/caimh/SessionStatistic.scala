package com.caimh

import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.StringOps
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{Map, mutable}
import scala.util.Random

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
    //    sessionId2FullDataInfo.foreach(println(_))

    //设置自定义累加器,实现所有数据的统计功能，注意累加器也是懒执行的
    val accumulator: SessionStatisticAccumulator = new SessionStatisticAccumulator
    // 注册自定义累加器
    sc.register(accumulator, "SessionStatisticAccumulator")

    //根据查询任务配置，过滤用户行为数据，同时在过滤过程中，对累加器中的数据进行统计
    //sessionId2FilterDataInfo是按照年龄、职业、城市范围、性别、搜索词、点击品类这些条件过滤后的最终结果
    val sessionId2FilterDataInfo: RDD[(String, String)] = filterFullDataInfo(sessionId2FullDataInfo, taskParm, accumulator)

    //    sessionId2FilterDataInfo.foreach(println(_))

    //对数据进行缓存
    sessionId2FilterDataInfo.cache()

    //业务功能一：统计session总数已经各个范围session占比，并写入MySQL
    caculateSessionRatio(spark, accumulator.value, taskUUID)

    //业务功能二：session随机抽取
    sessionRandomExtract(spark, sessionId2FilterDataInfo, taskUUID)

    //获取满足过滤条件的ActionInfo: RDD[(String, UserVisitAction)]
    val sessionId2FilterActionInfoRDD: RDD[(String, UserVisitAction)] = sessionId2ActionRDD.join(sessionId2FilterDataInfo).map {
      case (sessionId, (actionInfo, filterFullDataInfo)) => {
        (sessionId, actionInfo)
      }
    }

    //业务功能三：top10热门品类统计
    val top10CategoryArray: Array[(SortKey, String)] = popularCategoryOfTop10(spark, taskUUID, sessionId2FilterActionInfoRDD)

    //业务功能四：Top10热门品类的Top10活跃session统计(categoryId,sessionId=count)
    top10CategoryActiveSession(spark, taskUUID, top10CategoryArray, sessionId2FilterActionInfoRDD)

    // 关闭Spark上下文
    spark.close()
  }

  /**
    * Top10热门品类的Top10活跃Session统计(只统计click_category)
    *
    * @param spark
    * @param taskUUID
    * @param top10CategoryArray
    * @param sessionId2FilterActionInfoRDD
    * @return
    */
  def top10CategoryActiveSession(spark: SparkSession,
                                 taskUUID: String,
                                 top10CategoryArray: Array[(SortKey, String)],
                                 sessionId2FilterActionInfoRDD: RDD[(String, UserVisitAction)]) = {
    //获取Top10categoryId列表
    val top10CategoryIds: Array[Long] = top10CategoryArray.map {
      case (sortKey, info) =>
        val categoryId: Long = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        categoryId
    }

    //筛选top10CategoryIds范围的Siesson：RDD[sessionId,actionInfo]
    val sessionOfTop10Category: RDD[(String, UserVisitAction)] = sessionId2FilterActionInfoRDD.filter {
      case (sessionId, actionInfo) => {
        val clickCategoryId: Long = actionInfo.click_category_id
        clickCategoryId != -1 && top10CategoryIds.contains(clickCategoryId)
      }
    }

    //聚合RDD[sessionId,IterableActionInfo]->RDD[categoryId,sessionId = count]
    //统计sessinId点击的category次数
    val categoryId2SessionCountRDD: RDD[(Long, String)] = sessionOfTop10Category.groupByKey().flatMap {
      case (sessionId, iterableActionInfo) => {
        //(categoryId,sessionId=count)
        val list: ListBuffer[(Long, String)] = mutable.ListBuffer[(Long, String)]()
        //(categoryId,count)
        val categoryId2CountMap: mutable.HashMap[Long, Int] = mutable.HashMap[Long, Int]()
        for (actionInfo <- iterableActionInfo) {
          val clickCategoryId: Long = actionInfo.click_category_id
          categoryId2CountMap.get(clickCategoryId) match {
            case None => categoryId2CountMap(clickCategoryId) = 1
            case Some(categoryCount) => categoryId2CountMap(clickCategoryId) = categoryId2CountMap(clickCategoryId) + 1
            case _ =>
          }
        }

        for ((categoryId, count) <- categoryId2CountMap) {
          list.append((categoryId, sessionId + "=" + count))
        }

        list
      }
    }
    //聚合groupBykey:RDD[categoryId,"sessionId1=count1,sessionId2=count2,..."]
    //flatMap:RDD[Top10ActiveSession]
    val top10ActiveSessionRDD: RDD[Top10ActiveSession] = categoryId2SessionCountRDD.groupByKey().flatMap {
      case (categoryId, aggrSessionCountInfo) => {
        //排序取top10
        //sortWith排序，true:item1放前面，false：item2放前面。这个与compare排序规则不一样（>0正排，<0倒排）
        val top10ActiveSessionList: List[String] = aggrSessionCountInfo.toList.sortWith(_.split("=")(1).toLong > _.split("=")(1).toLong).take(10)
        top10ActiveSessionList.map {
          case item =>
            val split = item.split("=")
            val sessionId = split(0)
            val sessionCount = split(1).toInt
            Top10ActiveSession(taskUUID, categoryId, sessionId, sessionCount)
        }
      }
    }

//    top10ActiveSessionRDD.foreach(println(_))
    import spark.implicits._
    top10ActiveSessionRDD.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","top10_active_session")
      .mode(SaveMode.Append)
      .save()
  }

  def popularCategoryOfTop10(spark: SparkSession,
                             taskUUID: String,
                             sessionId2FilterActionInfoRDD: RDD[(String, UserVisitAction)]) = {
    //获取所有点击，下单，付款的品类
    var categoryId2CategoryIdRDD: RDD[(Long, Long)] = getFullCategoryIdRDD(sessionId2FilterActionInfoRDD)

    //去重（品类有重复）
    categoryId2CategoryIdRDD = categoryId2CategoryIdRDD.distinct()

    //分类点击次数，下单次数，付款次数
    //获取点击次数
    val categoryId2ClickCountRDD: RDD[(Long, Int)] = getCategoryClickCount(sessionId2FilterActionInfoRDD)

    //获取下单次数
    val categoryId2OrderCountRDD: RDD[(Long, Int)] = getCategoryOrderCount(sessionId2FilterActionInfoRDD)

    //获取付款次数
    val categoryId2PayCountRDD: RDD[(Long, Int)] = getCategoryPayCount(sessionId2FilterActionInfoRDD)

    //(1,categoryid=1|clickCount=139|orderCount=137|payCount=145)
    val categoryId2FullAggrInfoRDD = getFullCountInfoRDD(categoryId2CategoryIdRDD, categoryId2ClickCountRDD, categoryId2OrderCountRDD, categoryId2PayCountRDD)

    //    categoryId2FullAggrInfoRDD.foreach(println(_))

    //二次排序，sortBykey
    val top10CategoryInfoRDD: Array[(SortKey, String)] = categoryId2FullAggrInfoRDD.map {
      case (categoryId, fullAggrInfo) =>
        val categoryId = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_CLICK_COUNT).toInt
        val orderCount = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_ORDER_COUNT).toInt
        val payCount = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_PAY_COUNT).toInt

        (SortKey(clickCount, orderCount, payCount), fullAggrInfo)
    }.sortByKey(false).take(10)

    val top10CategoryRDD: RDD[Top10Category] = spark.sparkContext.makeRDD(top10CategoryInfoRDD).map {
      case (sortkey, aggrInfo) =>
        val categoryId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortkey.clickCount
        val orderCount = sortkey.orderCount
        val payCount = sortkey.payCount

        Top10Category(taskUUID, categoryId, clickCount, orderCount, payCount)
    }
    //    top10CategoryRDD.foreach(println(_))

    //写入数据库
    import spark.implicits._
    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category")
      .mode(SaveMode.Append)
      .save()

    top10CategoryInfoRDD
  }


  def getFullCountInfoRDD(categoryId2CategoryIdRDD: RDD[(Long, Long)],
                          categoryId2ClickCountRDD: RDD[(Long, Int)],
                          categoryId2OrderCountRDD: RDD[(Long, Int)],
                          categoryId2PayCountRDD: RDD[(Long, Int)]) = {

    val categoryId2AggrClickInfoRDD: RDD[(Long, String)] = categoryId2CategoryIdRDD.leftOuterJoin(categoryId2ClickCountRDD).map {
      case (categoryId, (cid, option)) =>
        val clickCount = if (option.isDefined) option.get else 0
        val aggrInfo = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount + "|"
        (categoryId, aggrInfo)
    }

    val categoryId2AggrOrderInfoRDD = categoryId2AggrClickInfoRDD.leftOuterJoin(categoryId2OrderCountRDD).map {
      case (categoryId, (info, option)) =>
        val orderCount = if (option.isDefined) option.get else 0
        val aggrInfo = info + Constants.FIELD_ORDER_COUNT + "=" + orderCount + "|"

        (categoryId, aggrInfo)
    }

    val categoryId2FullInfoRDD = categoryId2AggrOrderInfoRDD.leftOuterJoin(categoryId2PayCountRDD).map {
      case (categoryId, (info, option)) =>
        val payCount = if (option.isDefined) option.get else 0
        val aggrInfo = info + Constants.FIELD_PAY_COUNT + "=" + payCount
        (categoryId, aggrInfo)
    }
    categoryId2FullInfoRDD
  }


  def getCategoryClickCount(sessionId2FilterActionInfoRDD: RDD[(String, UserVisitAction)]) = {
    //先过滤满足条件的
    val categoryClickRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionInfoRDD.filter(_._2.click_category_id != -1)

    val categoryClickCountRDD: RDD[(Long, Int)] = categoryClickRDD.map(item => (item._2.click_category_id, 1)).reduceByKey(_ + _)
    categoryClickCountRDD
  }

  def getCategoryOrderCount(sessionId2FilterActionInfoRDD: RDD[(String, UserVisitAction)]) = {
    //先过滤满足条件的
    val categoryOrderRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionInfoRDD.filter(_._2.order_category_ids != null)

    //下单次数
    categoryOrderRDD.flatMap {
      case (sessionId, actionInfo) =>
        val categoryOrderCountBuffer: ArrayBuffer[(Long, Int)] = new ArrayBuffer[(Long, Int)]()
        for (categoryId <- actionInfo.order_category_ids.split(",")) {
          categoryOrderCountBuffer.append((categoryId.toLong, 1))
        }
        categoryOrderCountBuffer
    }.reduceByKey(_ + _)
  }

  def getCategoryPayCount(sessionId2FilterActionInfoRDD: RDD[(String, UserVisitAction)]) = {
    //满足过滤条件的
    val categoryPayRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionInfoRDD.filter(_._2.pay_category_ids != null)

    //付款次数
    val categoryPayCountRDD: RDD[(Long, Int)] = categoryPayRDD.flatMap {
      case (sessionId, actionInfo) =>
        val categoryPayCount: ArrayBuffer[(Long, Int)] = new ArrayBuffer[(Long, Int)]()
        for (categoryId <- actionInfo.pay_category_ids.split(",")) {
          categoryPayCount.append((categoryId.toLong, 1))
        }
        categoryPayCount
    }.reduceByKey(_ + _)
    categoryPayCountRDD
  }

  def getFullCategoryIdRDD(sessionId2FilterActionInfoRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    sessionId2FilterActionInfoRDD.flatMap {
      case (sessionId, actionInfo) => {
        val fullCategoryIds: ArrayBuffer[(Long, Long)] = new ArrayBuffer[(Long, Long)]()

        //点击品类
        if (actionInfo.click_category_id != -1) {
          //点击品类
          fullCategoryIds.append((actionInfo.click_category_id, actionInfo.click_category_id))
        } else if (actionInfo.order_category_ids != null) {
          //下单品类
          val orderCategoryIds: Array[String] = actionInfo.order_category_ids.split(",")
          for (categoryId <- orderCategoryIds) {
            fullCategoryIds.append((categoryId.toLong, categoryId.toLong))
          }
        } else if (actionInfo.pay_category_ids != null) {
          //付款品类
          val payCategoryIds: Array[String] = actionInfo.pay_category_ids.split(",")
          for (categoryId <- payCategoryIds) {
            fullCategoryIds.append((categoryId.toLong, categoryId.toLong))
          }
        }
        //满足条件全部品类
        fullCategoryIds
      }
    }
  }

  def sessionRandomExtract(spark: SparkSession, sessionId2FilterDataInfo: RDD[(String, String)], taskid: String): Unit = {
    //sessionId2FilterDataInfo->dateHour2FullDataInfoRDD：（dateHour,fullDataInfo）
    val dateHour2FullDataInfoRDD: RDD[(String, String)] = sessionId2FilterDataInfo.map {
      case (sessionId, fullDataInfo) => {
        val startTime: String = StringUtils.getFieldFromConcatString(fullDataInfo, "\\|", Constants.FIELD_START_TIME)
        //TODO startTime为null的判断
        val dateHour: String = DateUtils.getDateHour(startTime)
        (dateHour, fullDataInfo)
      }
    }

    //countBykey->dateHourCountMap->date2HourCountMap[date,[hour,count]]
    val dateHour2CountMap: Map[String, Long] = dateHour2FullDataInfoRDD.countByKey()
    val date2HourCountMap = mutable.HashMap[String, mutable.HashMap[String, Long]]()
    for ((dateHour, count) <- dateHour2CountMap) {
      //dateHour格式为yyyy-MM-dd_HH
      val dateHourSplit: Array[String] = dateHour.split("_")
      val date: String = dateHourSplit(0)
      val hour: String = dateHourSplit(1)

      date2HourCountMap.get(date) match {
        case None => {
          date2HourCountMap(date) = new mutable.HashMap[String, Long]()
          date2HourCountMap(date) += (hour -> count)
        }
        case Some(hourCountMap) => {
          hourCountMap += (hour -> count)
        }
      }
    }

    //sessionExtRandomIndexMap[date,[hour,List]]
    //每天抽取的Session数量，每天的session总数，每小时的session数量，每小时抽取的session数量
    val extrSessionPerDay: Int = 100 / date2HourCountMap.size
    val extrSessionRandomIndexMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()
    val random: Random = new Random()
    for ((date, hourCountMap) <- date2HourCountMap) {
      //每小时随机抽样索引
      extrSessionRandomIndexMap.get(date) match {
        case None => {
          extrSessionRandomIndexMap(date) = new mutable.HashMap[String, mutable.ListBuffer[Int]]()
          extrHourSessionIndex(extrSessionPerDay, hourCountMap, extrSessionRandomIndexMap(date))
        }
        case Some(extractHourIndexMap) => {
          extrHourSessionIndex(extrSessionPerDay, hourCountMap, extrSessionRandomIndexMap(date))
        }
      }
    }

    //sessionExtRandomIndexMap广播出去
    val extrSessionRandomIndexMapbd: Broadcast[mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]] = spark.sparkContext.broadcast(extrSessionRandomIndexMap)

    //dateHour2FullDataInfoRDD执行groupBykey->dateHour2GroupRDD,获取每个小时所有的session数据
    //dateHour2FullDataInfoRDD:[dataHour,fullDataInfo]
    //dateHour2GroupRDD:[dateHour,Iterable[fullDataInfo]]
    //dateHour2GroupRDD执行抽取操作
    val dateHour2GroupRDD: RDD[(String, Iterable[String])] = dateHour2FullDataInfoRDD.groupByKey()
    val sessionRandomExtractRDD: RDD[SessionRandomExtract] = dateHour2GroupRDD.flatMap {
      case (dateHour, iteraFullDataInfo) => {
        val split: mutable.ArrayOps[String] = dateHour.split("_")
        val date: StringOps = split(0)
        val hour: StringOps = split(1)
        var index = 0
        val dateHourExtractIndexMap = extrSessionRandomIndexMapbd.value
        val indexList: ListBuffer[Int] = dateHourExtractIndexMap(date)(hour)
        val sessionRandomExt: ArrayBuffer[SessionRandomExtract] = mutable.ArrayBuffer[SessionRandomExtract]()
        for (fullDataInfo <- iteraFullDataInfo) {
          if (indexList.contains(index)) {
            val sessionId: String = StringUtils.getFieldFromConcatString(fullDataInfo, "\\|", Constants.FIELD_SESSION_ID)
            val startTime: String = StringUtils.getFieldFromConcatString(fullDataInfo, "\\|", Constants.FIELD_START_TIME)
            val searchKeywords: String = StringUtils.getFieldFromConcatString(fullDataInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            val clickCategoryIds: String = StringUtils.getFieldFromConcatString(fullDataInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
            sessionRandomExt.append(SessionRandomExtract(taskid, sessionId, startTime, searchKeywords, clickCategoryIds))
          }
          index += 1
        }
        sessionRandomExt
      }
    }

    //    sessionRandomExtractRDD.foreach(println(_))

    //     引入隐式转换，准备进行RDD向Dataframe的转换
    import spark.implicits._
    // 为了方便地将数据保存到MySQL数据库，将RDD数据转换为Dataframe
    sessionRandomExtractRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_random_extract")
      .mode(SaveMode.Append)
      .save()
  }

  /**
    * 获取随机抽取Session的索引列表
    *
    * @param extrSessionPerDay   平均每天抽样的session数量
    * @param hourCountMap        小时session数量（小时，session数量）
    * @param extractHourIndexMap 随机从session列表抽取小时session对应的index
    */
  def extrHourSessionIndex(extrSessionPerDay: Int,
                           hourCountMap: mutable.HashMap[String, Long],
                           extractHourIndexMap: mutable.HashMap[String, ListBuffer[Int]]) = {
    //当前天的session总数
    val dateSessionCount: Long = hourCountMap.values.sum
    val random: Random = new Random()
    var index = 0
    for ((hour, count) <- hourCountMap) {
      //每小时抽取session数量
      var extrHourSessionCount: Int = ((count / dateSessionCount).toDouble * extrSessionPerDay).toInt
      if (extrHourSessionCount > count) {
        extrHourSessionCount = count.toInt
      }

      //通过模式匹配实现有则追加，无则新增
      extractHourIndexMap.get(hour) match {
        case None => {
          extractHourIndexMap(hour) = new ListBuffer[Int]()
          for (i <- 0 to extrHourSessionCount) {
            var extractHourIndex: Int = random.nextInt(count.toInt)
            while (extractHourIndexMap(hour).contains(extractHourIndex)) {
              extractHourIndex = random.nextInt(count.toInt)
            }
            extractHourIndexMap(hour).append(extractHourIndex)
          }
        }
        case Some(extractHourIndexList) => {
          for (i <- 0 to extrHourSessionCount) {
            var extractHourIndex: Int = random.nextInt(count.toInt)
            while (extractHourIndexMap(hour).contains(extractHourIndex)) {
              extractHourIndex = random.nextInt(count.toInt)
            }
            extractHourIndexMap(hour).append(extractHourIndex)
          }
        }
      }
    }
  }

  def caculateSessionRatio(spark: SparkSession, value: mutable.HashMap[String, Int], taskUUID: String): Unit = {
    //统计session访问时长，访问步长各个范围占比
    val sessionCount: Double = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

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
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

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
