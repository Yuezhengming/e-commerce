import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.{Map, mutable}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by caimh on 2019/11/13.
  */
object PageConversionRateStat {

  def main(args: Array[String]) {

    //限制条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam: JSONObject = JSONObject.fromObject(jsonStr)

    //获取唯一主键
    val taskUUID: String = UUID.randomUUID().toString

    //创建sparkConf
    val conf: SparkConf = new SparkConf().setAppName("PageConversionRateStat").setMaster("local[*]")

    //创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //获取用户行为数据userVisitAction
    val sessionId2UserActionRDD: RDD[(String, UserVisitAction)] = getUserVisitAction(taskUUID, sparkSession, taskParam)

    //获取pageFlowStr:"1,2,3,4,5,6,7"
    val tagetPageFlowStr: String = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    //pageFlowArray: Array[String]:[1,2,3,4,5,6,7]
    val pageFlowArray: Array[String] = tagetPageFlowStr.split(",")

    //获取目标页面切片：[1,2,3,4,5,6] + [2,3,4,5,6,7] ->[(1,2),(2,3),(3,4),(4,5),(5,6),(6,7)]->[1_2,2_3,3_4,4_5,5_6,6_7]
    //pageFlowArray.slice(0, pageFlowArray.length - 1):[1,2,3,4,5,6]
    //pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail):[(1,2),(2,3),(3,4),(4,5),(5,6),(6,7)]  zip拉链函数
    //pageFlowSplit: Array[String]:[1_2,2_3,3_4,4_5,5_6,6_7]
    val pageFlowSplit: Array[String] = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail).map {
      case (page1, page2) => page1 + "_" + page2
    }

    //    sessionId2UserActionRDD.foreach(println(_))

    //需求五：页面单跳转化率统计
    pageConversionRateStatistic(taskUUID, sparkSession, sessionId2UserActionRDD, pageFlowSplit, pageFlowArray)

  }

  def pageConversionRateStatistic(taskUUID: String,
                                  sparkSession: SparkSession,
                                  sessionId2UserActionRDD: RDD[(String, UserVisitAction)],
                                  pageFlowSplit: Array[String],
                                  pageFlowArray: Array[String]): Unit = {

    //pageFlowSplit2Count：[(1_2,1),(2_3,1),(1_2,1)]
    val pageFlowSplit2CountRDD: RDD[(String, Int)] = sessionId2UserActionRDD.groupByKey().flatMap {
      case (sessionId, iterableAction) =>
        //pageFlowList: List[Long]：[1,2,3,4,5]
        val pageFlowList: List[Long] = iterableAction.toList.sortWith {
          //true:action1排前面
          //false:action2排前面
          case (action1, action2) =>
            //action_time是字符串
            DateUtils.parseTime(action1.action_time).getTime < DateUtils.parseTime(action2.action_time).getTime
        }.map(_.page_id)
        // pageFlowList.slice(0, pageFlowList.length - 1):[1,2,3,4]
        // pageFlowList.slice(0, pageFlowList.length - 1).zip(pageFlowList.tail):[2,3,4,5]
        //filter先过滤
        //pageFlowSplit:[(1_2,1),(2_3,1),(3_4,1),(4_5,1)]
        val pageSplit2Num: List[(String, Int)] = pageFlowList.slice(0, pageFlowList.length - 1).zip(pageFlowList.tail).map {
          //          case (page1, page2) => (page1 + "_" + page2, 1)
          case (page1, page2) => page1 + "_" + page2
        }.filter {
          case pageSplit => pageFlowSplit.contains(pageSplit)
        }.map((_, 1))
        pageSplit2Num
    }

    //聚合：Map[(3_4,524),(2_3,540),(4_5,551),(6_7,576),(1_2,546),(5_6,612)]
    val pageSplitNumMap: Map[String, Long] = pageFlowSplit2CountRDD.countByKey()

    //统计页面转换率
    //第一个页面点击次数
    val startPageCount: Long = sessionId2UserActionRDD.filter {
      case (sessionId, userAction) =>
        userAction.page_id == pageFlowArray(0).toLong
    }.count()

    val pageConversionRatio: mutable.HashMap[String, Double] = mutable.HashMap[String, Double]()
    var previousPageCount = startPageCount
    for (pageSplit <- pageFlowSplit) {
      val pageCount: Long = pageSplitNumMap(pageSplit)
      val ratio = pageCount / previousPageCount.toDouble
      pageConversionRatio += (pageSplit -> ratio)
      previousPageCount = pageCount
    }

    val pageCoverationRatioStr: String = pageConversionRatio.map {
      case (pageSplit, ratio) => pageSplit + "=" + ratio
    }.mkString("|")

    val pageConverationRatio: PageConverationRatio = PageConverationRatio(taskUUID,pageCoverationRatioStr)
    import sparkSession.implicits._
    sparkSession.sparkContext.makeRDD(Array(pageConverationRatio)).toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","page_convert_ratio")
      .mode(SaveMode.Append)
      .save()
  }


  def getUserVisitAction(taskUUID: String, sparkSession: SparkSession, taskParam: JSONObject) = {

    //过滤条件
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sql = "select * from user_visit_action where date >='" + startDate + "' and date <='" + endDate + "'"

    import sparkSession.implicits._
    val sessionId2UserActionRDD: RDD[(String, UserVisitAction)] = sparkSession.sql(sql).as[UserVisitAction].rdd.map {
      action => (action.session_id, action)
    }
    sessionId2UserActionRDD
  }
}
