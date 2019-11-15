import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * Created by caimh on 2019/11/14.
  */
object AreaPopularProductStat {


  def main(args: Array[String]) {

    //获取统计任务参数，【为了方便从配置文件中获取，企业中会从一个调度平台获取】
    val jsonstr: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam: JSONObject = JSONObject.fromObject(jsonstr)

    //任务的执行ID,用户唯一标识运行后的结果，用在Mysql数据库
    val taskId: String = UUID.randomUUID().toString

    //创建SparkConf上下文
    val sparkConf: SparkConf = new SparkConf().setAppName("AreaPopularProductStat").setMaster("local[*]")

    //创建Spark客户端
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = sparkSession.sparkContext

    //TODO 自定义函数
    // 三个函数
    // UDF：contact_Long_String()，将两个字段拼接起来，用指定的分隔符
    // UDF：get_json_object()，从json字符串中获取指定字段的值
    // UDAF：group_concat_distinct()，将一个分组中的多个字段值，用逗号拼接起来，同时进行去重
    sparkSession.udf.register("contact_Long_String", (v1: Long, v2: String, split: String) => {
      v1.toString + split + v2
    })
    sparkSession.udf.register("get_json_object", (json: String, field: String) => {
      val jsonObj = JSONObject.fromObject(json)
      ParamUtils.getParam(jsonObj, field)
    }
    )
    sparkSession.udf.register("group_contact_distinct", new GroupContactDistinctUDAF())

    //获取任务日期参数
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    //查询用户指定日期范围内的点击行为数据（city_id,clickActionInfo）
    val cityId2ClickActionRDD: RDD[(Long, Row)] = getCityId2ClickActionRDD(sparkSession, startDate, endDate)

    //查询城市信息（city_id,cityInfo）
    val cityId2AreaCityInfoRDD: RDD[(Long, AreaCityInfo)] = getCityId2CityInfoRDD(sparkSession)

    //生成点击商品基础信息临时表（关联点击行为信息与区域城市信息）-("city_id","product_id","area_name","city_name")
    generateTempClickProductBasicInfoTable(sparkSession, cityId2ClickActionRDD, cityId2AreaCityInfoRDD)

    //生成点击商品次数和区域信息临时表（统计区域点击商品次数）
    generateTempAreaProductClickCountBasicInfoTable(sparkSession)

    //生成包含完整商品信息的各个区域各商品点击次数是临时表
    //关联temp_area_click_product_count和product_info表，在temp_area_click_product_count基础上引入商品详细信息
    generateTempFullAreaProductClickCountInfoTable(sparkSession)

    //窗口函数排序（各个区域排名前3的的热门商品）
    getAreaTop3ProductRDD(taskId, sparkSession)

    sparkSession.close()
  }

  def getAreaTop3ProductRDD(taskid: String, sparkSession: SparkSession): Unit = {
    //窗口函数排序
    // 华北、华东、华南、华中、西北、西南、东北
    // A级：华北、华东
    // B级：华南、华中
    // C级：西北、西南
    // D级：东北

    // case when
    // 根据多个条件，不同的条件对应不同的值
    // case when then ... when then ... else ... end
    val sql = "select " +
      "area_name," +
      "CASE " +
      "WHEN area_name ='华北' or area_name = '华东' then 'A' " +
      "WHEN area_name ='华南' or area_name = '华中' then 'B' " +
      "WHEN area_name ='西北' or area_name = '西南' then 'C' " +
      "ELSE 'D' " +
      "END area_level," +
      "product_id," +
      "product_name," +
      "click_product_count," +
      "city_infos," +
      "product_status " +
      "from (" +
      "select " +
      "area_name," +
      "product_id," +
      "product_name," +
      "click_product_count," +
      "city_infos," +
      "product_status," +
      " row_number() over(partition by area_name order by click_product_count desc) rank " +
      "from temp_full_area_product_click_count" +
      ") t " +
      "where rank<=3"

    val df: DataFrame = sparkSession.sql(sql)

    import sparkSession.implicits._
    val top3AreaProductDF: DataFrame = df.rdd.map {
      case row => Top3AreaProduct(taskid, row.getAs[String]("area_name")
        , row.getAs[String]("area_level")
        , row.getAs[Long]("product_id")
        , row.getAs[String]("product_name")
        , row.getAs[Long]("click_product_count")
        , row.getAs[String]("city_infos")
        , row.getAs[String]("product_status")
      )
    }.toDF()

    //插入数据
    top3AreaProductDF.write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "area_top3_product")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  def generateTempFullAreaProductClickCountInfoTable(sparkSession: SparkSession): Unit = {
    val sql = "select " +
      "tacpc.area_name," +
      "tacpc.product_id," +
      "tacpc.click_product_count," +
      "tacpc.city_infos," +
      "pi.product_name," +
      "if(get_json_object(pi.extend_info,'product_status')='0','Self','Third Party') product_status" +
      " from temp_area_click_product_count tacpc " +
      "join product_info pi on tacpc.product_id = pi.product_id "

    val df: DataFrame = sparkSession.sql(sql)

    df.createOrReplaceTempView("temp_full_area_product_click_count")
  }

  def generateTempAreaProductClickCountBasicInfoTable(sparkSession: SparkSession) = {
    //
    val sql = "select " +
      "area_name," +
      "product_id," +
      "count(*) click_product_count, " +
      "group_contact_distinct(contact_Long_String(city_id,city_name,':')) city_Infos " +
      "from temp_click_product_basic " +
      "group by area_name,product_id"
    val df: DataFrame = sparkSession.sql(sql)

    //各区域商品点击的次数（以及额外的城市列表），再次将查询出来的数据注册为一个临时表
    df.createOrReplaceTempView("temp_area_click_product_count")
  }

  /**
    * 生成点击商品基础信息临时表（"city_id","product_id","area_name","city_name"）
    *
    * @param sparkSession
    * @param cityId2ClickActionRDD
    * @param cityId2AreaCityInfoRDD
    */
  def generateTempClickProductBasicInfoTable(sparkSession: SparkSession,
                                             cityId2ClickActionRDD: RDD[(Long, Row)],
                                             cityId2AreaCityInfoRDD: RDD[(Long, AreaCityInfo)]) = {
    import sparkSession.implicits._

    //tuple
    // 执行join操作，进行点击行为数据和城市数据的关联
    // JavaPairRDD，转换成一个JavaRDD<Row>（才能将RDD转换为DataFrame）
    val joinRDD: RDD[(Long, Long, String, String)] = cityId2ClickActionRDD.join(cityId2AreaCityInfoRDD).map {
      case (cityId, (clickAction, areaCityInfo)) => (cityId, clickAction.getAs[Long]("click_product_id"), areaCityInfo.areaName, areaCityInfo.cityName)
    }

    val df: DataFrame = joinRDD.toDF("city_id", "product_id", "area_name", "city_name")

    //创建临时表
    df.createOrReplaceTempView("temp_click_product_basic")
  }


  /**
    * 获取区域&城市信息
    *
    * @param sparkSession
    * @return
    */
  def getCityId2CityInfoRDD(sparkSession: SparkSession) = {
    val cityInfo = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"), (3L, "广州", "华南"),
      (4L, "三亚", "华南"), (5L, "武汉", "华中"), (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L,
        "成都", "西南"), (9L, "哈尔滨", "东北"))

    sparkSession.sparkContext.makeRDD(cityInfo).map {
      case (cityId, cityName, areaName) => (cityId, AreaCityInfo(cityId, cityName, areaName))
    }
  }


  /**
    * 查询指定日期范围内的点击行为数据
    *
    * @param sparkSession
    * @param startDate 开始日期
    * @param endDate   截止日期
    * @return 点击行为数据
    */
  def getCityId2ClickActionRDD(sparkSession: SparkSession, startDate: String, endDate: String) = {

    val sql = "select " +
      "city_id," +
      "click_product_id " +
      "from user_visit_action " +
      "where date >= '" + startDate + "' " +
      "and date <= '" + endDate + "'" +
      " and click_product_id is not null " +
      " and click_product_id != -1"
    sparkSession.sql(sql).rdd.map {
      case row => (row.getAs[Long]("city_id"), row)
    }

  }

}
