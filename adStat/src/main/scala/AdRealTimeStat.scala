import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext, kafka010}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by caimh on 2019/11/15.
  */
object AdRealTimeStat {

  def main(args: Array[String]) {

    //构建Spark上下文
    val sparkConf: SparkConf = new SparkConf().setAppName("AdRealTimeStat").setMaster("local[*]")

    //创建Spark客户端
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))

    //获取kafka的配置
    val brokerList: String = ConfigurationManager.config.getString(Constants.KAFKA_BROKER_LIST)
    val topics: String = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    //kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers"->brokerList,//用于初始化连接到kafaka集群地址
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      //标识这个消费者属于哪个消费团体
      "group.id"->"adBlackListStatGroup",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //latest:先去Zookeeper获取offset,如果有，直接使用，如果没有，从最新的数据消费
      //earlist:先去Zookeeper获取offset，如果有，直接使用，如果没有，从最开始的数据消费
      //none:先去Zookeeper获取offset，如果有，直接使用，如果没有，直接报错
      "auto.offset.reset"->"latest",
      //如果是true,则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit"->(false:java.lang.Boolean))

      //创建DStream，返回接受到输入数据
      // LocationStrategies：根据给定的主题和集群地址创建consumer
      // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
      // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
      // ConsumerStrategies.Subscribe：订阅一系列主题
    //adRealTimeLogDStreamf:DStream[RDD RDD RDD ...]  RDD[message]  message: key value
    val adRealTimeLogDStream: InputDStream[ConsumerRecord[String, String]] = kafka010.KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferBrokers
      , ConsumerStrategies.Subscribe[String,String](Array(topics), kafkaParam))

    val adRealTimeValueDStream: DStream[String] = adRealTimeLogDStream.map {
      rdd => rdd.value()
    }

    //根据动态黑名单进行数据过滤（userid,timestamp province city userid adid）
    val filteredAdRealTimeValueDStream: DStream[(Long, String)] = filterByBlackList(sparkSession,adRealTimeValueDStream)

    //业务功能一：生成动态黑名单
    generateDynamicBlackList(sparkSession,filteredAdRealTimeValueDStream)

    ssc.start()
    ssc.awaitTermination()

  }


  /**
    * 动态生成黑名单
    *
    * @param sparkSession
    * @param filteredAdRealTimeValueDStream
    */
  def generateDynamicBlackList(sparkSession: SparkSession, filteredAdRealTimeValueDStream: DStream[(Long, String)])= {

    //计算接受的5秒的数据中，每天每个用户每个广告的点击量
    //原始数据：timestamp province city userid adid
    //将原始数据日志处理成（date_userid_adid,1）
    val dailyUserAdClickDStream: DStream[(String, Int)] = filteredAdRealTimeValueDStream.map {
      case (userid, log) =>
        val logsplit = log.split(" ")
        val date = new Date(logsplit(0).toLong)
        val datekey = DateUtils.formatDateKey(date)
        val adid = logsplit(4)

        val key = datekey + "_" + userid + "_" + adid
        (key, 1)
    }

    //reduceBykey,计算每天每个用户对每个广告的点击量
    val dailyUserAdClickCountDStream: DStream[(String, Int)] = dailyUserAdClickDStream.reduceByKey(_+_)

//    dailyUserAdClickCountDStream.foreachRDD(rdd=>rdd.foreach(println(_)))

    //每个5s的batch中，当天每个用户对每支广告的点击次数
    dailyUserAdClickCountDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        //对每个分区的数据去获取一次连接对象
        //每次都是从连接池中获取，而不是每次创建
        //写数据库操作，性能已经提高到最高了

        items =>
        val adUserClickCountBuffer = ArrayBuffer[AdUserClickCount]()

          for(item<-items)
            {
              val itemsplit: Array[String] = item._1.split("_")
              val adUserClickCount = item._2.toLong
//              val date = itemsplit(0)
              val date = DateUtils.formatDate(DateUtils.parseDateKey(itemsplit(0)))
              val userId = itemsplit(1).toLong
              val adId = itemsplit(2).toLong

              adUserClickCountBuffer += AdUserClickCount(date,userId,adId,adUserClickCount)
            }

          AdUserClickCountDAO.updateBatch(adUserClickCountBuffer.toArray)
      }
    }

    //黑名单统计
    //现在mysql里面，已经有了累计的每天各用户对广告的点击量
    //从mysql查询用户对广告的累计点击量>100,判定该用户为黑名单用户，就写入mysql表中，持久化
    //过滤出黑名单
    val blackListDStream: DStream[(String, Int)] = dailyUserAdClickCountDStream.filter {
      case (key, count) => {
        val keySplit: Array[String] = key.split("_")
        val date = DateUtils.formatDate(DateUtils.parseDateKey(keySplit(0)))
        val userId: Long = keySplit(1).toLong
        val adId: Long = keySplit(2).toLong

        //从Mysql中查询指定日期指定用户对指定广告的点击量
        val count: Int = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adId)

        //用户对广告的累计点击量>100,判定该用户为黑名单用户
        if (count >= 100) true else false
      }
    }

    //去重
    val blackListUserIdDS: DStream[Long] = blackListDStream.map(item=>item._1.split("_")(1).toLong)
    val blackListUserIdDistinct: DStream[Long] = blackListUserIdDS.transform(rdd=>rdd.distinct())

    //写入数据库
    blackListUserIdDistinct.foreachRDD{
      rdd=>rdd.foreachPartition{
        items=>
          val blackList: ArrayBuffer[AdBlacklist] = ArrayBuffer[AdBlacklist]()
          for(item<-items)
            {
              blackList+= AdBlacklist(item)
            }

          AdBlacklistDAO.insertBatch(blackList.toArray)
      }
    }
  }


  /**
    * 根据黑名单过滤数据
    *
    * @param sparkSession
    * @param adRealTimeValueDStream
    * @return
    */
  def filterByBlackList(sparkSession: SparkSession, adRealTimeValueDStream: DStream[String])={

    //接受到原始用户的点击行为日志之后,根据mysql中保存的黑名单，过滤掉黑名单点击行为数据
    //DStream使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD，功能很强大）
    val filterAdRealTimeValueDStream: DStream[(Long, String)] = adRealTimeValueDStream.transform {
      consumerRecordRDD => {

        //从Mysql数据库查询所有黑名单
        val adBlackList: Array[AdBlacklist] = AdBlacklistDAO.findAll()

        //转换成RDD
        val adBlackListRDD: RDD[(Long, Boolean)] = sparkSession.sparkContext.makeRDD(adBlackList).map {
          adBlack => (adBlack.userid, true)
        }

        //
        val userId2LogRDD: RDD[(Long, String)] = consumerRecordRDD.map {
          consumerRecord =>
            val userId = consumerRecord.split(" ")(3).toLong
            (userId, consumerRecord)
        }

        //join
        val joinedRDD: RDD[(Long, (String, Option[Boolean]))] = userId2LogRDD.leftOuterJoin(adBlackListRDD)

        //过滤黑名单数据
        val filteredRDD: RDD[(Long, (String, Option[Boolean]))] = joinedRDD.filter {
          case (userId, (log, black)) =>
            if (black.isDefined && black.get) false else true
        }

        //过滤后的数据
        filteredRDD.map {
          case (userId, (log, black)) => (userId, log)
        }
      }
    }
    filterAdRealTimeValueDStream
  }


}
