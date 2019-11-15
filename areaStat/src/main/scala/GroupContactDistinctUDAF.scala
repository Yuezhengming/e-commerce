import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * 用户自定义聚合函数：分组拼接去重
  *
  */
class GroupContactDistinctUDAF extends UserDefinedAggregateFunction {

  //UDFA：输入数据是String类型
  override def inputSchema: StructType = StructType(StructField("cityInfo", StringType) :: Nil)

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //缓冲中已经拼接过的城市信息单
    var bufferCityInfo: String = buffer.getString(0)
    //刚刚传进来的的某个城市信息
    val cityInfo: String = input.getString(0)

    //去重逻辑
    if (!bufferCityInfo.contains(cityInfo)) {
      if (bufferCityInfo.isEmpty) {
        bufferCityInfo += cityInfo
      } else {
        bufferCityInfo += "," + cityInfo
      }
      buffer.update(0, bufferCityInfo)
    }

  }

  override def bufferSchema: StructType = StructType(StructField("bufferCityInfo", StringType) :: Nil)

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferCityInfo1: String = buffer1.getString(0)
    val bufferCityInfo2: String = buffer2.getString(0)

    for (bufferCityInfo <- bufferCityInfo2.split(",")) {
      if (!bufferCityInfo1.contains(bufferCityInfo)) {
        if (bufferCityInfo1.isEmpty) {
          bufferCityInfo1 += bufferCityInfo
        } else {
          bufferCityInfo1 += "," + bufferCityInfo
        }
      }
    }
    buffer1.update(0, bufferCityInfo1)

  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  override def deterministic: Boolean = true

  override def evaluate(buffer: Row): Any = buffer.getString(0)

  override def dataType: DataType = StringType
}
