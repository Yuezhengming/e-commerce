
/**
  * 样例类：区域&城市信息
  *
  * @param cityId 城市ID
  * @param cityName 城市名称
  * @param areaName 区域名称
  */
case class AreaCityInfo(
                         cityId: Long,
                         cityName: String,
                         areaName: String
                       )

case class Top3AreaProduct(
                          taskId:String,
                          area:String,
                          areaLevel:String,
                          productId:Long,
                          productName:String,
                          clickCount:Long,
                          cityInfos:String,
                          productStatus:String
                          )
