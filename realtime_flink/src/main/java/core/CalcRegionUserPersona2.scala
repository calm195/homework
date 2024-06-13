package core

import `case`.{RegionCal, RegionCell, RegionFlow, UserInfo, UserRegionInfo, XDR}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.hbase.sink.{HBaseMutationConverter, HBaseSinkFunction}
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, KeyedBroadcastProcessFunction}
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Mutation, Put}

import java.util.Properties

/**
 * 计算各区域用户画像
 * Created by xuwei
 输出： hbase，key：区域id，value：当前区域人群构成：男、女、各年龄的人数
 1、读取信令数据
 2、读取基站、区域关系数据
 3、读取用户基础数据
 4、信令与用户基础数据关联： 广播，BroadcastProcessFunction
 5、用户出入行政区：行政区和基站数据广播，区分流入和流出， 保留用户上一个行政区状态， 按用户分区  KeyedBroadcastProcessFunction
 6、按行政区统计： 行政区keyby,  统计类state, KeyedProcessFunction,每1分钟输出一次统计结果，timeservice
 7、结果覆盖hbase

 8/checkoutpoint
 */
object CalcRegionUserPersona2 {

  def main(args: Array[String]): Unit = {
    //获取Flink执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    //开启Checkpoint
//    env.enableCheckpointing(1000*60*5)
//    env.setStateBackend(new FsStateBackend("hdfs://bigdata01:9000/flink/chk/CalcRegionUserPersona"))

    import org.apache.flink.api.scala._

    //获取基站信令数据-实时
    //9b0e2fa8-89e2-4b4c-9087-63afebe0847b|101_00002|39.958087|116.296185|1670468511000
    val topic = "xdr"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","bigdata01:9092")
    prop.setProperty("group.id","con1")
    val kafkaConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)
//    kafkaConsumer.setStartFromGroupOffsets()
    val xdrStringStream = env.addSource(kafkaConsumer)

    val xdrStream = xdrStringStream.map(record=>{
        XDR.fromKafka(record)
    }).filter(x=>x!=null)


    //获取用户基础数据-离线
    //9b0e2fa8-89e2-4b4c-9087-63afebe0847b|0|18
    val userInfoFilePath = "hdfs://bigdata01:9000/data/userInfo/"
    val userInfoStringStream = env.readFile(new TextInputFormat(new Path(userInfoFilePath)),userInfoFilePath,FileProcessingMode.PROCESS_CONTINUOUSLY,1000*10)
    //userInfoTupStream => (imsi,(gender,age))
    val userInfoStream = userInfoStringStream.map(line=>{
      UserInfo.fromStr(line)
    })

    val userInfoMapStateDescriptor = new MapStateDescriptor[String,UserInfo](
      "userInfo",
      classOf[String],
      classOf[UserInfo]
    )
//    userInfoMapStateDescriptor.enableTimeToLive(ttlConfig)
    val broadcastUserInfoTupStream = userInfoStream.broadcast(userInfoMapStateDescriptor)


    //第一步：关联基站信令数据和用户基础数据
    //laccellSignUserInfoStream => (imsi, laccell, gender, age)
    val laccellSignUserInfoStream = xdrStream.connect(broadcastUserInfoTupStream).process(new BroadcastProcessFunction[XDR, UserInfo, UserRegionInfo] {
      //处理基站信令数据
      override def processElement(value: XDR, ctx: BroadcastProcessFunction[XDR, UserInfo, UserRegionInfo]#ReadOnlyContext, out: Collector[UserRegionInfo]): Unit = {

        val imsi = value.imsi
        val laccell = value.laccell
        //取出BroadcastState中的数据
        val broadcastState = ctx.getBroadcastState(userInfoMapStateDescriptor)
        //关联数据
        val tupValue = broadcastState.get(imsi)

        if(tupValue!=null){
          val gender = tupValue.gender
          val age = tupValue.age
          out.collect(UserRegionInfo(imsi, laccell, gender, age))
        }

      }

      //处理用户基础数据，写入状态中
      override def processBroadcastElement(value:UserInfo, ctx: BroadcastProcessFunction[XDR, UserInfo, UserRegionInfo]#Context, out: Collector[UserRegionInfo]): Unit = {
        //获取BroadcastState

        val broadcastState = ctx.getBroadcastState(userInfoMapStateDescriptor)
        //向BroadcastState中写入用户基础数据
        broadcastState.put(value.imsi, value)
      }
    })






    //获取行政区-基站关系数据-离线
    //10001_00001|101_00002
    val regionCellFilePath = "hdfs://bigdata01:9000/data/regionCell/"
    val regionCellStream = env.readFile(new TextInputFormat(new Path(regionCellFilePath)),regionCellFilePath,FileProcessingMode.PROCESS_CONTINUOUSLY,1000*10)
    //regionCellTupStream => (laccell,region_id)
    val regionCellTupStream = regionCellStream.map(line=>{
      RegionCell.fromKafka(line)
    })

    val regionCellMapStateDescriptor = new MapStateDescriptor[String,String](
      "regionCell",
      classOf[String],
      classOf[String]
    )
//    regionCellMapStateDescriptor.enableTimeToLive(ttlConfig)
    val broadcastregionCellTupStream = regionCellTupStream.broadcast(regionCellMapStateDescriptor)

    //第二步：关联行政区-基站关系数据
    //joinStream => (region_id, gender, age)
    val joinStream = laccellSignUserInfoStream.keyBy(_.imsi).connect(broadcastregionCellTupStream).process(new KeyedBroadcastProcessFunction[String,UserRegionInfo,RegionCell,RegionFlow] {
      var lastRegion: ValueState[String] = _

      override def open(parameters: org.apache.flink.configuration.Configuration): Unit = {
        val lastRegionDescriptor = new ValueStateDescriptor[String]("lastRegion", createTypeInformation[String])
        lastRegion = getRuntimeContext.getState(lastRegionDescriptor)
      }

      //处理基站信令(包括用户基础数据)数据
      override def processElement(value:UserRegionInfo, ctx: KeyedBroadcastProcessFunction[String,UserRegionInfo,RegionCell,RegionFlow]#ReadOnlyContext, out: Collector[RegionFlow]): Unit = {
        val laccell = value.laccell
        val gender = value.gender
        val age = value.age
        //取出BroadcastState中的数据
        val broadcastState = ctx.getBroadcastState(regionCellMapStateDescriptor)
        //关联数据
        val region_id = broadcastState.get(laccell)
        if(!region_id.equals(lastRegion.value())){
          out.collect(RegionFlow(1,region_id,gender,age ))//1表示进入
          if(lastRegion.value()!=null){
            out.collect(RegionFlow(0,lastRegion.value(),gender,age ))
          }
          lastRegion.update(region_id)
        }

      }

      //处理行政区-基站关系数据，写入状态中
      override def processBroadcastElement(value: RegionCell, ctx: KeyedBroadcastProcessFunction[String,UserRegionInfo,RegionCell,RegionFlow]#Context, out: Collector[RegionFlow]): Unit = {
        //获取BroadcastState
        val broadcastState = ctx.getBroadcastState(regionCellMapStateDescriptor)
        //向BroadcastState中写入行政区-基站关系数据
        broadcastState.put(value.laccell, value.regionId)
      }
    })





    val resStream =joinStream.keyBy(_.regionId).process(new KeyedProcessFunction[String,RegionFlow,RegionCal] {
      var cal: ValueState[RegionCal] = _
      var isSettime: ValueState[Boolean] = _

      override def open(parameters: org.apache.flink.configuration.Configuration): Unit = {
        val calDescriptor = new ValueStateDescriptor[RegionCal]("cal", createTypeInformation[RegionCal])
        cal = getRuntimeContext.getState(calDescriptor)
        val isSettimeDescriptor = new ValueStateDescriptor[Boolean]("isSettime", createTypeInformation[Boolean])
        isSettime = getRuntimeContext.getState(isSettimeDescriptor)
      }
      //处理基站信令数据
      override def processElement(value: RegionFlow, ctx: KeyedProcessFunction[String, RegionFlow, RegionCal]#Context , out: Collector[RegionCal]): Unit = {
        val regionCell=Option(cal.value()).getOrElse(RegionCal(value.regionId,0,0,0,0,0))

        val addNum = if (value.isIn== 1) 1 else -1
        val gender = value.gender
        val age = value.age
        //统计男性、女性数量
        if (gender==1) {
          regionCell.manNum += addNum
        } else {
          regionCell.womanNum += addNum
        }
        //统计年龄区间数量
        if (age >= 10 && age < 20) {
          regionCell.age_10_20 += addNum
        } else if (age >= 20 && age < 40) {
          regionCell.age_20_40 += addNum
        } else if (age >= 40) {
          regionCell.age_40 += addNum
        }
        cal.update(regionCell)
        if( !Option(isSettime.value()).getOrElse(false)){
          ctx.timerService().registerProcessingTimeTimer(10000)
          isSettime.update(true)
        }

      }

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, RegionFlow, RegionCal]#OnTimerContext, out: Collector[RegionCal]): Unit = {
        val regionCell= cal.value()
        out.collect(regionCell)
        isSettime.update(false)
      }

    })

    resStream.print()




    //第四步：将结果输出到HBase中
    val conf = new org.apache.hadoop.conf.Configuration()
    conf.set("hbase.zookeeper.quorum","bigdata01:2181")
    conf.set("hbase.rootdir", "hdfs://bigdata01:9000/hbase")
    val hbaseSink = new HBaseSinkFunction[RegionCal]("region_cal",
      conf,
      //HBaseMutationConverter数据转换器
      new HBaseMutationConverter[RegionCal] {
        //初始化方法，只执行一次
        override def open(): Unit = {}
        //将Stream中的数据转化为HBase中的PUT操作
        override def convertToMutation(record: RegionCal): Mutation = {
          val rowkey = record.regionId

          val put = new Put(rowkey.getBytes())
          put.addColumn("persona".getBytes(),"man".getBytes(),record.manNum.toString.getBytes())
          put.addColumn("persona".getBytes(),"women".getBytes(),record.womanNum.toString.getBytes())
          put.addColumn("persona".getBytes(),"age_10_20".getBytes(),record.age_10_20.toString.getBytes())
          put.addColumn("persona".getBytes(),"age_20_40".getBytes(),record.age_20_40.toString.getBytes())
          put.addColumn("persona".getBytes(),"age_40".getBytes(),record.age_40.toString.getBytes())
          put
        }
      },
      //缓冲区最大大小字节,缓冲区最大写入申请数,缓冲区刷新距离
      100,
      100,
      1000
    )
    resStream.addSink(hbaseSink)
//    resStream.print()
    env.execute("CalcRegionUserPersona")
  }

}
