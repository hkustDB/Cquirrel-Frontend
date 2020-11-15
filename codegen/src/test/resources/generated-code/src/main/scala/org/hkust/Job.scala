import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.hkust.RelationType.Payload
object Job {
   lineitemTag: OutputTag[Payload] = OutputTag[Payload]("lineitem")
   ordersTag: OutputTag[Payload] = OutputTag[Payload]("orders")
   customerTag: OutputTag[Payload] = OutputTag[Payload]("customer")
   def main(args: Array[String]) {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val params: ParameterTool = ParameterTool.fromArgs(args)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      var executionConfig = env.getConfig
      executionConfig.enableObjectReuse()
      val inputpath = "file:///home/data/qwangbp/lineitem.tbl"
      val outputpath = "file:///home/data/qwangbp/testQ6.out"
      val inputStream : DataStream[Payload] = getStream(env,inputpath)
      val customer : DataStream[Payload] = inputStream.getSideOutput(customerTag)
      val lineitem : DataStream[Payload] = inputStream.getSideOutput(lineitemTag)
      val orders : DataStream[Payload] = inputStream.getSideOutput(ordersTag)
      val Q3CustomerS = Q3Customer.keyBy(i => i._3)
      .process(new Q3CustomerProcessFunction())
      .connect(orders)
      .process(new Q3OrdersProcessFunction)
      val Q3OrdersS = Q3Orders.keyBy(i => i._3)
      .process(new Q3OrdersProcessFunction())
      .connect(lineitem)
      .process(new Q3lineitemProcessFunction)
      val result  = inputStream.keyBy(i => i._3)
      .process(new Q3lineitemProcessFunction())
      .keyBy(i => i._3)
      .process(new Q3lineitemProcessFunction())
      .map(x => (x._4.mkString(", "), x._5.mkString(", "), x._6))
      .writeAsText(outputpath,FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      env.execute("Flink Streaming Scala API Skeleton")
   }
   private def getStream(env: StreamExecutionEnvironment, dataPath: String): DataStream[Payload] = {
      val data = env.readTextFile(dataPath).setParallelism(1)
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      var cnt : Long = 0
      val restDS : DataStream[Payload] = data
      .process((value: String, ctx: ProcessFunction[String, Payload]#Context, out: Collector[Payload]) => {
      val header = line.substring(0,3)
      val cells : Array[String] = line.substring(3).split("\\|")
      var relation = ""
      var action = ""
      header match {
         case "+LI" =>
         action = "Insert"
         relation = "lineitem"
         val i = Tuple2(format.parse(cells(10)),cells(5).toDouble)
         cnt = cnt + 1
         ctx.output(lineitemTag, Payload(relation, action, cells(0).toInt.asInstanceOf[Any],
         Array[Any](i._1,i._2),
         Array("L_SHIPDATE","L_EXTENDEDPRICE"), cnt)
         case "-LI" =>
         action = "Delete"
         relation = "lineitem"
         val i = Tuple2(format.parse(cells(10)),cells(5).toDouble)
         cnt = cnt + 1
         ctx.output(lineitemTag, Payload(relation, action, cells(0).toInt.asInstanceOf[Any],
         Array[Any](i._1,i._2),
         Array("L_SHIPDATE","L_EXTENDEDPRICE"), cnt)
         case _ =>
         out.collect(Payload("", "", 0, Array(), Array(), 0))
         }
         case "+OR" =>
         action = "Insert"
         relation = "orders"
         val i = Tuple1(cells(4).toLong)
         cnt = cnt + 1
         ctx.output(ordersTag, Payload(relation, action, cells(0).toInt.asInstanceOf[Any],
         Array[Any](i._1),
         Array("O_ORDERDATE"), cnt)
         case "-OR" =>
         action = "Delete"
         relation = "orders"
         val i = Tuple1(cells(4).toLong)
         cnt = cnt + 1
         ctx.output(ordersTag, Payload(relation, action, cells(0).toInt.asInstanceOf[Any],
         Array[Any](i._1),
         Array("O_ORDERDATE"), cnt)
         case _ =>
         out.collect(Payload("", "", 0, Array(), Array(), 0))
         }
         case "+CU" =>
         action = "Insert"
         relation = "customer"
         val i = Tuple1(cells(6).toLong)
         cnt = cnt + 1
         ctx.output(customerTag, Payload(relation, action, cells(0).toInt.asInstanceOf[Any],
         Array[Any](i._1),
         Array("C_MKTSEGMENT"), cnt)
         case "-CU" =>
         action = "Delete"
         relation = "customer"
         val i = Tuple1(cells(6).toLong)
         cnt = cnt + 1
         ctx.output(customerTag, Payload(relation, action, cells(0).toInt.asInstanceOf[Any],
         Array[Any](i._1),
         Array("C_MKTSEGMENT"), cnt)
         case _ =>
         out.collect(Payload("", "", 0, Array(), Array(), 0))
         }
         }).setParallelism(1)
         restDS
      }
      }
