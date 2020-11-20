import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.hkust.RelationType.Payload
object Job {
   val lineitemTag: OutputTag[Payload] = OutputTag[Payload]("lineitem")
   val ordersTag: OutputTag[Payload] = OutputTag[Payload]("orders")
   val customerTag: OutputTag[Payload] = OutputTag[Payload]("customer")
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
         val i = Tuple9(format.parse(cells(10)),cells(5).toDouble,cells(6).toDouble,cells(0).toLong,cells(3).toInt)
         cnt = cnt + 1
         ctx.output(lineitemTag, Payload(relation, action, cells(0).toInt.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4,i._5,i._6,i._7,i._8,i._9),
         Array[String]("SHIPDATE","EXTENDEDPRICE","DISCOUNT","ORDERKEY","LINENUMBER"), cnt))
         case "-LI" =>
         action = "Delete"
         relation = "lineitem"
         val i = Tuple9(format.parse(cells(10)),cells(5).toDouble,cells(6).toDouble,cells(0).toLong,cells(3).toInt)
         cnt = cnt + 1
         ctx.output(lineitemTag, Payload(relation, action, cells(0).toInt.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4,i._5,i._6,i._7,i._8,i._9),
         Array[String]("SHIPDATE","EXTENDEDPRICE","DISCOUNT","ORDERKEY","LINENUMBER"), cnt))
         case "+OR" =>
         action = "Insert"
         relation = "orders"
         val i = Tuple9(cells(1).toLong,cells(4).toLong,cells(7).toLong,cells(0).toLong)
         cnt = cnt + 1
         ctx.output(ordersTag, Payload(relation, action, cells(0).toInt.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4,i._5,i._6,i._7,i._8,i._9),
         Array[String]("CUSTKEY","ORDERDATE","SHIPPRIORITY","ORDERKEY"), cnt))
         case "-OR" =>
         action = "Delete"
         relation = "orders"
         val i = Tuple9(cells(1).toLong,cells(4).toLong,cells(7).toLong,cells(0).toLong)
         cnt = cnt + 1
         ctx.output(ordersTag, Payload(relation, action, cells(0).toInt.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4,i._5,i._6,i._7,i._8,i._9),
         Array[String]("CUSTKEY","ORDERDATE","SHIPPRIORITY","ORDERKEY"), cnt))
         case "+CU" =>
         action = "Insert"
         relation = "customer"
         val i = Tuple9(cells(0).toLong,cells(6).toLong)
         cnt = cnt + 1
         ctx.output(customerTag, Payload(relation, action, cells(0).toInt.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4,i._5,i._6,i._7,i._8,i._9),
         Array[String]("CUSTKEY","MKTSEGMENT"), cnt))
         case "-CU" =>
         action = "Delete"
         relation = "customer"
         val i = Tuple9(cells(0).toLong,cells(6).toLong)
         cnt = cnt + 1
         ctx.output(customerTag, Payload(relation, action, cells(0).toInt.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4,i._5,i._6,i._7,i._8,i._9),
         Array[String]("CUSTKEY","MKTSEGMENT"), cnt))
         case _ =>
         out.collect(Payload("", "", 0, Array(), Array(), 0))
         }
         }).setParallelism(1)
         restDS
      }
      }
