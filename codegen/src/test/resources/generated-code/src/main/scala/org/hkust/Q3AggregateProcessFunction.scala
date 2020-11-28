import org.hkust.RelationType.Payload
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.hkust.BasedProcessFunctions.AggregateProcessFunction
class Q3AggregateProcessFunction extends AggregateProcessFunction[Any, Double]("Q3AggregateProcessFunction", Array("orderkey"), Array("orderkey","orderdate","shippriority"), aggregateName = "revenue", deltaOutput = true) {
   override def aggregate(value: Payload): Double = {
      value("EXTENDEDPRICE").asInstanceOf[Double]*1.0-value("DISCOUNT").asInstanceOf[Double]
   }
   override def addition(value1: Double, value2: Double): Double = value1 + value2
   override def subtraction(value1: Double, value2: Double): Double = value1 - value2
   override def initstate(): Unit = {
      val valueDescriptor = TypeInformation.of(new TypeHint[Double](){})
      val aliveDescriptor : ValueStateDescriptor[Double] = new ValueStateDescriptor[Double]("Q3AggregateProcessFunction"+"Alive", valueDescriptor)
      alive = getRuntimeContext.getState(aliveDescriptor)
      }
         override val init_value: Double = 0.0
         }
