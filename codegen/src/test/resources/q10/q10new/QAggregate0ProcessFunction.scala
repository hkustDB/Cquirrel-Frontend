import org.hkust.RelationType.Payload
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.hkust.BasedProcessFunctions.AggregateProcessFunction
class QAggregate0ProcessFunction extends AggregateProcessFunction[Any, Double]("QAggregate0ProcessFunction", Array("CUSTKEY"), Array("CUSTKEY","C_NAME","C_ACCTBAL","C_PHONE","N_NAME","C_ADDRESS","C_COMMENT"), aggregateName = "revenue", deltaOutput = true) {
   override def aggregate(value: Payload): Double = {
      value("L_EXTENDEDPRICE").asInstanceOf[Double]*(1.0-value("L_DISCOUNT").asInstanceOf[Double])
   }
   override def addition(value1: Double, value2: Double): Double = value1 + value2
   override def subtraction(value1: Double, value2: Double): Double = value1 - value2
   override def initstate(): Unit = {
      val valueDescriptor = TypeInformation.of(new TypeHint[Double](){})
      val aliveDescriptor : ValueStateDescriptor[Double] = new ValueStateDescriptor[Double]("QAggregate0ProcessFunction"+"Alive", valueDescriptor)
      alive = getRuntimeContext.getState(aliveDescriptor)
      }
         override val init_value: Double = 0.0
         }
