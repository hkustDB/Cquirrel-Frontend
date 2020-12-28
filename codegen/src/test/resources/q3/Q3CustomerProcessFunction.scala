import scala.math.Ordered.orderingToOrdered
import org.hkust.BasedProcessFunctions.RelationFKProcessFunction
import org.hkust.RelationType.Payload
class Q3CustomerProcessFunction extends RelationFKProcessFunction[Any]("customer",Array("custkey"),Array("custkey"),false) {
override def isValid(value: Payload): Boolean = {
   if(value("C_MKTSEGMENT").asInstanceOf[String]=="BUILDING"){
   true}else{
   false}
}
}
