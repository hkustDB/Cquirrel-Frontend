import scala.math.Ordered.orderingToOrdered
import org.hkust.BasedProcessFunctions.RelationFKCoProcessFunction
import org.hkust.RelationType.Payload
import java.util.Date
class Q10lineitemProcessFunction extends RelationFKCoProcessFunction[Any]("lineitem",1,Array("ORDERKEY"),Array("CUSTKEY"),true, true) {
override def isValid(value: Payload): Boolean = {
   if(value("RETURNFLAG").asInstanceOf[Character]=="R"){
   true}else{
   false}
}
}
