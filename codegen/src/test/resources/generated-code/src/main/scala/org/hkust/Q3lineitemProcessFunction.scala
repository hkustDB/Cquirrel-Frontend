import scala.math.Ordered.orderingToOrdered
import org.hkust.BasedProcessFunctions.RelationFKProcessFunction
import org.hkust.RelationType.Payload
class Q3lineitemProcessFunction extends RelationFKCoProcessFunction[Any]("lineitem",1,Array("orderkey"),Array("orderkey"),true, true) {
override def isValid(value: Payload): Boolean = {
   if(value("L_SHIPDATE").asInstanceOf[java.util.Date]>format.parse("1995-03-15")){
   true}else{
   false}
}
}
