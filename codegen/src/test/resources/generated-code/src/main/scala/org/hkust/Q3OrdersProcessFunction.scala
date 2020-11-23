import scala.math.Ordered.orderingToOrdered
import org.hkust.BasedProcessFunctions.RelationFKProcessFunction
import org.hkust.RelationType.Payload
class Q3OrdersProcessFunction extends RelationFKCoProcessFunction[Any]("orders",1,Array("custkey"),Array("orderkey"),false, true) {
override def isValid(value: Payload): Boolean = {
   if(value("O_ORDERDATE").asInstanceOf[java.util.Date]<format.parse("1995-03-15")){
   true}else{
   false}
}
}
