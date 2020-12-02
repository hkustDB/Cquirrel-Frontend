import scala.math.Ordered.orderingToOrdered
import org.hkust.BasedProcessFunctions.RelationFKProcessFunction
import org.hkust.RelationType.Payload
import java.util.Date
class Q6lineitemProcessFunction extends RelationFKProcessFunction[Any]("lineitem",Array("ORDERKEY","LINENUMBER"),Array(),true) {
override def isValid(value: Payload): Boolean = {
   if(value("SHIPDATE").asInstanceOf[java.util.Date]>=format.parse("1994-01-01")&&value("SHIPDATE").asInstanceOf[java.util.Date]<format.parse("1995-01-01")&&value("DISCOUNT").asInstanceOf[Double]>0.05&&value("DISCOUNT").asInstanceOf[Double]<0.07&&value("QUANTITY").asInstanceOf[Double]<24){
   true}else{
   false}
}
}
