import scala.math.Ordered.orderingToOrdered
import org.hkust.BasedProcessFunctions.RelationFKProcessFunction
import org.hkust.RelationType.Payload

class Q6lineitemProcessFunction extends RelationFKProcessFunction[Any]("Lineitem", Array("orderkey", "l_linenumber"), Array(), true) {
  override def isValid(value: Payload): Boolean = {
    if (value("L_SHIPDATE").asInstanceOf[java.util.Date] >= format.parse("1994-01-01") && value("L_SHIPDATE").asInstanceOf[java.util.Date] < format.parse("1995-01-01") && value("L_DISCOUNT").asInstanceOf[Double] > 0.05 && value("L_DISCOUNT").asInstanceOf[Double] < 0.07 && value("L_QUANTITY").asInstanceOf[Double] < 24) {
      true
    } else {
      false
    }
  }
}
