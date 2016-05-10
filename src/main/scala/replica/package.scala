import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import com.rbmhtechnology.eventuate.VectorTime
import io.dmitryivanov.crdt.sets.ORSet

package object replica {

  final class NamedThreadFactory(var name: String) extends ThreadFactory {
    private def namePrefix = name + "-thread"

    private val threadNumber = new AtomicInteger(1)
    private val group: ThreadGroup = Thread.currentThread().getThreadGroup

    override def newThread(r: Runnable) = {
      val th = new Thread(this.group, r, s"$namePrefix-${threadNumber.getAndIncrement()}", 0L)
      th.setDaemon(true)
      th
    }
  }

  trait ChatEvent {
    def id: String
    def pos: Long
  }

  case class ChatMessage(id: String, pos: Long, message: String = "") extends ChatEvent

  trait OrderEvent {
    def orderId: String
  }

  case class OrderCreated(orderId: String, creator: String = "") extends OrderEvent
  case class OrderItemAdded(orderId: String, item: String) extends OrderEvent
  case class OrderItemRemoved(orderId: String, item: String) extends OrderEvent
  case class OrderConflictResolved(orderId: String, items: scala.collection.mutable.SortedSet[String]) extends OrderEvent
  case class PrintOrder(orderId: String) extends OrderEvent
  case class PrintOrderT(orderId: String) extends OrderEvent

  case class InstallAkkaTReplica(orderId: String, replicaName: String, actor: akka.typed.ActorRef[OrderEvent]) extends OrderEvent
  case class ReplicationEvent(orderId: String, senderReplica: String, lastSeenVt: VectorTime, event: OrderItemAdded) extends OrderEvent
  case class ReplicationRemEvent(orderId: String, senderReplica: String, lastSeenVt: VectorTime, event: OrderItemRemoved) extends OrderEvent

  case class InstallReplica(orderId: String, replicaName: String, actor: scalaz.concurrent.Actor[OrderEvent]) extends OrderEvent
  case class InstallAkkaReplica(orderId: String, replicaName: String, actor: akka.actor.ActorRef) extends OrderEvent

  case class StateBasedReplication(orderId: String, crdt: ORSet[String]) extends OrderEvent
  case class ReplicationEventCrdt2(orderId: String, event: OrderEvent, vt: VectorTime) extends OrderEvent

  trait State {
    def id: String
    def addLine(item: String): Order
    def removeLine(item: String): Order
  }

  case class Dialog(id: String, items: List[ChatMessage] = Nil) {
    def add(item: ChatMessage): Dialog = copy(items = items :+ item)
    override def toString() = s"[${id}] messages=${items.reverse.mkString(",")}"
  }

  case class Order(id: String, items: List[String] = Nil) extends State {
    override def addLine(item: String): Order = copy(items = item :: items)
    override def removeLine(item: String): Order = copy(items = items.filterNot(_ == item))
    override def toString() = s"[${id}] items=${items.reverse.mkString(",")}"
  }
}