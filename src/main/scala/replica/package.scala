import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import com.rbmhtechnology.eventuate.VectorTime
import io.dmitryivanov.crdt.sets.ORSet

package object replica {

  final class NamedThreadFactory(var name: String) extends ThreadFactory {
    private def namePrefix = s"$name-thread"

    private val threadNumber = new AtomicInteger(1)
    private val group: ThreadGroup = Thread.currentThread().getThreadGroup

    override def newThread(r: Runnable) = {
      val th = new Thread(this.group, r, s"$namePrefix-${threadNumber.getAndIncrement()}", 0L)
      th.setDaemon(true)
      th
    }
  }

  sealed trait Event {
    def id: String
  }

  case class ItemCreated(id: String, creator: String = "") extends Event
  case class ItemAdded(id: String, item: String) extends Event
  case class ItemRemoved(id: String, item: String) extends Event
  case class ItemConflictResolved(id: String, items: scala.collection.mutable.SortedSet[String]) extends Event
  case class PrintOrder(id: String) extends Event
  case class PrintOrderT(id: String) extends Event

  case class InstallAkkaTReplica(id: String, replicaName: String, actor: akka.typed.ActorRef[Event]) extends Event
  case class ReplicationEvent(id: String, senderReplica: String, lastSeenVt: VectorTime, event: ItemAdded) extends Event
  case class ReplicationRemEvent(id: String, senderReplica: String, lastSeenVt: VectorTime, event: ItemRemoved) extends Event

  case class InstallReplica(id: String, replicaName: String, actor: scalaz.concurrent.Actor[Event]) extends Event
  case class InstallAkkaReplica(id: String, replicaName: String, actor: akka.actor.ActorRef) extends Event

  case class StateBasedReplication(id: String, crdt: ORSet[String]) extends Event
  case class ReplicationEventCrdt2(id: String, event: Event, vt: VectorTime) extends Event

  trait State {
    def id: String
    def addLine(item: String): Order
    def removeLine(item: String): Order
  }

  case class Order(id: String, items: List[String] = Nil) extends State {
    override def addLine(item: String): Order = copy(items = item :: items)
    override def removeLine(item: String): Order = copy(items = items.filterNot(_ == item))
    override def toString() = s"[${id}] items=${items.reverse.mkString(",")}"
  }
}