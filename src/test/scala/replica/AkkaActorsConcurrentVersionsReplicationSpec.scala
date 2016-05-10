package replica

import java.util.concurrent.Executors._

import akka.actor.ActorDSL._
import akka.actor._
import akka.event.slf4j.Logger
import akka.testkit.{ ImplicitSender, TestKit }
import com.rbmhtechnology.eventuate.{ VectorTime, ConcurrentVersions, ConcurrentVersionsTree }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }

import scala.collection.mutable
import scala.concurrent.forkjoin.ThreadLocalRandom
import scalaz.concurrent.{ Strategy, Task }
import scalaz.stream.{ Process, async }

/*
  http://rbmhtechnology.github.io/eventuate/user-guide.html

  If you have updates that commute, than it does not matter in which order they are received
  If operations commute they can be concurrent

  If state update operations of concurrent events do not commute,
  for example in this example we want to preserve lexicographical order of products we should use ConcurrentVersions,
  and solve conflicts manually.
  One more example where operations don't commute being a chat application. Here we have causal consistency
  to preserve an order of events. It is very important for dialog.


  ops pair:
   add(a-product);add(b-product)
    doesn't commute with
   add(b-product);add(a-product)

  because:
    List(a-product,  b-product) != List(b-product,  a-product)
    we want to preserve lexicographical order

*/
class AkkaActorsConcurrentVersionsReplicationSpec extends TestKit(ActorSystem("Replication-ConcurrentVersions"))
    with WordSpecLike with MustMatchers
    with BeforeAndAfterEach with BeforeAndAfterAll with ImplicitSender {

  val orderId = "order-1231-23235-4636-345634"

  override def afterAll() { TestKit.shutdownActorSystem(system) }

  def concurrentVersionsReplica(replicaNum: Int, waiter: ActorRef) =
    actor(new Act {
      val replicaName = s"replica-$replicaNum"
      val logger = Logger(replicaName)
      val cb: ActorRef = waiter
      var replicas: List[akka.actor.ActorRef] = Nil
      var applicationState: ConcurrentVersions[Order, Event] =
        ConcurrentVersionsTree(Order(orderId)) { (order: Order, event: Event) ⇒
          event match {
            case PrintOrder(orderId)                   ⇒ order
            case ItemCreated(orderId, _)              ⇒ Order(orderId)
            case ItemAdded(orderId, item)         ⇒ order.addLine(item)
            case ItemRemoved(orderId, item)       ⇒ order.removeLine(item)
            case ItemConflictResolved(orderId, items) ⇒ Order(orderId, items.toList)
          }
        }

      become {
        case event: InstallAkkaReplica ⇒
          logger.info(s"Create replication channel from ${event.replicaName} to $replicaName")
          replicas = event.actor :: replicas
        case event: ItemAdded ⇒
          val nextVectorTimestamp = applicationState.all.head.vectorTimestamp.increment(replicaName)
          applicationState = applicationState.update(event, nextVectorTimestamp)
          logger.info(s"[add-item]: ${event.item}")
          replicas.foreach(_.!(ReplicationEvent(orderId, replicaName, nextVectorTimestamp, event)))
        case event: ReplicationEvent ⇒
          applicationState = {
            val updated = applicationState.update(event.event, event.lastSeenVt)
            if (updated.conflict) {
              /*
              How to solve a conflict  ?
              Here I am just collecting and sorting unique names using SortedSet.
              For example if event contains reliable ts we can sort them by ts and preserve client's order of event,
              doing this we can reconstruct a global sequence of events those are being sent by client
              */

              //Interactive conflict resolution
              val conflicts = updated.all.map(_.vectorTimestamp).mkString(" - ")
              val conflictingVersions = updated.all
              val allProducts = mutable.SortedSet(conflictingVersions.map(_.value.items).flatten: _*)
              val newTimestamp = conflictingVersions.map(_.vectorTimestamp).reduce(_ merge _)
              val resolved = updated.update(ItemConflictResolved(conflictingVersions.head.value.id, allProducts), newTimestamp)
              val result = (resolved resolve newTimestamp)
              logger.info(s"[conflict] $conflicts resolved with $newTimestamp \nstate: ${result.all.mkString(",")}")
              result
            } else updated
          }
        case event: PrintOrder ⇒
          logger.info(applicationState.all.head.value.items.mkString(","))
          cb ! applicationState.all.map(_.vectorTimestamp)
      }
    })

  "Replication" should {
    "ConcurrentVersions as a state, Akka actors as a reliable delivery mechanism and only add-item ops" in {
      val QSize = 100
      val batchSize = 100
      val messagesNum = 10
      val orderId = "qwerty"

      val rnd = ThreadLocalRandom.current()
      val products = Vector.fill(messagesNum) { rnd.nextInt('a'.toInt, 'z'.toInt).toChar + "-product" }

      val Ex = Strategy.Executor(newFixedThreadPool(4, new NamedThreadFactory("producer")))
      val replicasN = Set(1, 2, 3, 4)
      val cancelled = List()

      val operations = async.boundedQueue[Event](QSize)(Ex)
      val replicas = async.boundedQueue[Int](QSize)(Ex)

      replicasN.foreach(replicas.enqueueOne(_).run)

      val events: List[Event] = products.map(name ⇒ ItemAdded(orderId, name)).toList :::
        cancelled.map(ind ⇒ ItemRemoved(orderId, s"${products(ind)}-product"))

      val writer = (scalaz.stream.Process.emitAll(events).toSource.zipWithIndex.map { orderEventWithInd ⇒
        //By the time a message was delivered to any replica, all previous replication messages already had been delivered to all other replicas
        //conflict free execution
        //Thread.sleep(500)

        if (orderEventWithInd._2 % batchSize == 0) Thread.sleep(500)

        if (orderEventWithInd._2 >= messagesNum) Thread.sleep(1000)

        orderEventWithInd._1
      } to operations.enqueue)
        .drain
        .onComplete(Process.eval_ {
          replicas.close.flatMap(_ ⇒ operations.close)
        })

      val size = replicasN.size

      val replica1 = concurrentVersionsReplica(1, testActor)
      val replica2 = concurrentVersionsReplica(2, testActor)
      val replica3 = concurrentVersionsReplica(3, testActor)
      val replica4 = concurrentVersionsReplica(4, testActor)

      replica1.!(InstallAkkaReplica(orderId, "replica-2", replica2))
      replica1.!(InstallAkkaReplica(orderId, "replica-3", replica3))
      replica1.!(InstallAkkaReplica(orderId, "replica-4", replica4))

      replica2.!(InstallAkkaReplica(orderId, "replica-1", replica1))
      replica2.!(InstallAkkaReplica(orderId, "replica-3", replica3))
      replica2.!(InstallAkkaReplica(orderId, "replica-4", replica4))

      replica3.!(InstallAkkaReplica(orderId, "replica-1", replica1))
      replica3.!(InstallAkkaReplica(orderId, "replica-2", replica2))
      replica3.!(InstallAkkaReplica(orderId, "replica-4", replica4))

      replica4.!(InstallAkkaReplica(orderId, "replica-1", replica1))
      replica4.!(InstallAkkaReplica(orderId, "replica-2", replica2))
      replica4.!(InstallAkkaReplica(orderId, "replica-3", replica3))

      val actors = Vector(replica1, replica2, replica3, replica4)

      (writer merge operations.dequeue.zipWithIndex.map { eventWithInd ⇒
        actors(eventWithInd._2 % size) ! eventWithInd._1
      })(Ex)
        .onComplete(Process.eval {
          Task.delay {
            Thread.sleep(3000) //wait for replication happens
            actors.foreach(_.!(PrintOrder(orderId)))
          }
        }).run.runAsync(_ ⇒ ())

      import scala.concurrent.duration._

      within(15 second) {
        var results = Map[Int, Seq[VectorTime]]()
        results = results.+(1 -> expectMsgType[Seq[VectorTime]])
        results = results.+(2 -> expectMsgType[Seq[VectorTime]])
        results = results.+(3 -> expectMsgType[Seq[VectorTime]])
        results = results.+(4 -> expectMsgType[Seq[VectorTime]])

        val actual = results.values.foldLeft(0l) { (acc, c) ⇒
          acc + c./:(0l) { _ + _.value.values.sum }
        }

        println(results.mkString("\n"))

        val expected = (messagesNum * size).toLong
        if (actual != expected) fail(s"Error sum expected $expected actual $actual")
      }
    }
  }
}