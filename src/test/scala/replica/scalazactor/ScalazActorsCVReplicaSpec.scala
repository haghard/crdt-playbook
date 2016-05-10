package replica.scalazactor

import java.util.concurrent.Executors._
import java.util.concurrent.{ CountDownLatch, TimeUnit }

import akka.event.slf4j.Logger
import com.rbmhtechnology.eventuate.{ ConcurrentVersions, ConcurrentVersionsTree, VectorTime }
import org.scalatest.{ Matchers, WordSpec }
import replica._

import scala.collection.mutable
import scala.concurrent.forkjoin.ThreadLocalRandom
import scalaz.concurrent.{ Strategy, Task }
import scalaz.stream.async
import scalaz.stream.Process

class ScalazActorsCVReplicaSpec extends WordSpec with Matchers {
  val QSize = 100
  val P = scalaz.stream.Process
  val orderId = "123-1234132-234sdfg-234-36b7"
  val Ex = Strategy.Executor(newFixedThreadPool(4, new NamedThreadFactory("worker")))

  //If state update operations of concurrent events do not commute,
  //for example we want to preserve lexicographical order of added products we should use ConcurrentVersions

  //pair add(a-product);add(b-product) doesn't commute with add(b-product);add(a-product)
  //List(a-product,  b-product) != List(b-product,  a-product) we want to preserve lexicographical order
  def scalazActorReplica(replicaNum: Int, testActor: scalaz.concurrent.Actor[Seq[VectorTime]])(implicit S: Strategy) = scalaz.concurrent.Actor[OrderEvent] {
    val replicaName = s"replica-$replicaNum"
    val logger = Logger(replicaName)
    var replicas: List[scalaz.concurrent.Actor[OrderEvent]] = Nil

    var applicationState: ConcurrentVersions[Order, OrderEvent] =
      ConcurrentVersionsTree(Order(orderId)) { (order: Order, event: OrderEvent) ⇒
        event match {
          case OrderCreated(orderId, _)              ⇒ Order(orderId)
          case OrderItemAdded(orderId, item)         ⇒ order.addLine(item)
          case OrderConflictResolved(orderId, items) ⇒ Order(orderId, items.toList)
        }
      }

    //Writing into location does not need to be coordinated with other locations
    (ev: OrderEvent) ⇒
      ev match {
        case event: InstallReplica ⇒
          logger.info(s"Replication channel ${event.replicaName} ~ $replicaName")
          replicas = event.actor :: replicas

        //it is a regular causally related update
        case event: OrderItemAdded ⇒
          logger.info(s"[add-item]: ${event.item}")
          val nextVT = applicationState.all.head.vectorTimestamp.increment(replicaName)
          applicationState = applicationState.update(event, nextVT)
          replicas.foreach(_.!(ReplicationEvent(orderId, replicaName, nextVT, event)))

        //it is a replicated and potentially concurrent update
        case event: ReplicationEvent ⇒
          applicationState = {
            val updated = applicationState.update(event.event, event.lastSeenVt)
            //Interactive conflict resolution
            if (updated.conflict) {
              val conflicts = updated.all.map(_.vectorTimestamp).mkString(" - ")
              val conflictingVersions = updated.all
              val sortedProducts = mutable.SortedSet(conflictingVersions.map(_.value.items).flatten: _*)
              val newTimestamp = conflictingVersions.map(_.vectorTimestamp).reduce(_ merge _)
              val resolved = updated.update(OrderConflictResolved(conflictingVersions.head.value.id, sortedProducts), newTimestamp)
              val result = (resolved resolve newTimestamp)
              logger.info(s"[conflict]: $conflicts resolved with $newTimestamp \nState: ${result.all.mkString(",")}")
              result
            } else updated
          }
        case ev: PrintOrder ⇒
          logger.info(s"Final state products: " + applicationState.all.map(_.value.items).flatten.mkString(";"))
          testActor.!(applicationState.all.map(_.vectorTimestamp))
      }
  }(S)

  "Replication" when {
    "ConcurrentVersions as state and Scalaz actors as a reliable delivery mechanism and only AddItem ops" must {
      val rnd = ThreadLocalRandom.current()
      val batchSize = 10
      val messagesNum = 100
      val products = Vector.fill(messagesNum) {
        rnd.nextInt('a'.toInt, 'z'.toInt).toChar + "-product"
      }

      val cancelled = List()

      val operations = async.boundedQueue[OrderEvent](QSize)(Ex)
      val replicas = async.boundedQueue[Int](QSize)(Ex)

      val replicasN = Set(1, 2, 3)
      replicasN.foreach(replicas.enqueueOne(_).run)

      val events: List[OrderEvent] = products.map(name ⇒ OrderItemAdded(orderId, name)).toList :::
        cancelled.map(ind ⇒ OrderItemRemoved(orderId, s"${products(ind)}-product")) :::
        replicasN.toList.map(_ ⇒ PrintOrder("shopping-cart-qwerty"))

      val eventsWriter = (P.emitAll(events).toSource.zipWithIndex.map { orderEventWithInd ⇒
        //By the time a message was delivered to any replica, all previous replication messages already had been delivered to all other replicas
        //conflict free execution
        //Thread.sleep(batchSize)

        if (orderEventWithInd._2 % batchSize == 0) //Sleep every batch
          Thread.sleep(500)

        if (orderEventWithInd._2 >= messagesNum)
          Thread.sleep(1000) //wait for replication

        orderEventWithInd._1
      } to operations.enqueue)
        .drain
        .onComplete(P.eval_(replicas.close.flatMap(_ ⇒ operations.close)))

      val latch = new CountDownLatch(3)
      val actorLatch = scalaz.concurrent.Actor[Seq[VectorTime]] {
        (vt: Seq[VectorTime]) ⇒
          val actual = vt./:(0l) {
            _ + _.value.values.sum
          }
          if (actual != messagesNum) fail(s"Error expected $messagesNum actual $actual")
          latch.countDown()
      }

      val replica1 = scalazActorReplica(1, actorLatch)(Ex)
      val replica2 = scalazActorReplica(2, actorLatch)(Ex)
      val replica3 = scalazActorReplica(3, actorLatch)(Ex)

      //Install a reliable delivery channel that preserves causal event ordering
      replica1.!(InstallReplica("shopping-cart-qwerty", "replica-2", replica2))
      replica1.!(InstallReplica("shopping-cart-qwerty", "replica-3", replica3))

      replica2.!(InstallReplica("shopping-cart-qwerty", "replica-1", replica1))
      replica2.!(InstallReplica("shopping-cart-qwerty", "replica-3", replica3))

      replica3.!(InstallReplica("shopping-cart-qwerty", "replica-1", replica1))
      replica3.!(InstallReplica("shopping-cart-qwerty", "replica-2", replica2))

      val actors = Vector(replica1, replica2, replica3)
      val size = replicasN.size

      (eventsWriter merge operations.dequeue.zipWithIndex.map { eventWithInd ⇒
        //round robin
        actors(eventWithInd._2 % size) ! eventWithInd._1
      })(Ex)
        .onComplete(Process.eval {
          Task.delay(Thread.sleep(1000)) // wait for replication
        }).run.runAsync(_ ⇒ ())

      latch.await(20, TimeUnit.SECONDS) shouldBe true
    }
  }
}