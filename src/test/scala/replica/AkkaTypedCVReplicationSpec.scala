/*

package replica

import akka.event.slf4j.Logger
import akka.actor.{ ActorRef, TypedActor, ActorSystem }
import akka.testkit.{ ImplicitSender, TestKit }
import akka.typed.{ PreStart, Behavior, Props }
import akka.typed.ScalaDSL.{ Full, Sig, Static, Same, ContextAware }
import com.rbmhtechnology.eventuate.{ ConcurrentVersionsTree, ConcurrentVersions }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }
import scala.collection.mutable

class AkkaTypedActorsCVReplicationSpec extends WordSpecLike with MustMatchers
    with BeforeAndAfterEach with BeforeAndAfterAll {

  val orderId = "order-99"

  def concurrentVersionsReplica(replicaNum: Int): Behavior[OrderEvent] =
    Static[OrderEvent] {
      val replicaName = "replica-" + replicaNum
      val logger = Logger(replicaName)
      var replicas: List[akka.typed.ActorRef[OrderEvent]] = Nil
      var applicationState: ConcurrentVersions[Order, OrderEvent] =
        ConcurrentVersionsTree(Order(orderId)) { (order: Order, event: OrderEvent) ⇒
          event match {
            case PrintOrder(orderId)                   ⇒ order
            case OrderCreated(orderId, _)              ⇒ Order(orderId)
            case OrderItemAdded(orderId, item)         ⇒ order.addLine(item)
            case OrderConflictResolved(orderId, items) ⇒ Order(orderId, items.toList)
          }
        }
      (ex: OrderEvent) ⇒ ex match {
        case event: InstallAkkaTReplica ⇒
          logger.info(s"Create replication channel from ${event.replicaName} to $replicaName")
          replicas = event.actor :: replicas
        case event: OrderItemAdded ⇒
          val nextVectorTimestamp = applicationState.all.head.vectorTimestamp.increment(replicaName)
          applicationState = applicationState.update(event, nextVectorTimestamp)
          logger.info(s"[add-item]: ${event.item}")
          replicas.foreach(_.!(ReplicationEvent(orderId, replicaName, nextVectorTimestamp, event)))
        case event: ReplicationEvent ⇒
          applicationState = {
            val updated = applicationState.update(event.event, event.lastSeenVt)
            if (updated.conflict) {
              val conflicts = updated.all.map(_.vectorTimestamp).mkString(" - ")
              val conflictingVersions = updated.all
              val allProducts = mutable.SortedSet(conflictingVersions.map(_.value.items).flatten: _*)
              val newTimestamp = conflictingVersions.map(_.vectorTimestamp).reduce(_ merge _)
              val resolved = updated.update(OrderConflictResolved(conflictingVersions.head.value.id, allProducts), newTimestamp)
              val result = resolved.resolve(newTimestamp)
              logger.info(s"[conflict] $conflicts resolved with $newTimestamp \nstate: ${result.all.mkString(",")}")
              result
            } else updated
          }
        case event: PrintOrderT ⇒
          logger.info("Final:" + applicationState.all.head.value.items.mkString(","))
        //event.replyTo ! applicationState.all.map(_.vectorTimestamp)
      }
    }

  val scenario: Behavior[OrderEvent] =
    ContextAware[OrderEvent] { ctx ⇒
      var seqNum = 0
      var replicas = Vector[akka.typed.ActorRef[OrderEvent]]()

      val replica1 = ctx.spawn(Props(concurrentVersionsReplica(1)), "replica-1")
      val replica2 = ctx.spawn(Props(concurrentVersionsReplica(2)), "replica-2")
      val replica3 = ctx.spawn(Props(concurrentVersionsReplica(3)), "replica-3")

      replica1.!(InstallAkkaTReplica(orderId, "replica-2", replica2))
      replica1.!(InstallAkkaTReplica(orderId, "replica-3", replica3))

      replica2.!(InstallAkkaTReplica(orderId, "replica-1", replica1))
      replica2.!(InstallAkkaTReplica(orderId, "replica-3", replica3))

      replica3.!(InstallAkkaTReplica(orderId, "replica-1", replica1))
      replica3.!(InstallAkkaTReplica(orderId, "replica-2", replica2))

      replicas = replicas ++ Seq(replica1, replica2, replica3)
      val n = replicas.size

      Static[OrderEvent] {
        case event: OrderItemAdded ⇒
          replicas(seqNum % n) ! event
          seqNum = seqNum + 1

        case event: PrintOrderT ⇒
          replicas.foreach(_ ! event)
      }
    }

  val replicationSystem = akka.typed.ActorSystem("replication", akka.typed.Props(scenario))

  "Replication" should {
    "ConcurrentVersions as a state, Akka actors as a reliable delivery mechanism and only add-item ops" in {

      val events =
        OrderItemAdded(orderId, "product-a") ::
          OrderItemAdded(orderId, "product-c") ::
          OrderItemAdded(orderId, "product-c") ::
          OrderItemAdded(orderId, "product-b") ::
          OrderItemAdded(orderId, "product-c") ::
          OrderItemAdded(orderId, "product-d") ::
          OrderItemAdded(orderId, "product-e") ::
          OrderItemAdded(orderId, "product-f") ::
          OrderItemAdded(orderId, "product-g") ::
          OrderItemAdded(orderId, "product-g") ::
          OrderItemAdded(orderId, "product-f") ::
          OrderItemAdded(orderId, "product-f") :: Nil

      events.foreach(replicationSystem.!(_))

      //import akka.typed.AskPattern._

      Thread.sleep(5000)

      replicationSystem tell (PrintOrderT(orderId))

      Thread.sleep(1000)

      if (1 != 0) fail(s"Error")
    }
  }

  override def afterAll() {
    replicationSystem.terminate()
  }
}*/
