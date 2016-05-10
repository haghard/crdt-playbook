package replica

import java.util.concurrent.Executors._
import akka.actor.ActorDSL._
import akka.actor.{ ActorRef, ActorSystem }
import akka.event.slf4j.Logger
import akka.testkit.{ ImplicitSender, TestKit }
import com.rbmhtechnology.eventuate.VectorTime
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }

import scalaz.concurrent.{ Task, Strategy }
import scalaz.stream.async

/*
    Operation based CRDT, State based CRDT example in StateBaseCrdtORSetSpec

    Important !!!:
      Eventuate provides implementations of Operation-based CRDTs that rely on a replicated event log
      to reliably broadcast update operations to replicas (reliable causal broadcast)
      if means all updates must be applied in order in which they were sent from their origin (reliable causal broadcast)

    https://blog.acolyer.org/2016/04/25/delta-state-replicated-data-types/
*/
class OpBaseCrdtORSetSpec extends TestKit(ActorSystem("Replication-OpBaseCrdtORSet"))
    with WordSpecLike with MustMatchers
    with BeforeAndAfterEach with BeforeAndAfterAll with ImplicitSender {

  val orderId = "order-hjk4h56kj245k7j245n7oi4ou"

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val events =
    OrderItemAdded(orderId, "product-a") ::
      OrderItemAdded(orderId, "product-b") ::
      OrderItemAdded(orderId, "product-c") ::
      OrderItemRemoved(orderId, "product-a") ::
      OrderItemAdded(orderId, "product-d") :: //first d
      OrderItemAdded(orderId, "product-e") ::
      OrderItemAdded(orderId, "product-f") ::
      OrderItemAdded(orderId, "product-d") :: //duplicate d
      OrderItemAdded(orderId, "product-f") :: //duplicate f
      OrderItemAdded(orderId, "product-g") ::
      OrderItemAdded(orderId, "product-g") :: //duplicate g
      OrderItemAdded(orderId, "product-f") :: //duplicate f
      OrderItemAdded(orderId, "product-h") ::
      OrderItemRemoved(orderId, "product-h") ::
      OrderItemAdded(orderId, "product-a") :: //remove a
      OrderItemRemoved(orderId, "product-b") :: Nil // add a for the second time

  def eventuateORSetReplica(replicaNum: Int, testActor: ActorRef) =
    actor(new Act {
      val replicaName = s"replica-$replicaNum"
      val logger = Logger(replicaName)
      var replicas: List[akka.actor.ActorRef] = Nil
      var lastVectorTimestamp = VectorTime(replicaName -> 0l)

      var shoppingCart: com.rbmhtechnology.eventuate.crdt.ORSet[String] =
        com.rbmhtechnology.eventuate.crdt.ORSet[String]()

      become {
        //it is a regular i.e. causally related update
        case event: InstallAkkaReplica ⇒
          logger.info(s"Create replication channel from ${event.replicaName} to $replicaName")
          replicas = event.actor :: replicas
        case event: OrderItemAdded ⇒
          lastVectorTimestamp = (lastVectorTimestamp increment replicaName)
          shoppingCart = shoppingCart.add(event.item, lastVectorTimestamp)
          logger.info(s"[add-item]: ${event.item} at $lastVectorTimestamp state:${shoppingCart.value}")
          replicas.foreach(_.!(ReplicationEventCrdt2(orderId, event, lastVectorTimestamp)))
        case event: OrderItemRemoved ⇒
          //remove an entry by removing all pairs
          val allTimestamps = (shoppingCart prepareRemove event.item)
          shoppingCart = (shoppingCart remove allTimestamps)
          logger.info(s"[remove-item]: ${event.item} at $lastVectorTimestamp state:${shoppingCart.value}")
          replicas.foreach(_.!(ReplicationEventCrdt2(orderId, event, lastVectorTimestamp)))
        //it is a replicated and potentially concurrent update
        case event: ReplicationEventCrdt2 ⇒
          lastVectorTimestamp = event.vt merge lastVectorTimestamp
          event.event match {
            case ev: OrderItemAdded ⇒
              shoppingCart = shoppingCart.add(ev.item, lastVectorTimestamp)
              logger.info(s"[add-item-replication]:${ev.item} at $lastVectorTimestamp state:${shoppingCart.value}")
            case ev: OrderItemRemoved ⇒
              val allTimestamps = (shoppingCart prepareRemove ev.item)
              shoppingCart = shoppingCart.remove(allTimestamps)
              logger.info(s"[remove-item-replication]:${ev.item} at $lastVectorTimestamp state:${shoppingCart.value}")
          }

        case event: PrintOrder ⇒
          logger.info(lastVectorTimestamp.value.mkString("-"))
          testActor ! shoppingCart.value
      }
    })

  "Replication" should {
    import scala.collection._
    "ORSet from Eventuate as a state, Akka actors as the delivery mechanism" in {
      val QSize = 100
      val Ex = Strategy.Executor(newFixedThreadPool(4, new NamedThreadFactory("producer")))
      val replicasN = Set(1, 2, 3)

      val operations = async.boundedQueue[OrderEvent](QSize)(Ex)
      val replicas = async.boundedQueue[Int](QSize)(Ex)

      replicasN.foreach(replicas.enqueueOne(_).run)

      val expected = immutable.HashSet("product-a", "product-c", "product-d", "product-e", "product-g", "product-f")

      val writer = (scalaz.stream.Process.emitAll(events).toSource.zipWithIndex.map { orderEventWithInd ⇒
        //linearizable execution is required by op based CRDT delivery mechanism
        Thread.sleep(500)
        orderEventWithInd._1
      } to operations.enqueue)
        .drain
        .onComplete(scalaz.stream.Process.eval_ {
          replicas.close.flatMap(_ ⇒ operations.close)
        })

      val size = replicasN.size

      val replica1 = eventuateORSetReplica(1, testActor)
      val replica2 = eventuateORSetReplica(2, testActor)
      val replica3 = eventuateORSetReplica(3, testActor)

      replica1.!(InstallAkkaReplica(orderId, "replica-2", replica2))
      replica1.!(InstallAkkaReplica(orderId, "replica-3", replica3))

      replica2.!(InstallAkkaReplica(orderId, "replica-1", replica1))
      replica2.!(InstallAkkaReplica(orderId, "replica-3", replica3))

      replica3.!(InstallAkkaReplica(orderId, "replica-1", replica1))
      replica3.!(InstallAkkaReplica(orderId, "replica-2", replica2))

      Thread.sleep(1000)

      val actors = Vector(replica1, replica2, replica3)

      (writer merge operations.dequeue.zipWithIndex.map { eventWithInd ⇒
        actors(eventWithInd._2 % size) ! eventWithInd._1
      })(Ex)
        .onComplete(scalaz.stream.Process.eval_ {
          Task.delay {
            Thread.sleep(5000) //wait for replication happens
            actors.foreach(_.!(PrintOrder(orderId)))
          }
        }).run.runAsync(_ ⇒ ())

      import scala.concurrent.duration._

      within(15 second) {
        val results1 = expectMsgType[scala.collection.immutable.Set[String]]
        val results2 = expectMsgType[scala.collection.immutable.Set[String]]
        val results3 = expectMsgType[scala.collection.immutable.Set[String]]

        println(expected)
        println(results1)
        println(results2)
        println(results3)

        if (results1 != expected) fail(s"Error \n $expected - $results1")
        if (results2 != expected) fail(s"Error \n $expected - $results2")
        if (results3 != expected) fail(s"Error \n $expected - $results3")
      }
    }
  }
}