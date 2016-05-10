package replica

import java.util.concurrent.Executors._
import akka.actor.ActorDSL._
import akka.actor.{ActorRef, ActorSystem}
import akka.event.slf4j.Logger
import akka.testkit.{ImplicitSender, TestKit}
import com.rbmhtechnology.eventuate.VectorTime
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike}

import scalaz.concurrent.{Strategy, Task}
import scalaz.stream.async

class OpBasedShoppingCartSpec extends TestKit(ActorSystem("Replication-OpBaseCrdtORSet"))
  with WordSpecLike with MustMatchers
  with BeforeAndAfterEach with BeforeAndAfterAll with ImplicitSender {

  val cardNumber = "hjk4h56kj245k7j245n7oi4ou"

  override def afterAll() { TestKit.shutdownActorSystem(system) }

  val shoppingCart =
    ItemAdded(cardNumber, "product-a") :: ItemAdded(cardNumber, "product-b") ::
      ItemAdded(cardNumber, "product-c") :: ItemRemoved(cardNumber, "product-a") ::
      ItemAdded(cardNumber, "product-d") :: //first d
      ItemAdded(cardNumber, "product-e") :: ItemAdded(cardNumber, "product-f") ::
      ItemAdded(cardNumber, "product-d") :: //duplicate d
      ItemAdded(cardNumber, "product-f") :: //duplicate f
      ItemAdded(cardNumber, "product-g") ::
      ItemAdded(cardNumber, "product-g") :: //duplicate g
      ItemAdded(cardNumber, "product-f") :: //duplicate f
      ItemAdded(cardNumber, "product-h") :: ItemRemoved(cardNumber, "product-h") ::
      ItemAdded(cardNumber, "product-a") :: //remove a
      ItemRemoved(cardNumber, "product-b") :: Nil // add a for the second time

  def eventuateORSetReplica(replicaNum: Int, testActor: ActorRef) =
    actor(new Act {
      val replicaName = s"replica-$replicaNum"
      val logger = Logger(replicaName)
      var replicas: List[akka.actor.ActorRef] = Nil
      var lastVectorTimestamp = VectorTime(replicaName -> 0l)

      var shoppingCart: com.rbmhtechnology.eventuate.crdt.ORCart[String] =
        com.rbmhtechnology.eventuate.crdt.ORCart[String]()

      become {
        //it is a regular i.e. causally related update
        case event: InstallAkkaReplica ⇒
          logger.info(s"Create replication channel from ${event.replicaName} to $replicaName")
          replicas = event.actor :: replicas
        case event: ItemAdded ⇒
          lastVectorTimestamp = (lastVectorTimestamp increment replicaName)
          shoppingCart = shoppingCart.add(event.item, 1, lastVectorTimestamp)
          logger.info(s"[add-item]: ${event.item} at $lastVectorTimestamp state:${shoppingCart.value}")
          replicas.foreach(_.!(ReplicationEventCrdt2(cardNumber, event, lastVectorTimestamp)))
        case event: ItemRemoved ⇒
          //remove an entry by removing all pairs
          val allTimestamps = (shoppingCart prepareRemove event.item)
          shoppingCart = (shoppingCart remove allTimestamps)
          logger.info(s"[remove-item]: ${event.item} at $lastVectorTimestamp state:${shoppingCart.value}")
          replicas.foreach(_.!(ReplicationEventCrdt2(cardNumber, event, lastVectorTimestamp)))
        //it is a replicated and potentially concurrent update
        case event: ReplicationEventCrdt2 ⇒
          lastVectorTimestamp = event.vt merge lastVectorTimestamp
          event.event match {
            case ev: ItemAdded ⇒
              shoppingCart = shoppingCart.add(ev.item, 1, lastVectorTimestamp)
              logger.info(s"[add-item-replication]:${ev.item} at $lastVectorTimestamp state:${shoppingCart.value}")
            case ev: ItemRemoved ⇒
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

      val operations = async.boundedQueue[Event](QSize)(Ex)
      val replicas = async.boundedQueue[Int](QSize)(Ex)

      replicasN.foreach(replicas.enqueueOne(_).run)


      val writer = (scalaz.stream.Process.emitAll(shoppingCart).toSource.zipWithIndex.map { orderEventWithInd ⇒
        //linearizable execution is required by operation based CRDT delivery mechanism
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

      replica1.!(InstallAkkaReplica(cardNumber, "replica-2", replica2))
      replica1.!(InstallAkkaReplica(cardNumber, "replica-3", replica3))

      replica2.!(InstallAkkaReplica(cardNumber, "replica-1", replica1))
      replica2.!(InstallAkkaReplica(cardNumber, "replica-3", replica3))

      replica3.!(InstallAkkaReplica(cardNumber, "replica-1", replica1))
      replica3.!(InstallAkkaReplica(cardNumber, "replica-2", replica2))

      Thread.sleep(1000)

      val actors = Vector(replica1, replica2, replica3)

      (writer merge operations.dequeue.zipWithIndex.map { eventWithInd ⇒
        actors(eventWithInd._2 % size) ! eventWithInd._1
      }) (Ex)
        .onComplete(scalaz.stream.Process.eval_ {
          Task.delay {
            Thread.sleep(5000) //wait for replication happens
            actors.foreach(_.!(PrintOrder(cardNumber)))
          }
        }).run.runAsync(_ ⇒ ())

      import scala.concurrent.duration._

      within(15 second) {

        val expected = immutable.Map("product-a" -> 1, "product-c" -> 1, "product-d" -> 2, "product-e" -> 1,
          "product-g" -> 2, "product-f" -> 3)

        val results1 = expectMsgType[scala.collection.immutable.Map[String, Int]]
        val results2 = expectMsgType[scala.collection.immutable.Map[String, Int]]
        val results3 = expectMsgType[scala.collection.immutable.Map[String, Int]]

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