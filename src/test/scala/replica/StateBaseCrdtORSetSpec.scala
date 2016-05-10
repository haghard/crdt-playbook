package replica

import java.util.UUID
import java.util.concurrent.Executors._
import akka.actor.ActorDSL._
import akka.actor.{ ActorRef, ActorSystem }
import akka.event.slf4j.Logger
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scalaz.concurrent.{ Task, Strategy }
import scalaz.stream.async

/**
 *
 * CRDT Strong Eventual Consistency (Once you've seen the same event, you're in the same state)
 *
 * CRDT use cases
 * GCounter distributed counted
 * GSet: something that you can't remove
 * a) play-station/x-box achievements
 * b) visited urls
 *
 * ORSet add / remove with some biases
 * a) Shopping card
 *
 * +---------+
 * | commute |
 * +---------+
 * no | yes
 * |
 * +---+----+
 * |        |
 * CVersions  CRDT
 *
 * If state update operations commute, you should use CRDT, ORSet for example
 *
 * For ORSet most operation pair commute add(a); rm(f) == rm(f);add(a)
 * but
 * add(a);rm(a) != rm(a);
 * add(a) in this case add biases wins
 *
 * State based CRDT: every time you want to propagate change you should sent the whole state
 *
 * State based CRDT
 * Requirements:
 * Don't go backwards(each new update should dominate the previous one) => partial order, monotonic grow
 * Apply each update once: idempotent
 * Merge in any order(you never know in which order the merge is gonna happen): m commutative (order doesn't matter)
 * Merge contains several updates(merge can contain multiple updates): associative (batching doesn't matter)
 * Monotonic semi-lattice + merge = Least Upper Bound
 *
 * Supports unique tags to track causality between add - remove (UUID in our case)
 */
class StateBaseCrdtORSetSpec extends TestKit(ActorSystem("Replication-StateBaseCrdtORSet"))
    with WordSpecLike with MustMatchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with ImplicitSender {

  val orderId = "order-hjk4h56kj245k7j245n7oi4ou"

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val uuids = Map(
    "product-a" -> UUID.randomUUID().toString, "product-b" -> UUID.randomUUID().toString, "product-c" -> UUID.randomUUID().toString,
    "product-d" -> UUID.randomUUID().toString, "product-e" -> UUID.randomUUID().toString, "product-f" -> UUID.randomUUID().toString,
    "product-g" -> UUID.randomUUID().toString, "product-h" -> UUID.randomUUID().toString, "product-i" -> UUID.randomUUID().toString)

  //Unreliable delivery channel with reordering of messages and state duplication
  val events =
    OrderItemAdded(uuids("product-a"), "product-a") ::
      OrderItemAdded(uuids("product-b"), "product-b") ::
      OrderItemAdded(uuids("product-c"), "product-c") ::
      OrderItemRemoved(uuids("product-a"), "product-a") ::
      OrderItemAdded(uuids("product-d"), "product-d") :: //first d
      OrderItemAdded(uuids("product-e"), "product-e") ::
      OrderItemAdded(uuids("product-f"), "product-f") ::
      OrderItemAdded(UUID.randomUUID().toString, "product-d") :: //duplicate d
      OrderItemAdded(UUID.randomUUID().toString, "product-f") :: //duplicate f
      OrderItemAdded(uuids("product-g"), "product-g") ::
      OrderItemAdded(UUID.randomUUID().toString, "product-g") :: //duplicate g
      OrderItemAdded(UUID.randomUUID().toString, "product-f") :: //duplicate f
      OrderItemAdded(uuids("product-h"), "product-h") ::
      OrderItemRemoved(uuids("product-h"), "product-h") ::
      OrderItemAdded(UUID.randomUUID().toString, "product-a") :: //remove a
      OrderItemRemoved(uuids("product-b"), "product-b") :: Nil // add a for the second time

  def stateBasedCrdtORSetReplica(replicaNum: Int, testActor: ActorRef) =
    actor(new Act {
      val replicaName = s"replica-$replicaNum"
      val logger = Logger(replicaName)
      var replicas: List[akka.actor.ActorRef] = Nil

      var shoppingCart: io.dmitryivanov.crdt.sets.ORSet[String] =
        new io.dmitryivanov.crdt.sets.ORSet[String]()

      def value[T](tag: String, v: T) = io.dmitryivanov.crdt.sets.ORSet.ElementState(tag, v)

      become {
        case event: InstallAkkaReplica ⇒
          logger.info(s"Create replication channel from ${event.replicaName} to $replicaName")
          replicas = event.actor :: replicas

        //it is a regular i.e. causally related update
        case event: OrderItemAdded ⇒
          shoppingCart = shoppingCart.add(value(event.orderId, event.item))
          logger.info(s"[add-item]: ${event.item} state:${shoppingCart.lookup}")
          //emulate network latency
          system.scheduler.scheduleOnce(50 millis) { replicas.foreach(_.!(StateBasedReplication(orderId, shoppingCart))) }
        case event: OrderItemRemoved ⇒
          shoppingCart = (shoppingCart remove value(event.orderId, event.item))
          logger.info(s"[remove-item]: ${event.item}")
          system.scheduler.scheduleOnce(50 millis) { replicas.foreach(_.!(StateBasedReplication(orderId, shoppingCart))) }

        //it is a replicated and potentially concurrent update
        case event: StateBasedReplication ⇒
          shoppingCart = (event.crdt merge shoppingCart)
          logger.info(s"[replication-event]: ${shoppingCart.lookup}")

        case event: PrintOrder ⇒ testActor ! shoppingCart.lookup
      }
    })

  "Replication" should {
    import scala.collection._
    "ORSet from ScalaCrdt as a state, Akka actors as the delivery mechanism" in {
      val QSize = 100
      val Ex = Strategy.Executor(newFixedThreadPool(4, new NamedThreadFactory("producer")))
      val replicasN = Set(1, 2, 3)

      val operations = async.boundedQueue[OrderEvent](QSize)(Ex)
      val replicas = async.boundedQueue[Int](QSize)(Ex)

      replicasN.foreach(replicas.enqueueOne(_).run)

      val expected = immutable.HashSet("product-a", "product-c", "product-d", "product-e", "product-g", "product-f")

      println("products:" + events.filter(_.isInstanceOf[OrderItemAdded]))
      println("cancelled:" + events.filter(_.isInstanceOf[OrderItemRemoved]))

      val writer = (scalaz.stream.Process.emitAll(events).toSource.zipWithIndex.map(_._1) to operations.enqueue).drain
        .onComplete(scalaz.stream.Process.eval_ {
          replicas.close.flatMap(_ ⇒ operations.close)
        })

      val size = replicasN.size

      val replica1 = stateBasedCrdtORSetReplica(1, testActor)
      val replica2 = stateBasedCrdtORSetReplica(2, testActor)
      val replica3 = stateBasedCrdtORSetReplica(3, testActor)

      replica1.!(InstallAkkaReplica(orderId, "replica-2", replica2))
      replica1.!(InstallAkkaReplica(orderId, "replica-3", replica3))

      replica2.!(InstallAkkaReplica(orderId, "replica-1", replica1))
      replica2.!(InstallAkkaReplica(orderId, "replica-3", replica3))

      replica3.!(InstallAkkaReplica(orderId, "replica-1", replica1))
      replica3.!(InstallAkkaReplica(orderId, "replica-2", replica2))

      //wait for replication chanells installations
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