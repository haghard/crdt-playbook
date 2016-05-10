package replica.crdt

import java.util.UUID
import org.scalatest.{ Matchers, WordSpec }
import io.dmitryivanov.crdt.sets._

class ScalaCrdtSpec extends WordSpec with Matchers {

  //State based CRDTs

  "ORSet" should {

    "Add the same element with different tag will result in one element" in {
      def value[T](tag: String, v: T) = ORSet.ElementState(tag, v)

      val first = UUID.randomUUID().toString
      val second = UUID.randomUUID().toString

      val result = new ORSet[String]()
        .add(value(first, "YlN"))
        .add(value(second, "YlN"))

      result.lookup.size shouldBe (1)
    }

    "Product won't be removed" in {
      def value[T](tag: String, v: T) = ORSet.ElementState(tag, v)

      val uuid1 = UUID.randomUUID().toString
      val uuid2 = UUID.randomUUID().toString
      val uuid3 = UUID.randomUUID().toString

      val result = new ORSet[String]()
        .add(value(uuid1, "YlN"))
        .add(value(uuid2, "YlM"))
        .add(value(uuid3, "SVM"))
        .remove(value(uuid1, "SVM"))
        .lookup

      result.size shouldBe 3
    }

    "Product will be removed" in {
      def value[T](tag: String, v: T) = ORSet.ElementState(tag, v)

      val uuid1 = UUID.randomUUID().toString
      val uuid2 = UUID.randomUUID().toString
      val uuid3 = UUID.randomUUID().toString

      val result = new ORSet[String]()
        .add(value(uuid1, "YlN"))
        .add(value(uuid2, "YlM"))
        .add(value(uuid3, "SVM"))
        .remove(value(uuid3, "SVM"))
        .lookup

      result.size shouldBe (2)
    }

    "Add/Remove/Add sequence results in last Add" in {
      def value[T](tag: String, v: T) = ORSet.ElementState(tag, v)

      val uuid1 = UUID.randomUUID().toString
      val uuid2 = UUID.randomUUID().toString

      val result = new ORSet[String]()
        .add(value(uuid1, "YlN"))
        .remove(value(uuid1, "YlN"))
        .add(value(uuid2, "YlN"))
        .lookup

      result.size shouldBe (1)
    }

    "Product will be removed after merge" in {
      //should be remove on replica where it was added

      def value[T](tag: Int, v: T) = ORSet.ElementState(tag.toString, v)

      val replicaOne = new ORSet[String]().add(value(1, "product-EcM"))
      val replicaTwo = new ORSet[String]().add(value(2, "product-WwQ"))
      val replicaThree = new ORSet[String]().add(value(3, "product-ZxQ"))

      val merged = ((replicaOne merge replicaTwo) merge replicaThree)

      //val result = merged.remove(value(1, "product-ZxQ")).lookup
      val result = merged.remove(value(3, "product-ZxQ")).lookup

      result shouldBe Set("product-EcM", "product-WwQ")
    }

    "Concurrent ops [add|remove] Add wins because add biases" in {
      //ORSet arbitrary additions and removals from the set
      //Property captures causal information in object itself that guarantees correct convergence
      //Add wins(add biases) because we are allowed to remove things that already added

      def value[T](tag: Int, v: T) = ORSet.ElementState(tag.toString, v)

      //Concurrent addition of the item product-EcM
      val result = (new ORSet[String]().add(value(1, "product-EcM")) merge new ORSet[String]().remove(value(2, "product-EcM")))
        .lookup

      result shouldBe Set("product-EcM")
    }

    "Concurrent ops replica-A[add|remove] replica-B[add]" in {
      //Add wins(add biases) because we are allowed to remove things that already added

      def value[T](tag: Int, v: T) = ORSet.ElementState(tag.toString, v)

      //We are allowed to remove elements those we have added
      val one = new ORSet[String]()
        .add(value(1, "product-EcM"))
        .remove(value(1, "product-EcM"))

      val two = new ORSet[String]()
        .add(value(2, "product-EcM"))

      val result = (one merge two).lookup

      result shouldBe Set("product-EcM")
    }
  }

  //Challenges: Client go offline, duplication of state, reordering of messages
  /**
   * Note that LWWSet relies on synchronized clocks and should only be used when the choice of value is not
   * important for concurrent updates occurring within the clock skew.
   * Instead of using timestamps based on System.currentTimeMillis() time it is possible to use a timestamp value based on something else,
   * for example an increasing version number from a database record that is used for optimistic concurrency control
   * or just use vector clocks.
   */

  //a) merge incoming vc + local one
  //b) increment merged result and write into LWWSet
  //c)
  //This sequence will create

  "LWWSet" should {
    "Concurrent add/remove. Product will be removed" in {

      def value[T](tag: Long, v: String) = LWWSet.ElementState(tag, v)

      val one = new LWWSet[String]().add(value(2l, "prod_a"))
      val two = new LWWSet[String]().remove(value(3l, "prod_a"))

      val merged = (one merge two)
      val result = merged.lookup

      result.size shouldBe (0)
    }

    "Concurrent add/remove. Product won't be removed" in {
      //Product will be removed
      def value[T](tag: Long, v: String) = LWWSet.ElementState(tag, v)

      val one = new LWWSet[String]().add(value(3l, "prod_a"))
      val two = new LWWSet[String]().remove(value(3l, "prod_a"))

      val merged = (one merge two)
      val result = merged.lookup

      result.size shouldBe (1)
    }

    "add/remove/add LWWSet" in {
      val lww = new LWWSet[String]()

      def value[T](tag: Long, v: String) = LWWSet.ElementState(tag, v)

      val result = lww
        .add(value(1l, "prod_a"))
        .remove(value(2l, "prod_a"))
        .add(value(3l, "prod_a"))
        .add(value(3l, "prod_b"))
        .lookup

      result shouldBe Set("prod_a", "prod_b")
    }

    "LWWSet merge 2 sets" in {
      def value[T](tag: Long, v: T) = LWWSet.ElementState(tag, v)

      val one = new LWWSet[String]()
        .add(value(2l, "a"))
        .add(value(3l, "b"))

      val two = new LWWSet[String]()
        //.remove(value(1l, "a"))
        .remove(value(4l, "b"))

      (one merge two).lookup shouldBe (Set("a"))
    }
  }
}