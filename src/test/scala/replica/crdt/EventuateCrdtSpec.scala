package replica.crdt

import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.crdt._
import org.scalatest.{ Matchers, WordSpec }

class EventuateCrdtSpec extends WordSpec with Matchers {

  "ORSet" should {

    "Add/Remove" in {
      val set = ORSet[String]().add("prod_a", VectorTime("replica-1" -> 0l))
      val ts = set.prepareRemove("prod_a")
      val newSet = set.remove(ts)
      newSet.value.size shouldBe 0
    }

    "Add/Remove/Add" in {
      val vectorTime = VectorTime("replica-1" -> 0l)

      val set = ORSet[String]().add("prod_a", vectorTime)

      val ts = set.prepareRemove("prod_a")
      val deletedSet = set.remove(ts)

      val added = deletedSet.add("prod_a", vectorTime)

      added.value.size shouldBe 1
    }

    "merge two addition" in {

      var tsOne = VectorTime("r1" -> 0l)
      tsOne = (tsOne increment "r1")
      val one = ORSet[String]().add("a", tsOne)

      var tsTwo = VectorTime("r2" -> 0l)
      tsTwo = (tsTwo increment "r2")
      val two = ORSet[String]().add("b", tsTwo)

      //on 1
      val merged1_2 = two.versionedEntries./:(tsOne)(_ merge _.vectorTimestamp)
      val mergedSet1_2 = one.add("b", merged1_2)

      println(mergedSet1_2.value)
      println(mergedSet1_2.versionedEntries)

      mergedSet1_2.value.size shouldBe 2
    }
  }
}