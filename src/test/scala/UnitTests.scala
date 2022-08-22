
import org.scalatest.{FreeSpec, FunSuite, Matchers}

import java.util.concurrent.LinkedBlockingQueue


class UnitTests extends FreeSpec with Matchers {
  "findPredictedLabel method should calculate weights properly" in {
    Row(1, "B", 0.2, 0.8, 0.3, 0.7, 0.4, 0.6)
      .findPredictedLabel shouldBe "B"
  }

  "drop take in slidingWindow should work properly" in {
    List(1, 2, 3).drop(1).:+(4) shouldBe List(2, 3, 4)
  }

  "LinkedBlockingQueue should add elements properly" in {
    val queue = new LinkedBlockingQueue[Int](5)
    val input = List(1, 2, 3, 4, 5)
    input.foreach(i => queue.put(i))
    val output = (1 to 5).map(_ => queue.take()).toList
    output shouldBe input
  }

  "foldLeft should run properly" in {
    val input = List("test", "test1", "test2")
    val lineIterator = input.iterator
    val output = lineIterator.foldLeft(List.empty[String])((all, next) => {
      all.:+(next)
    })
    input shouldBe output
  }

  "Producer should produce properly" in {
    val input = new LinkedBlockingQueue[Row]()
    new Main.Producer(input, 3, "src/test/resources/inputtest.csv").run()

    val output = (1 to 3).map(_ => input.take()).toList
    output.size shouldBe 3
  }

}
