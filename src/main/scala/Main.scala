import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
import scala.io.Source


object Main {
  class Producer(input: BlockingQueue[Row], chunkSize: Int, dataset:String) extends Runnable {
    def createRow(columns: Array[String]): Row = Row(
      columns(0).toInt, columns(1),
      columns(2).toDouble, columns(3).toDouble,
      columns(4).toDouble, columns(5).toDouble,
      columns(6).toDouble, columns(7).toDouble
    )

    def run(): Unit = {
      val src = Source.fromFile(dataset)
      val lines: Iterator[String] = src.getLines()
      lines.next() // skip header

      val splitIntoChunks: (List[String], String) => List[String] = (recordBag, nextRecord) => { // recordbag her biri row string
        if (recordBag.:+(nextRecord).size == chunkSize) { // queue row tipinde
          recordBag.:+(nextRecord).foreach(record => input.put(createRow(record.split(",")))) //
          Thread.sleep(1000) // 500 records per second
          List.empty[String]
        } else {
          recordBag.:+(nextRecord)
        }
      }

      lines.foldLeft(List.empty[String])(splitIntoChunks)
      src.close()
      println("producer sent all records to data source.")
    }
  }

  class Consumer(input: BlockingQueue[Row], output: BlockingQueue[ConfMatrix], chunkSize: Int) extends Runnable {
    def run(): Unit = {
      var slidingWindow: Seq[Row] = (1 to chunkSize).map(_ => input.take())

      while (true) {
        // println(slidingWindow.mkString("\n")) // TODO delete this line
        val confMatrix = slidingWindow
          .map(l => (l.givenLabel, l.findPredictedLabel))
          .foldLeft(ConfMatrix1.empty)((matrix, next) => {
            next match {
              case ("A", "A") => ConfMatrix(slidingWindow.head.id,slidingWindow.last.id,matrix.actualA_predictedA + 1, matrix.actualA_predictedB, matrix.actualB_predictedA, matrix.actualB_predictedB)
              case ("A", "B") => ConfMatrix(slidingWindow.head.id,slidingWindow.last.id, matrix.actualA_predictedA, matrix.actualA_predictedB + 1, matrix.actualB_predictedA, matrix.actualB_predictedB)
              case ("B", "A") => ConfMatrix(slidingWindow.head.id,slidingWindow.last.id, matrix.actualA_predictedA, matrix.actualA_predictedB, matrix.actualB_predictedA + 1, matrix.actualB_predictedB)
              case ("B", "B") => ConfMatrix(slidingWindow.head.id,slidingWindow.last.id, matrix.actualA_predictedA, matrix.actualA_predictedB, matrix.actualB_predictedA, matrix.actualB_predictedB + 1)
            }
          })
        output.put(confMatrix)
        // Thread.sleep(1000) // TODO delete this line
        slidingWindow = slidingWindow.drop(1).:+(input.take())
      }
    }
  }

  class Writer(output: BlockingQueue[ConfMatrix]) extends Runnable {
    override def run(): Unit = {
      while (true) {
        val confMatrix: ConfMatrix = output.take()
        //println(confMatrix)
        println(
          s"""Start: ${confMatrix.start} End: ${confMatrix.end}
             |   Prd  A   B
             | Act   --------
             |    A | ${confMatrix.actualA_predictedA}   ${confMatrix.actualA_predictedB}
             |    B | ${confMatrix.actualB_predictedA}   ${confMatrix.actualB_predictedB}
             |""".stripMargin
        )
        // TODO
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val dataset = "tazi-se-interview-project-data.csv"
    val producerChunkSize = 500
    val slidingWindowSize = 1000

    val continuousSource: LinkedBlockingQueue[Row] = new LinkedBlockingQueue[Row](producerChunkSize)
    val confMatrixQueue: LinkedBlockingQueue[ConfMatrix] = new LinkedBlockingQueue[ConfMatrix](producerChunkSize)

    new Thread(new Producer(continuousSource, producerChunkSize, dataset)).start() // csv finished, thread finished.
    new Thread(new Consumer(continuousSource, confMatrixQueue, slidingWindowSize)).start() // programın akıcı olması için aynı anda producer consumer writer çalışması lazım
    new Thread(new Writer(confMatrixQueue)).start()

  }
}

case class Row(id: Int, givenLabel: String, m1A: Double, m1B: Double, m2A: Double, m2B: Double, m3A: Double, m3B: Double) {
  def findPredictedLabel: String = List(m1A * 0.5, m1B * 0.5, m2A * 0.6, m2B * 0.6, m3A * 0.7, m3B * 0.7)
    .zipWithIndex
    .maxBy(l => l._1)._2 % 2 match {
    case 0 => "A"
    case 1 => "B"
  }
}

case class ConfMatrix(start:Int, end:Int, actualA_predictedA: Int, actualA_predictedB: Int, actualB_predictedA: Int, actualB_predictedB: Int)

object ConfMatrix1 {
  def empty = ConfMatrix(0, 0, 0, 0, 0, 0)
}

// Row(1009,B,0.36319168259515866,0.6368083174048413,0.055909472076237976,0.944090527923762,0.0025846255323278555,0.9974153744676721)

// Row(1009,B,0.36319168259515866,0.6368083174048413,0.055909472076237976,0.944090527923762, 0.9974153744676721, 0.0025846255323278555)
// actual B predicted ?
