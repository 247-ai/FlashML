/**
  * Created by samik on 11/5/17.
  * Usage: amm prepInput.sc <full file path> <minimum class support>
  * Notes:
  * - Original Yelp dataset is ~4.4GB, can be downloaded from Yelp
  * - The amm script is available at: https://ammonite.io/
  */
import $file.Json

import Json.Json
import java.io._

import scala.collection.{mutable}
import scala.io._

/**
  * Extension class
  * @param as
  * @tparam A
  */
implicit class IteratorUtils[A](val as: Iterator[A]) extends AnyVal
{
    /**
      * Utility method for folding until certain condition is met.
      * @param init
      * @param op
      * @tparam B
      * @return
      */
    def foldLeftUntil[B](init: B)(op: (B, A) => Either[B, B]): B =
    {
        if (as.hasNext)
        {
            op(init, as.next) match
            {
                case Left(b) => b
                case Right(b) => as.foldLeftUntil(b)(op)
            }
        }
        else init
    }
}

@main
def main(filePath: String, minSupportForClass: Int) =
{
    val fileStream = Source.fromFile(filePath)
    val starCounts = new mutable.HashMap[Int, Int]().withDefaultValue(0)
    println(s"Collecting $minSupportForClass data for each class.")
    val finalFile = fileStream.getLines()
                    .foldLeftUntil(new StringBuffer())((accum: StringBuffer, line: String) =>
                    {
                        val json = Json.parse(line).asMap
                        val stars = json("stars").asInt
                        if(starCounts(stars) < minSupportForClass)
                        {
                            //accum.append(s"${json("text").asString.replaceAll("\\n", " ")}###$stars\n")
                            accum.append(Json.Value(Map("review_id" -> json("review_id"), "text" -> json("text"), "stars" -> stars.toString)).write + "\n")
                            starCounts(stars) += 1
                        }

                        if(starCounts.values.size == 5 && starCounts.values.forall(_ >= minSupportForClass)) Left(accum) else Right(accum)
                    })
    println(s"Collected $minSupportForClass data for each class.")
    println(s"Total length (bytes): ${finalFile.length()}")
    /*
    source.getLines()
        .takeWhile(_ => !starSupportCount.forall(v => v._2 == minSupportForClass))
        .foreach(line =>
        {
            val json = Json.parse(line).asMap
            val stars = json("stars").asInt
            if(starSupportCount(stars) < minSupportForClass)
            {
                finalFile.append(s"${json("text").asString.replaceAll("\\n", " ")}###$stars\n")
                starSupportCount(stars) += 1
            }
        })
    */

    // Write output
    val out = new BufferedWriter(new FileWriter("reviews.json"))
    out.write(finalFile.toString)
    out.close
}

