import org.rogach.scallop._
import org.json4s.jackson.Serialization
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val json = opt[String]()
  verify()
}

object Economics {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    var conf = new Conf(args)

    // Save answers as JSON
    def printToFile(content: String,
                    location: String = "./answers.json") =
      Some(new java.io.PrintWriter(location)).foreach{
        f => try{
          f.write(content)
        } finally{ f.close }
    }
    conf.json.toOption match {
      case None => ;
      case Some(jsonFile) => {
        var json = "";
        {
          // Limiting the scope of implicit formats with {}
          implicit val formats = org.json4s.DefaultFormats

          val answers: Map[String, Any] = Map(
            "Q5.1.1" -> Map(
              "MinDaysOfRentingICC.M7" -> 0.0, // Datatype of answer: Double
              "MinYearsOfRentingICC.M7" -> 0.0 // Datatype of answer: Double
            ),
            "Q5.1.2" -> Map(
              "DailyCostICContainer_Eq_ICC.M7_RAM_Throughput" -> 0.0, // Datatype of answer: Double
              "RatioICC.M7_over_Container" -> 0.0, // Datatype of answer: Double
              "ContainerCheaperThanICC.M7" -> true // Datatype of answer: Boolean
            ),
            "Q5.1.3" -> Map(
              "DailyCostICContainer_Eq_4RPi4_Throughput" -> 0.0, // Datatype of answer: Double
              "Ratio4RPi_over_Container_MaxPower" -> 0.0, // Datatype of answer: Double
              "Ratio4RPi_over_Container_MinPower" -> 0.0, // Datatype of answer: Double
              "ContainerCheaperThan4RPi" -> true // Datatype of answer: Boolean
            ),
            "Q5.1.4" -> Map(
              "MinDaysRentingContainerToPay4RPis_MinPower" -> 0.0, // Datatype of answer: Double
              "MinDaysRentingContainerToPay4RPis_MaxPower" -> 0.0 // Datatype of answer: Double
            ),
            "Q5.1.5" -> Map(
              "NbRPisForSamePriceAsICC.M7" -> 0.0, // Datatype of answer: Double
              "RatioTotalThroughputRPis_over_ThroughputICC.M7" -> 0.0, // Datatype of answer: Double
              "RatioTotalRAMRPis_over_RAMICC.M7" -> 0.0 // Datatype of answer: Double
            ),
            "Q5.1.6" ->  Map(
              "NbUserPerGB" -> 0.0, // Datatype of answer: Double 
              "NbUserPerRPi" -> 0.0, // Datatype of answer: Double 
              "NbUserPerICC.M7" -> 0.0 // Datatype of answer: Double 
            )
            // Answer the Question 5.1.7 exclusively on the report.
           )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  } 
}
