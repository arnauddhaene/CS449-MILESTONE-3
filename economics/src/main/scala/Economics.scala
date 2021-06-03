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

    val PRICE_M7 = 35000.0
    val RATE_M7 = 20.4

    val DAYS_PER_YEAR = 365.0

    val RATE_PER_GB = 0.012
    val RATE_PER_vCPU = 0.088128

    val M7_RAM_GB = 24 * 64
    val M7_TP_vCPU = 28

    val RPi_RAM_GB = 8
    val RPi_TP_vCPU = 0.25

    val RATE_CONTAINER_LIKE_M7 = RATE_PER_GB * M7_RAM_GB + RATE_PER_vCPU * M7_TP_vCPU
    val RATE_CONTAINER_LIKE_RPi = RATE_PER_GB * RPi_RAM_GB + RATE_PER_vCPU * RPi_TP_vCPU

    val RATE_RPi_MIN = 0.0108
    val RATE_RPi_MAX = 0.054

    val PRICE_RPi = 94.83

    def TIME_CONTAINER_PAY_RPi(rate: Double) = (PRICE_RPi / (RATE_CONTAINER_LIKE_RPi - rate)).ceil

    val RPi_w_M7_PRICE = (PRICE_M7 / PRICE_RPi).floor

    def USERS_IN(bytes: Double) = (bytes / 900.0).floor

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
              "MinDaysOfRentingICC.M7" -> PRICE_M7 / RATE_M7, // Datatype of answer: Double
              "MinYearsOfRentingICC.M7" -> PRICE_M7 / (RATE_M7 * DAYS_PER_YEAR) // Datatype of answer: Double
            ),
            "Q5.1.2" -> Map(
              "DailyCostICContainer_Eq_ICC.M7_RAM_Throughput" -> RATE_CONTAINER_LIKE_M7, // Datatype of answer: Double
              "RatioICC.M7_over_Container" -> RATE_M7 / RATE_CONTAINER_LIKE_M7, // Datatype of answer: Double
              "ContainerCheaperThanICC.M7" -> (RATE_CONTAINER_LIKE_M7 < RATE_M7) // Datatype of answer: Boolean
            ),
            "Q5.1.3" -> Map(
              "DailyCostICContainer_Eq_4RPi4_Throughput" -> 4 * RATE_CONTAINER_LIKE_RPi, // Datatype of answer: Double
              "Ratio4RPi_over_Container_MaxPower" -> RATE_RPi_MAX / RATE_CONTAINER_LIKE_RPi, // Datatype of answer: Double
              "Ratio4RPi_over_Container_MinPower" -> RATE_RPi_MIN / RATE_CONTAINER_LIKE_RPi, // Datatype of answer: Double
              "ContainerCheaperThan4RPi" -> (RATE_CONTAINER_LIKE_RPi < RATE_RPi_MAX) // Datatype of answer: Boolean
            ),
            "Q5.1.4" -> Map(
              "MinDaysRentingContainerToPay4RPis_MinPower" -> TIME_CONTAINER_PAY_RPi(RATE_RPi_MIN), // Datatype of answer: Double
              "MinDaysRentingContainerToPay4RPis_MaxPower" -> TIME_CONTAINER_PAY_RPi(RATE_RPi_MAX) // Datatype of answer: Double
            ),
            "Q5.1.5" -> Map(
              "NbRPisForSamePriceAsICC.M7" -> RPi_w_M7_PRICE, // Datatype of answer: Double
              "RatioTotalThroughputRPis_over_ThroughputICC.M7" -> (RPi_w_M7_PRICE * RPi_TP_vCPU) / M7_TP_vCPU, // Datatype of answer: Double
              "RatioTotalRAMRPis_over_RAMICC.M7" -> (RPi_w_M7_PRICE * RPi_RAM_GB) / M7_RAM_GB // Datatype of answer: Double
            ),
            "Q5.1.6" ->  Map(
              "NbUserPerGB" -> USERS_IN(0.5 * 1e9d), // Datatype of answer: Double 
              "NbUserPerRPi" -> USERS_IN(0.5 * 1e9d * 8), // Datatype of answer: Double 
              "NbUserPerICC.M7" -> USERS_IN(0.5 * 1e9d * 24 * 64) // Datatype of answer: Double 
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
