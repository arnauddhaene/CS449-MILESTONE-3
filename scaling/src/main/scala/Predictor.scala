import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.log4j.Logger
import org.apache.log4j.Level
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import CSCMatrixFunctions._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val k = opt[Int]()
  val json = opt[String]()
  val users = opt[Int]()
  val movies = opt[Int]()
  val separator = opt[String](default=Option("\t"))
  verify()
}

//*****************************************************************************************************************

class CSCMatrixFunctions(data : CSCMatrix[Double]) {

    def activeMask: CSCMatrix[Double] = data.mapActiveValues(_ => 1.0)

    def perRowAverage: DenseVector[Double] = {
        val rowReducer = DenseVector.ones[Double](data.cols)
        return (data * rowReducer) /:/ (data.mapActiveValues(_ => 1.0) * rowReducer)
    }

    def perColAverage: DenseVector[Double] = {
        val colReducer = DenseVector.ones[Double](data.rows)
        return (data * colReducer) /:/ (data.mapActiveValues(_ => 1.0) * colReducer)
    }

}

object CSCMatrixFunctions {
    implicit def addCSCMatrixFunctions(data : CSCMatrix[Double]) = new CSCMatrixFunctions(data)
}

//*****************************************************************************************************************


object Predictor {
  def main(args: Array[String]) {
    var conf = new Conf(args)

    // Remove these lines if encountering/debugging Spark
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    //*****************************************************************************************************************


    println("Loading training data from: " + conf.train())
    val read_start = System.nanoTime
    val trainFile = Source.fromFile(conf.train())
    val trainBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies()) 
    for (line <- trainFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        trainBuilder.add(cols(0).toInt - 1, cols(1).toInt - 1, cols(2).toDouble)
    }
    val train = trainBuilder.result()
    trainFile.close
    val read_duration = System.nanoTime - read_start
    println("Read data in " + (read_duration/pow(10.0, 9)) + "s")

    //*****************************************************************************************************************

    /**
      * Computes x scaled by the user average rating.
      *
      * @param x 
      * @param userAvg
      * 
      * @return x scaled by userAvg
      */
    def scaler(x: Double, userAvg: Double): Double = {
        x match {
            case _ if x > userAvg => (5.0 - userAvg)
            case _ if x < userAvg => (userAvg - 1.0)
            case userAvg => 1.0
        }
    }

    /**
      * Normalized deviations
      *
      * @param ratings
      * @param userAverages
      * 
      * @return CSCMatrix[Double]
      */
    def normalizedDeviations(
        ratings: CSCMatrix[Double], userAverages: DenseVector[Double]
    ): CSCMatrix[Double] = {

        // ATTENTION: NORMALIZED DEVIATIONS ARE IMPLICITLY TRANSPOSED FOR DOWNSTREAM PURPOSES
        val normDevBuilder = new CSCMatrix.Builder[Double](rows = ratings.cols, cols = ratings.rows)
    
        for ( ((u, i), r) <- ratings.activeIterator) {
            normDevBuilder.add(i, u, 1.0 * (r - userAverages(u)) / scaler(r, userAverages(u)).toDouble)
        }

        return normDevBuilder.result()
    }

    /**
      * Preprocess ratings following Equation 4 of Milestone 2
      *
      * @param trainNormalized: CSCMatrix[Double]
      * @return CSCMatrix[Double] of preprocessed ratings
      */
    def preprocess(trainNormalized: CSCMatrix[Double]): CSCMatrix[Double] = {

        val processBuilder = new CSCMatrix.Builder[Double](rows = trainNormalized.cols,
                                                           cols = trainNormalized.rows)

        val cosineSimilarityDenominator = 
            sqrt(pow(trainNormalized, 2).t * DenseVector.ones[Double](trainNormalized.rows))

        for ( ((i, u), r) <- trainNormalized.activeIterator ) {

            val clippedRes = if (cosineSimilarityDenominator(u) != 0.0) r / cosineSimilarityDenominator(u) else 0.0

            processBuilder.add(u, i, clippedRes)
        }

        return processBuilder.result()

    }

    /**
      * Cosine Similarity calculator
      *
      * @param processed: CSCMatrix[Double]
      * @param u: user similarities
      * 
      * @return user's cosine similarities with all other users
      */
    def cosineSimilarities(processed: CSCMatrix[Double], u: Int): DenseVector[Double] = {

        val similarities = processed * processed(u, 0 to processed.cols - 1).t.toDenseVector

        // Zero out user's self-similarity
        similarities(u) = 0.0
        
        return similarities
    }

    /**
      * Find k Nearest Neighbors for all users
      *
      * @param processed: preprocessed ratings
      * @param k: top k Nearest Neighbors following cosine similarity
      * 
      * @return CSCMatrix[Double] of (User, User) -> Similarity sparse matrix
      */
    def kNearestNeighbors(processed: CSCMatrix[Double], k: Int): CSCMatrix[Double] = {

        val neighborBuilder = new CSCMatrix.Builder[Double](rows = processed.rows, cols = processed.rows)

        (0 to processed.rows - 1).foreach(
            u => {
                val userSimilarities = cosineSimilarities(processed, u)

                for (v <- argtopk(userSimilarities, k)) {
                    neighborBuilder.add(u, v, userSimilarities(v))
                }
            }
        )

        return neighborBuilder.result()

    }

    //*****************************************************************************************************************


    // conf object is not serializable, extract values that
    // will be serialized with the parallelize implementations
    val conf_users = conf.users()
    val conf_movies = conf.movies()
    val conf_k = conf.k()
    println("Compute kNN on train data...")

    val userAverages = train.perRowAverage
    val trainNormalized = normalizedDeviations(train, userAverages)
    val processed = preprocess(trainNormalized)

    println("\t computed processed ratings")

    val brProcessed = sc.broadcast(processed)

    println("\t sent processed ratings to broadcast variable")

    //*****************************************************************************************************************

    /**
      * Procedure mentioned in Part 4 of Milestone 3
      *
      * @param user: user id
      * 
      * @return top k cosine similarities
      */
    def topk(user: Int): (Int, IndexedSeq[(Int, Double)]) = {
        val processed = brProcessed.value

        val userSimilarities = cosineSimilarities(processed, user)

        return (user, argtopk(userSimilarities, conf_k).map(v => (v, userSimilarities(v))))
    }

    //*****************************************************************************************************************

    val topks = sc.parallelize(0 to conf_users - 1).map(topk).collect()

    println("\t parallelized and collected topk computation")

    val knnBuilder = new CSCMatrix.Builder[Double](rows = conf_users, cols = conf_users)

    println("\t initialized knn builder")

    topks.foreach {
        case (u, lvs) => {
            lvs.foreach {
                case (v, s) => knnBuilder.add(u, v, s)
            }
        }
    }

    val neighbors = knnBuilder.result()

    println("\t retrieved knn builder result")

    //*****************************************************************************************************************

    println("Loading test data from: " + conf.test())
    val testFile = Source.fromFile(conf.test())
    val testBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies()) 
    for (line <- testFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        testBuilder.add(cols(0).toInt - 1, cols(1).toInt - 1, cols(2).toDouble)
    }
    val test = testBuilder.result()
    testFile.close

    //*****************************************************************************************************************

    /**
      * User Specific Weighted Sum Deviations
      *
      * @param kNearestNeighbors
      * @param normalizedDeviations
      * 
      * @return CSCMatrix[Double] of user specific weighted sum deviations
      */
    def weightedSumDeviations(
        kNearestNeighbors: CSCMatrix[Double], normalizedDeviations: CSCMatrix[Double]
    ): CSCMatrix[Double] = {

        val nbUsers = normalizedDeviations.cols

        val deviationsBuilder = new CSCMatrix.Builder[Double](rows = nbUsers,
                                                              cols = normalizedDeviations.rows)


        (0 to nbUsers - 1).foreach(
            u => {
                val neighs = kNearestNeighbors(u, 0 to nbUsers - 1).t.toDenseVector

                val userNum = normalizedDeviations * neighs
                val userDenom = normalizedDeviations.activeMask * abs(neighs)

                for ((v, r) <- userNum.activeIterator) {
                    val clippedWeigSumDev = if (userDenom(v) != 0.0) r / userDenom(v) else 0.0

                    deviationsBuilder.add(u, v, clippedWeigSumDev)
                }
            }
        )

        return deviationsBuilder.result()

    }

    def predictor(userSpecWeightDev : CSCMatrix[Double], test : CSCMatrix[Double]): CSCMatrix[Double] = {
    
        val predictionBuilder = new CSCMatrix.Builder[Double](rows = test.rows, cols = test.cols)

        for ( ((u, i), r) <- test.activeIterator ) {
            val uAvg = userAverages(u)
            val uDev = userSpecWeightDev(u, i)

            predictionBuilder.add(u, i, uAvg + uDev * scaler(uAvg + uDev, uAvg))
        }

        return predictionBuilder.result()

    }

    /**
      * Mean Absolute Error of our predictions
      *
      * @param test: CSCMatrix[Double]
      * @param predictions: CSCMatrix[Double]
      * 
      * @return CSCMatrix[Double] of each of the errors
      */
    def mae(test : CSCMatrix[Double], predictions : CSCMatrix[Double]): CSCMatrix[Double] = abs(test - predictions)

    //*****************************************************************************************************************

    val userSpecWeightDev = weightedSumDeviations(neighbors, trainNormalized)

    val brWeightedSumDev = sc.broadcast(userSpecWeightDev)
    val brUserAverages = sc.broadcast(train.perRowAverage)

    //*****************************************************************************************************************

    def predict(user: Int, item: Int): (Int, Int, Double) = {

        val userSpecWeightDev = brWeightedSumDev.value
        val userAverages = brUserAverages.value

        val uAvg = userAverages(user)
        val uDev = userSpecWeightDev(user, item)

        return (user, item, uAvg + uDev * scaler(uAvg + uDev, uAvg))

    }

    //*****************************************************************************************************************

    val prediction_start = System.nanoTime

    val parPredictions = sc.parallelize(test.findAll(_ > 0.0))
        .map { case (u, i) => predict(u, i) }.collect()

    val predictionBuilder = new CSCMatrix.Builder[Double](rows = conf_users, cols = conf_movies)

    parPredictions.foreach {
        case (u, i, p) => predictionBuilder.add(u, i, p)
    }

    val predictions = predictionBuilder.result()
        
    val prediction_duration = System.nanoTime - prediction_start
    println(s"Compute predictions on test data... [${(prediction_duration/pow(10.0, 9))} sec]")

    //*****************************************************************************************************************

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
            "Q4.1.1" -> Map(
              "MaeForK=200" -> sum(mae(test, predictions)) / test.activeSize  // Datatype of answer: Double
            ),
            // Both Q4.1.2 and Q4.1.3 should provide measurement only for a single run
            "Q4.1.2" ->  Map(
              "DurationInMicrosecForComputingKNN" -> 0.0  // Datatype of answer: Double
            ),
            "Q4.1.3" ->  Map(
              "DurationInMicrosecForComputingPredictions" -> 0.0 // Datatype of answer: Double  
            )
            // Answer the other questions of 4.1.2 and 4.1.3 in your report
           )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
    spark.stop()
  } 
}
