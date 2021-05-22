import org.rogach.scallop._
import org.json4s.jackson.Serialization
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

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

object Predictor {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    var conf = new Conf(args)

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

    val knn_start = System.nanoTime

    val userAverages = train.perRowAverage
    val trainNormalized = normalizedDeviations(train, userAverages)
    val processed = preprocess(trainNormalized)
    val neighbors100 = kNearestNeighbors(processed, 100)

    val knn_duration = System.nanoTime - knn_start
    println(s"Compute kNN on train data... [${(knn_duration/pow(10.0, 9))} sec]")

    //*****************************************************************************************************************

    val neighbors200 = kNearestNeighbors(processed, 200)

    //*****************************************************************************************************************
    
    val test_start = System.nanoTime
    println("Loading test data from: " + conf.test())

    val testFile = Source.fromFile(conf.test())
    val testBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies()) 
    for (line <- testFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        testBuilder.add(cols(0).toInt - 1, cols(1).toInt - 1, cols(2).toDouble)
    }
    val test = testBuilder.result()
    testFile.close
    
    val test_duration = System.nanoTime - test_start
    println(s"Read test data in... [${(test_duration/pow(10.0, 9))} sec]")

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

    //*****************************************************************************************************************

    val prediction_start = System.nanoTime

    val userSpecWeightDev100 = weightedSumDeviations(neighbors100, trainNormalized)
    val predictions100 = predictor(userSpecWeightDev100, test)
        
    val prediction_duration = System.nanoTime - prediction_start
    println(s"Compute predictions on test data... [${(prediction_duration/pow(10.0, 9))} sec]")

    //*****************************************************************************************************************
    
    val userSpecWeightDev200 = weightedSumDeviations(neighbors200, trainNormalized)
    val predictions200 = predictor(userSpecWeightDev200, test)

    //*****************************************************************************************************************

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

    /**
        * Average of an Iterable of a Numeric Type
        *
        * @param ts: iterable
        * 
        * @return average
        */
    def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
        num.toDouble(ts.sum) / ts.size
    }

    /**
        * Variance of an Iterable of a Numeric Type
        *
        * @param xs: Iterable
        * 
        * @return variance
        */
    def variance[T](xs: Iterable[T])(implicit num: Numeric[T]) = {
        val avg = average(xs)
        xs.map(num.toDouble(_)).map(a => math.pow(a - avg, 2)).sum / xs.size
    }

    /**
        * Standard deviation of an Iterable of a Numeric Type
        *
        * @param xs: Iterable
        * 
        * @return standard deviation
        */
    def stdev[T](xs: Iterable[T])(implicit num: Numeric[T]) = math.sqrt(variance(xs))

    /**
      * Timer
      *
      * @param train
      * @param test
      * @param topK
      * 
      * @return timed similarities and total prediction time
      */
    def time(
        train: CSCMatrix[Double], test: CSCMatrix[Double],
        topK: Int = 943
    ): (Double, Double) = {
        
        val start = System.nanoTime()

        val userAverages = train.perRowAverage
        val trainNormalized = normalizedDeviations(train, userAverages)
        val processed = preprocess(trainNormalized)
        val neighbors = kNearestNeighbors(processed, topK)
        
        val timeSimilarities = System.nanoTime() - start
        
        val userSpecWeightDev = weightedSumDeviations(neighbors, trainNormalized)
        val predictions = predictor(userSpecWeightDev, test)
        
        val timeTotal = System.nanoTime() - start

        return (timeSimilarities / 1e3d, timeTotal / 1e3d)

    }

    //*****************************************************************************************************************

    val timeVectors = (1 to 5).map(iter => {
        println(s"Iteration $iter")
        (time(train, test))
    }).unzip

    val timeForSimilarities = timeVectors._1
    val timeForPredictions = timeVectors._2

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
            "Q3.3.1" -> Map(
              "MaeForK=100" -> sum(abs(mae(test, predictions100))) / sum(test.activeMask), // Datatype of answer: Double
              "MaeForK=200" -> sum(abs(mae(test, predictions200))) / sum(test.activeMask)  // Datatype of answer: Double
            ),
            "Q3.3.2" ->  Map(
              "DurationInMicrosecForComputingKNN" -> Map(
                "min" -> timeForSimilarities.min,  // Datatype of answer: Double
                "max" -> timeForSimilarities.max, // Datatype of answer: Double
                "average" -> average(timeForSimilarities), // Datatype of answer: Double
                "stddev" -> stdev(timeForSimilarities) // Datatype of answer: Double
              )
            ),
            "Q3.3.3" ->  Map(
              "DurationInMicrosecForComputingPredictions" -> Map(
                "min" -> timeForPredictions.min,  // Datatype of answer: Double
                "max" -> timeForPredictions.max, // Datatype of answer: Double
                "average" -> average(timeForPredictions), // Datatype of answer: Double
                "stddev" -> stdev(timeForPredictions) // Datatype of answer: Double
              )
            )
            // Answer the Question 3.3.4 exclusively on the report.
           )
          json = Serialization.writePretty(answers)
        }

        // println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  } 
}
