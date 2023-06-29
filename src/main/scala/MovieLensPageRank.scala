import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.sql.functions.desc

object MovieLensPageRank {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("MovieLensPageRank").getOrCreate()

    // Load the data
    val ratings = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("ratings.csv")

    val movies = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("movies.csv")

    // Create the graph
    val users = ratings.select("User_Id").rdd.distinct().map(row => (row.getInt(0).toLong, "user"))
    val movieVertices = ratings.select("Movie_Id").rdd.distinct().map(row => (row.getInt(0).toLong, "movie"))
    val vertices = users.union(movieVertices)
    val edges = ratings.rdd.map(row => Edge(row.getInt(0).toLong, row.getInt(1).toLong, row.getInt(2).toLong))
    val graph = Graph(vertices, edges)

    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices

    // Join the ranks with the movies
    val moviesDF = movies.withColumnRenamed("Movie_Id", "id").select("id", "Movie_Title")
    // Convert the VertexRDD to an RDD
    val ranksRDD = ranks.map {
      case (id, rank) => (id, rank)
    }

    // Convert the RDD to a DataFrame
    val ranksDF = spark.createDataFrame(ranksRDD).toDF("id", "rank")
    val movieRanks = moviesDF.join(ranksDF, "id")

    // Get the top 10 recommendations for a specific user
    val userId = 1
    val userRatings = ratings.filter(ratings("User_Id") === userId).select("Movie_Id")
    val recommendations = movieRanks.join(userRatings, movieRanks("id") === userRatings("Movie_Id"), "left_anti")
      .orderBy(desc("rank"))
      .limit(10)

    // Print the results
    recommendations.collect().foreach(println)

    spark.stop()
  }
}
