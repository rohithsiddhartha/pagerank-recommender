# PageRank-Recommendation-System
This is a Scala application that uses the PageRank algorithm to rank and recommend movies from the MovieLens dataset based on their importance, as inferred from user ratings. The application is built on Apache Spark and GraphX.

## Project Overview
The program reads two CSV files: ratings.csv and movies.csv. The ratings.csv file contains user ratings for movies, and movies.csv file contains movie details.

The application constructs a bipartite graph where users and movies are vertices, and ratings are edges between them. It then runs the PageRank algorithm on this graph, assigning each movie a rank.

Finally, it recommends the top 10 movies to a user. These movies are ones that the user has not yet rated, ordered by their PageRank score.

## Code Structure
The main class of the application is `MovieLensPageRank`. Its main function performs the following steps:

- Load the ratings.csv and movies.csv files into Spark DataFrames.
- Create a graph where users and movies are vertices and ratings are edges.
- Run the PageRank algorithm on the graph.
- Join the resulting ranks with the movies DataFrame to get movie titles along with their ranks.
- Generate recommendations for a specific user by selecting the top-ranked movies that the user has not yet rated.

## How to Run
- You can run this application using the spark-submit command. You need to build a JAR file of the application and then submit it to Spark. Here's a basic example:

- `spark-submit --class MovieLensPageRank target/scala-2.12/movielenspagerank_2.12-1.0.jar`
- Please replace target/scala-2.12/movielenspagerank_2.12-1.0.jar with the path to your JAR file.

Ensure that the ratings.csv and movies.csv files are in the same directory where you run the spark-submit command, or update the paths in the code to match their location.

## Output
The application prints the top 10 movie recommendations for a specific user (user with id 1 in the provided code). Each line of output contains a movie id, its title, and its PageRank score.