import com.mongodb.spark._
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import org.bson.Document
import com.mongodb.spark.config._
import javax.tools.DocumentationTool.DocumentationTask
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg._

val select =
  """
    { $match: {
        year: 2013,
        month: 4,
        day: 1
      }
    }
  """;
val join = """
  { $lookup:
   { from: "stations",
     foreignField: "COOP_STATION_ID",
     localField: "station_id",
     as: "station"
   }
  }""";
val unwind = """{ $unwind: "$station"}""";
val project = """{ $project: {
   "_id": 0,
   "stationID": "$station.COOP_STATION_ID",
   "precipitation": "$value",
   "elevation": "$station.ELEVATION",
   "latitude": "$station.LATITUDE",
   "longitude": "$station.LONGITUDE"
    }
  }""";

// Aggregation Example
val rdd = sc.loadFromMongoDB().withPipeline(
  Seq(
    Document.parse( select ),
    Document.parse( join ),
    Document.parse( unwind),
    Document.parse( project )
  )
)

def toVector( document: Document ): Vector = {
  return Vectors.dense(
    document.get( "precipitation").asInstanceOf[Integer].toDouble,
    document.get( "elevation").asInstanceOf[Double],
    document.get( "latitude").asInstanceOf[Double],
    document.get( "longitude").asInstanceOf[Double]
  )
}

rdd.cache()
// Cluster the data into 5 classes using KMeans
val model = KMeans.train( rdd.map( toVector ), 5, 20)

var centers = model.clusterCenters
centers.foreach( println )

val predictions = rdd.map {
  r => (
    r.getString( "stationID" ),
    centers( model.predict( toVector( r ) ) )(0),
    r.getDouble( "latitude" ).asInstanceOf[Double],
    r.getDouble( "longitude" ).asInstanceOf[Double]
  )
}

def mapToDoc( tuple: ( String, Double, Double, Double ) ): Document = {
  val doc = new Document();
  doc.append( "station", tuple._1 )
  doc.append( "cluster", tuple._2 )
  doc.append( "latitude", tuple._3 )
  doc.append( "longitude", tuple._4 )
  return doc;
}

val clusterDocs = predictions.map( mapToDoc )
clusterDocs.take(10)foreach( println )

val writeConf = WriteConfig(sc)
val writeConfig = WriteConfig(Map("collection" -> "clusters", "writeConcern.w" -> "majority", "db" -> "noaa"), Some(writeConf))

import com.mongodb.spark.api.java.MongoSpark

MongoSpark.save(clusterDocs, writeConfig)