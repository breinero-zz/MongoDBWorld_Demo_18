db.log.aggregate(
[
 { "$lookup": {
 	from: "stations",
 	foreignField: "COOP_STATION_ID",
 	localField: "station_id",
 	as: "station"
  }
 }
 ,{ $unwind: "$station"}
 ,{ $project: {
   "_id": 0,
   "station": "$station.STATION_NAME",
   "value": "$value",
   "elevation": "$station.ELEVATION",
   "latitude": "$station.LATITUDE",
   "longitude": "$station.LONGITUDE"
  }
}
]).pretty()