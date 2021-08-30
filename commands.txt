import com.mapr.db.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import spark.implicits._

val bData = spark.loadFromMapRDB("/tables/movie")

bData.selectExpr("CAST(_id AS STRING) AS key", "to_json(struct(*)) AS value")
.write
.format("kafka")
.option("topic","/apps/stream:movie-records")
.save()
