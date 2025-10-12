import dlt
from pyspark.sql.functions import expr, desc

@dlt.table
def top_artists_by_year():
  return (
      spark.read.table("songs_prepared")
      .filter(expr("year > 0"))
      .groupBy("artist_name", "year")
      .count().withColumnRenamed("count", "total_number_of_songs")
      .sort(desc("total_number_of_songs"), desc("year"))
  )
