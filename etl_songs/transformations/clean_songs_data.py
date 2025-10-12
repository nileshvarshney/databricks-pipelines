import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    comment="Million Song Dataset with data cleaned and prepared for analysis."
)
@dlt.expect("valid_artist_name", "artist_name IS NOT NULL")
@dlt.expect("valid_title", "song_title IS NOT NULL")
@dlt.expect("valid_duration", "duration > 0")
def songs_prepared():
  return (
      spark.read.table("songs_raw")
      .withColumnRenamed("title", "song_title")
      .select("artist_id", "artist_name", "duration", "release", "tempo", "time_signature", "song_title", "year")
  )