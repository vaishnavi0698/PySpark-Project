


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Ratings').getOrCreate()
df1 = spark.read.csv('/Users/vaishnaviyenakandla/Downloads/raw_credits.csv', header=True, inferSchema=True)


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Ratings').getOrCreate()


#Creating a temperory view
spark.read.option("header",True).csv('/Users/vaishnaviyenakandla/Downloads/raw_credits.csv').createOrReplaceTempView("credits")
spark.read.option("header",True).csv('/Users/vaishnaviyenakandla/Downloads/raw_titles.csv').createOrReplaceTempView("titles")
spark.sql("""SELECT * FROM 
                (SELECT
                    c.name, c.role, t.title, t.type, t.release_year, t.imdb_score,
                    ROW_NUMBER()OVER 
                    (Partition by t.release_year ORDER BY t.imdb_score asc) 
                    as Rank FROM credits as c
                    join Titles as t ON c.id = t.id 
                    WHERE c.role = 'ACTOR' and t.imdb_score is not null and t.type = 'MOVIE' ORDER BY Rank ASC
                ) as R where Rank < 2 order by R.release_year, Rank """).show(100)

##USING the DATAFRAMES API from SPARKCORE

from pyspark.sql.functions import col, row_number, rank,when
from pyspark.sql.window import Window

# Reading data from CSV files and creating DataFrame
credits_df = spark.read.option("header", True).csv('/Users/vaishnaviyenakandla/Downloads/raw_credits.csv')
titles_df = spark.read.option("header", True).csv('/Users/vaishnaviyenakandla/Downloads/raw_titles.csv')

# Filtering the data and selecting required columns
df = credits_df.join(titles_df, credits_df.id == titles_df.id).filter((col('role') == 'ACTOR') &(col('type').isin('MOVIE','SHOW'))& (col('imdb_score').isNotNull()))     .select(col('name'), col('role'), col('title'), col('type'), col('release_year'), col('imdb_score'))


# Creating window specification for ranking
window_spec = Window.partitionBy(df.release_year).orderBy(col('imdb_score').asc())

# Adding rank column
df = df.withColumn('rank', row_number().over(window_spec))

# Selecting rows with rank less than 2
result_df = df.filter(col('rank') < 2).orderBy(col('release_year'), col('rank'))

# Displaying the result
result_df.show(100)


