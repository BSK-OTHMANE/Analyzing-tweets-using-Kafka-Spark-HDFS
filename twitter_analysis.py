# Import des modules nécessaires
from pyspark.sql import SparkSession
from textblob import TextBlob
from pyspark.sql.functions import udf, explode, split
from pyspark.sql.types import FloatType

# Fonction pour analyser le sentiment
def get_sentiment(text):
    return TextBlob(text).sentiment.polarity

# Création de la session Spark
spark = SparkSession.builder.appName("TwitterAnalysis").getOrCreate()

# Chargement des données depuis HDFS
tweets_df = spark.read.json("hdfs://localhost:9000/user/hadoop/tweets")
tweets_df.createOrReplaceTempView("tweets")

# Analyse des tendances : Trouver les hashtags les plus courants
hashtags_df = spark.sql("""
    SELECT explode(split(text, ' ')) as word
    FROM tweets
    WHERE word LIKE '#%'
""")
top_hashtags = hashtags_df.groupBy('word').count().orderBy('count', ascending=False).limit(10)

# Affichage des hashtags les plus courants
print("Les hashtags les plus courants :")
top_hashtags.show()

# Analyse des sentiments
sentiment_udf = udf(get_sentiment, FloatType())
tweets_with_sentiment = tweets_df.withColumn('sentiment', sentiment_udf(tweets_df['text']))
average_sentiment = tweets_with_sentiment.agg({'sentiment': 'avg'}).collect()[0][0]

# Affichage de la moyenne des sentiments
print("Average Sentiments polarity : {:.2f}".format(average_sentiment))

# Instructions pour exécuter ce script avec spark-submit:
# Assurez-vous d'avoir démarré Zookeeper et Kafka, ainsi que créé le topic Kafka nommé 'twitter_topic'.
# Avant d'exécuter ce script, assurez-vous que le répertoire de destination dans HDFS est créé en utilisant la commande :
# hdfs dfs -mkdir -p /user/hadoop/tweets
# Utilisez la commande suivante pour exécuter ce script avec spark-submit :
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 twitter_analysis.py
