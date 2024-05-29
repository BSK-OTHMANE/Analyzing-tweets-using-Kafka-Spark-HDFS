# Import des modules nécessaires
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import json
from kafka import KafkaConsumer

# Fonction pour sauvegarder les données dans HDFS
def save_to_hdfs(rdd):
    if not rdd.isEmpty():
        # Convertir RDD en DataFrame
        spark = SparkSession.builder.appName("TwitterAnalysis").getOrCreate()
        df = spark.createDataFrame(rdd.map(lambda x: json.loads(x)))

        # Chemin du répertoire de destination dans HDFS
        hdfs_path = "hdfs://localhost:9000/user/hadoop/tweets"

        # Sauvegarder le DataFrame au format JSON dans HDFS
        df.write.mode('append').json(hdfs_path)

# Vérification si ce script est exécuté directement
if __name__ == "__main__":
    # Créer une session Spark
    spark = SparkSession.builder.appName("TwitterAnalysis").getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 5)  # Intervalle de 5 secondes pour chaque lot de données

    # Paramètres Kafka
    kafka_params = {"bootstrap.servers": "localhost:9092", "group.id": "twitter-consumer-group"}

    # Topics Kafka à consommer
    topics = ["twitter_topic"]

    # Créer un consommateur Kafka
    consumer = KafkaConsumer(*topics, **kafka_params)

    # Démarrer le contexte de streaming
    ssc.start()

    # Traiter les messages Kafka
    for message in consumer:
        tweets = message.value.decode('utf-8')  # Supposant que les messages sont encodés en JSON
        save_to_hdfs(sc.parallelize([tweets]))  # Sauvegarder les tweets dans HDFS

    # Attendre la fin du contexte de streaming
    ssc.awaitTermination()

# Instructions pour exécuter ce script avec spark-submit:
# Assurez-vous d'avoir démarré Zookeeper et Kafka, ainsi que créé le topic Kafka nommé 'twitter_topic'.
# Avant d'exécuter ce script, assurez-vous que le répertoire de destination dans HDFS est créé en utilisant la commande :
# hdfs dfs -mkdir -p /user/hadoop/tweets
# Utilisez la commande suivante pour exécuter ce script avec spark-submit :
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 TwitterConsumer.py
