# Import des modules nécessaires
from confluent_kafka import Producer
import pandas as pd
import json

# Fonction de rappel pour signaler la livraison des messages
def delivery_report(err, msg):
    if err is not None:
        print("Échec de la livraison du message :", err)
    else:
        print("Message livré à", msg.topic())

# Fonction pour produire les tweets à partir d'un fichier CSV et les envoyer à un topic Kafka
def produce_tweets(bootstrap_servers, topic, csv_file):
    # Création d'un producteur Kafka
    producer = Producer({'bootstrap.servers': bootstrap_servers})

    # Lecture du fichier CSV contenant les tweets
    df = pd.read_csv(csv_file)

    for index, row in df.iterrows():
        # Construction du message de tweet au format JSON
        tweet = { 
            "id": row['id'],
            "timestamp": row['created_at'],
            "tweet_text": row['text']
        }
        # Production du message vers le topic spécifié avec le tweet converti en JSON comme valeur
        producer.produce(topic, key=None, value=json.dumps(tweet), callback=delivery_report)
        producer.poll(0)  # Déclenchement du rappel de livraison
        # Ajout d'un petit délai si nécessaire
        # time.sleep(0.1)

    # Flush pour s'assurer que tous les messages sont envoyés
    producer.flush()

# Vérification si ce script est exécuté directement
if __name__ == "__main__":
    # Configuration des serveurs bootstrap et du topic Kafka
    bootstrap_servers = 'localhost:9092'
    topic = 'twitter_topic'
    # Chemin vers le fichier CSV contenant les tweets (remplacé par votre propre fichier CSV)
    csv_file = 'viral_tweets.csv'

    # Appel de la fonction pour produire les tweets à partir du fichier CSV
    produce_tweets(bootstrap_servers, topic, csv_file)

# en raison de problèmes avec l'API Twitter.
# J'ai initialement essayé d'utiliser l'API Twitter pour récupérer les tweets en temps réel, mais j'ai rencontré des problèmes
# avec l'API, donc j'ai opté pour une solution utilisant un fichier CSV contenant les tweets préalablement collectés.
# Ensuite , Allez au fichier TwitterConsumer.py