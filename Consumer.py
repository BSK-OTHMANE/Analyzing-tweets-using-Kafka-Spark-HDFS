# Assurez-vous d'exécuter un serveur Zookeeper et un serveur Kafka avant d'exécuter ce script.
# Pour démarrer Zookeeper, exécutez la commande suivante dans votre répertoire Kafka :
# sh bin/zookeeper-server-start.sh config/zookeeper.properties
# Pour démarrer le serveur Kafka, utilisez la commande suivante :
# sh bin/kafka-server-start.sh config/server.properties
# Assurez-vous également d'avoir initialisé un topic nommé 'twitter_topic' en utilisant la commande suivante :
# bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic twitter_topic
# Vous pouvez vérifier la liste des topics avec la commande :
# bin/kafka-topics.sh --list --bootstrap-server localhost:9092
# Ensuite, exécutez ce script et passez à TwitterProducer.py et exécutez le pour produire des messages.

# Import des modules nécessaires
from confluent_kafka import Consumer, KafkaError

# Fonction pour consommer les messages d'un topic Kafka
def consume_from_topic(bootstrap_servers, topic):
    # Configuration du consommateur Kafka
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    })

    # Abonnement au topic spécifié
    consumer.subscribe([topic])

    try:
        # Boucle de consommation des messages
        while True:
            # Polling des messages avec un délai d'attente de 1 seconde
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Fin de la partition
                    continue
                else:
                    # Erreur autre que la fin de la partition
                    print("Erreur du consommateur: {}".format(msg.error()))
                    break

            print('Message reçu: {}'.format(msg.value().decode('utf-8')))
    finally:
        # Fermeture du consommateur Kafka
        consumer.close()

# Vérification si ce script est exécuté directement
if __name__ == "__main__":
    # Configuration des serveurs bootstrap et du topic à consommer
    bootstrap_servers = 'localhost:9092'
    topic = 'twitter_topic'

    # Appel de la fonction de consommation
    consume_from_topic(bootstrap_servers, topic)
