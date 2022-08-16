import os
from time import sleep
from kafka import KafkaConsumer, KafkaProducer
from json import dumps, loads
import threading
from utils import getData, listMovies, setData

FILE_NAME = "data_client_2.json"

data = []

consumer = KafkaConsumer(
    "movies",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda x: loads(x.decode("utf-8")),
)

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: dumps(x).encode("utf-8"),
)


def receive_data():
    for message in consumer:
        message = message.value
        data = message
        setData(data, FILE_NAME)
        print("Filmes foram atualizados: \n", message)


thread = threading.Thread(target=receive_data)
thread.start()

print("Loading...")
sleep(0.5)
producer.send("get_movies", value="get_movies")
os.system("clear")


def main():
    action = input("1 - Comprar ingresso\n2 - Listar filmes\n3 - Sair\n")
    data = getData(FILE_NAME)
    os.system("clear")
    if action == "1":
        for i in range(len(data)):
            print(
                str(i + 1)
                + " - "
                + data[i].get("title")
                + " - "
                + str(data[i].get("spaceAvailable"))
                + " ingressos"
            )
        movie_index = int(input("\nEscolha o filme: ")) - 1
        if data[movie_index].get("spaceAvailable") > 0:
            count = int(input("Quantidade de ingressos: "))
            if count > data[movie_index].get("spaceAvailable"):
                print("Não há ingressos suficientes!")
                main()
            data[movie_index]["spaceAvailable"] -= count
            setData(data, FILE_NAME)
            producer.send("tickets", value=data)
            print("Ingresso comprado com sucesso!\n")
        else:
            print("Não há ingressos disponíveis!\n")
    elif action == "2":
        listMovies(data)
    else:
        print("Saindo...")
        thread.join()
        exit()
    main()


main()
