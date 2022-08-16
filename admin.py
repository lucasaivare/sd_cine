import os
import threading
from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads
from utils import getData, listMovies, selectMovie, setData

FILE_NAME = "data_admin.json"
TOPIC = "movies"
data = []

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: dumps(x).encode("utf-8"),
)

consumer = KafkaConsumer(
    "tickets",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda x: loads(x.decode("utf-8")),
)

consumer_movies = KafkaConsumer(
    "get_movies",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda x: loads(x.decode("utf-8")),
)


def receive_data():
    for message in consumer:
        message = message.value
        data = message
        setData(data, FILE_NAME)
        print("Ingressos foram comprados: \n")
        listMovies(data)
        # main()


def receive_data_movie():
    for message in consumer_movies:
        message_value = message.value
        print(message_value)
        if message.value == "get_movies":
            data = getData(FILE_NAME)
            producer.send(TOPIC, value=data)
            print("Dados enviados com sucesso!")


thread = threading.Thread(target=receive_data)
thread_movie = threading.Thread(target=receive_data_movie)

thread.start()
thread_movie.start()


def send_request():
    producer.send(TOPIC, value={data})
    print("Dados enviados com sucesso!")


def main():
    action = input(
        "1 - Inserir novo filme\n2 - Listar filmes\n3 - Deletar filme\n4 - Editar nome\n5 - Editar espaços disponíveis \n6 - Sair\n"
    )
    data = getData(FILE_NAME)
    os.system("clear")
    if action == "1":
        title = input("Título: ")
        spaceAvailable = int(input("Espaços liberados: "))
        data.append({"title": title, "spaceAvailable": spaceAvailable})
        producer.send(
            TOPIC,
            value=data,
        )
        setData(data, FILE_NAME)
        print("Filme inserido com sucesso!\n")
    elif action == "2":
        listMovies(data)
    elif action == "3":
        movieIndex = selectMovie(data)
        data.pop(movieIndex)
        setData(data, FILE_NAME)
        producer.send(
            TOPIC,
            value=data,
        )
    elif action == "4":
        movieIndex = selectMovie(data)
        title = input("Novo título: ")
        data[movieIndex]["title"] = title
        setData(data, FILE_NAME)
        producer.send(
            TOPIC,
            value=data,
        )
    elif action == "5":
        movieIndex = selectMovie(data)
        spaceAvailable = int(input("Novo espaço disponível: "))
        if spaceAvailable < 0:
            print("Espaço disponível não pode ser negativo!")
            return
        data[movieIndex]["spaceAvailable"] = spaceAvailable
        setData(data, FILE_NAME)
        producer.send(
            TOPIC,
            value=data,
        )
    elif action == "6":
        print("Saindo...")
        thread.join()
        thread_movie.join()
        exit()
    main()


main()
