import json

FILE_NAME = "data_client.json"


def setData(data, fileName=FILE_NAME):
    json_object = json.dumps(data, indent=4)
    with open(fileName, "w+") as outfile:
        outfile.write(json_object)


def getData(fileName=FILE_NAME):
    try:
        with open(fileName, "r+") as openFile:
            json_object = json.load(openFile)
        return json_object
    except FileNotFoundError:
        return []


def listMovies(data):
    print("Filmes em cartaz:\n")
    for movie in data:
        print(movie)
    print("\n")


def selectMovie(data):
    for i in range(len(data)):
        print(
            str(i + 1)
            + " - "
            + data[i].get("title")
            + " - "
            + str(data[i].get("spaceAvailable"))
            + " ingressos"
        )
    movieIndex = int(input("\nEscolha o filme: ")) - 1
    return movieIndex
