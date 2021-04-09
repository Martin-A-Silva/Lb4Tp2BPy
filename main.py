# Ejecutar en terminal: python -m pip install requests pymongo
import requests, pymongo, json

# Setup de conexion a la bbdd
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["paises_db"]
collec = db["paises"]


def populate():
    for i in range(1, 301):

        try:
            # Conseguimos el json de la url
            dataURL = requests.get(f'https://restcountries.eu/rest/v2/callingcode/{i}').json()[0]

            data = {
                "codigoPais": dataURL["callingCodes"][0],
                "nombrePais": dataURL["name"],
                "capitalPais": dataURL["capital"],
                "region": dataURL["region"],
                "poblacion": dataURL["population"],
                "latitud": dataURL["latlng"][0],
                "longitud": dataURL["latlng"][1],
                "superficie": dataURL["area"]
            }

            # Buscamos si ya hay una entrada en la bbdd

            if collec.count_documents({"codigoPais": str(i)}) == 0:

                # Si no la hay, la creamos
                print(f"El codigo {i} no se encuentra en la bbdd, insertando...")

                collec.insert_one(data)


            else:

                # Si la hay, la actualizamos
                print(f"El codigo {i} ya se encuentra en la bbdd, actualizando...")

                collec.update_one({"codigoPais": str(i)}, {"$set": data})

        except KeyError:
            print(f"El código {i} no existe en restcountries")
        except Exception as ex:
            print(ex)


def select_americas():
    for x in collec.find({"region": "Americas"}):
        print(x)


def select_americas_population():
    for x in collec.find({"region": "Americas", "poblacion": {"$gt": 100000000}}):
        print(x)


def select_not_africa():
    for x in collec.find({"region": {"$ne": "Africa"}}):
        print(x)


def update_egypt():
    collec.update_one({"nombrePais": "Egypt"}, {"$set": {"nombrePais": "Egipto", "poblacion": 95000000}})
    print(collec.find_one({"codigoPais": "20"}))


def delete_258():
    collec.delete_one({"codigoPais": "258"})


def metodo_drop():
    print("Usar el metodo drop() de una colección elimina todos sus documentos, indices, y la colección en sí")


def select_population_50m_150m():
    for x in collec.find({"poblacion": {"$gt": 50000000, "$lt": 150000000}}):
        print(x)


def select_name_ascend():
    for x in collec.find().sort("nombrePais"):
        print(x)


def metodo_skip():
    print("Usar skip() y pasandole un numero como argumento nos saltea una cantidad de documentos,"
          "por ejemplo, con skip(2) el metodo find() saltea los dos primeros documentos que son canadá y rusia")
    for x in collec.find().skip(2):
        print(x)


def regex():
    print("El uso de expresiones regulares (regex) equivale al uso de LIKE en SQL, por ejemplo, buscando paises"
          "que empiezen con 'Ar'")
    for x in collec.find({"nombrePais": {"$regex": "^Ar"}}):
        print(x)


def crear_index():
    print(collec.create_index([("codigoPais", 1)]))


def backup():
    print("La API PyMongo no provee herramientas para hacer backup, para eso hay que hacerlo desde una terminal"
          "con el comando mongodump --dbpath PATH_DE_LA_BBDD --out PATH_DEL_BACKUP (por defecto en /bin/dump/"
          "y después restaurando con mongorestore")




opcion = 14
while (opcion != 13):
    print("\nElija que punto ejecutar:\n"
          "0 Poblar la bbdd\n"
          "1 Codifique un método que seleccione los documentos de la colección países donde la región sea "
          "Americas.\n"
          "2 Codifique un método que seleccione los documentos de la colección países donde la región sea "
          "Americas y la población sea mayor a 100000000\n"
          "3 Codifique un método que seleccione los documentos de la colección países donde la región sea "
          "distinto de Africa. (investigue $ne).\n"
          "4 Codifique un método que actualice el documento de la colección países donde el name sea Egypt, "
          "cambiando el name a “Egipto” y la población a 95000000\n"
          "5 Codifique un método que elimine el documento de la colección países donde el código del país sea 258\n"
          "6 Describa que sucede al ejecutar el método drop() sobre una colección y sobre una base de datos.\n"
          "7 Codifique un método que seleccione los documentos de la colección países cuya población sea "
          "mayor a 50000000 y menor a 150000000.\n"
          "8 Codifique un método que seleccione los documentos de la colección países ordenados por nombre (name) "
          "en forma Ascendente. sort().\n"
          "9 Describa que sucede al ejecutar el método skip() sobre una colección. Ejemplifique con la colección países.\n"
          "10 Describa y ejemplifique como el uso de expresiones regulares en Mongo puede reemplazar el uso de la "
          "cláusula LIKE de SQL.\n"
          "11 Cree un nuevo índice para la colección países asignando el campo código como índice. "
          "investigue createIndex())\n"
          "12 Describa como se realiza un backup de la base de datos mongo países_db.\n"
          "13 Salir")
    opcion = int(input())
    if opcion == 0:
        populate()
    elif opcion == 1:
        select_americas()
    elif opcion == 2:
        select_americas_population()
    elif opcion == 3:
        select_not_africa()
    elif opcion == 4:
        update_egypt()
    elif opcion == 5:
        delete_258()
    elif opcion == 6:
        metodo_drop()
    elif opcion == 7:
        select_population_50m_150m()
    elif opcion == 8:
        select_name_ascend()
    elif opcion == 9:
        metodo_skip()
    elif opcion == 10:
        regex()
    elif opcion == 11:
        crear_index()
    elif opcion == 12:
        backup()
    elif opcion == 13:
        pass
    else:
        print("opcion no valida")
