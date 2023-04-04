[![Build Status](https://travis-ci.com/SmartcitySantiagoChile/S3FileUploader.svg?branch=master)](https://travis-ci.com/SmartcitySantiagoChile/S3FileUploader)
[![Coverage Status](https://coveralls.io/repos/github/SmartcitySantiagoChile/S3FileUploader/badge.svg?branch=master)](https://coveralls.io/github/SmartcitySantiagoChile/S3FileUploader?branch=master)
# Mini programa para subir archivos a un bucket S3

El objetivo de este programa es enviar archivos almacenados en la máquina local a un bucket en el servicio S3 de AWS.

Es importante destacar que para que funcione el envío, las credenciales dadas a boto3 (librería cliente de aws) deben tener permisos de escritura.

# Requisitos    

- Python 3
- dependencias (mirar archivo `requirements.txt`)

# Instalación

Se recomienda la utilización de un entorno virtual, puedes crearlo dentro de la misma carpeta.

```
virtualenv venv
```

En caso de tener python 2.7 por defecto es necesario definir que sea python3 para el entorno virtual

```
virtualenv -p python3 venv
```


Luego se debe activar el entorno virtual e instalar las dependencias.
 
```
# activar
source venv/bin/activate
 
# instalar dependencias
pip install -r requirements.txt
```

El siguiente paso es generar el archivo que almacenará las llaves de acceso a aws. Este archivo debe llamarse `.env` y su contenido es el siguiente:
```
AWS_ACCESS_KEY_ID='PUT_HERE_YOUR_ACCESS_KEY'
AWS_SECRET_ACCESS_KEY='PUT_HERE_YOUR_SECRET_ACCESS_KEY'
```
`PUT_HERE_YOUR_ACCESS_KEY` y `PUT_HERE_YOUR_SECRET_ACCESS_KEY` se obtienen de un usuario de aws (https://console.aws.amazon.com/iam/home?#/users), sección 'Credenciales de seguridad'

# Ejecutar pruebas 
Para comprobar que todo está en orden puede ejecutar los tests.
 
```
python -m unittest discover
```
 
 # Ejecutar programa
 
 Existen dos comandos: `upload_to_s3.py` y `delete_object_in_s3.py`. El primero sube uno o más archivos a un bucket en S3 y el segundo permite eliminar un objecto (archivo) en s3.
 
 ## Ejemplo de ejecución
 
 ### Comando upload_to_s3.py
```
python upload_to_s3.py ruta/a/archivo*.gz nombre_bucket
```
  Notar que la ruta al archivo puede ser un patrón, esto permite subir varios archivos a la vez.
  
 #### Ayuda
```
# consultar ayuda
python upload_to_s3.py --help

usage: upload_to_s3.py [-h] [--omit-filename-check] [--replace]
                       file [file ...] bucket

move document to S3 bucket

positional arguments:
  file                  data file path. It can be a pattern, e.g. /path/to/file or /path/to/file*.zip
  bucket                bucket name. Valid options are:

optional arguments:
  -h, --help            show this help message and exit
  --omit-filename-check It Accepts filenames with distinct format to YYYY-mm-dd.*
  --replace             It replaces file if exists in bucket, default behavior ask to user a confirmation
```
  
 ### Comando delete_object_in_s3.py
```
python delete_object_in_s3.py nombre_archivo nombre_bucket
```
  A diferencia del comando anterior el primer parámetro es el nombre del archivo, no la ruta en el disco local.
  
 #### Ayuda
```
# consultar ayuda
python delete_object_in_s3.py --help
 
usage: delete_object_in_s3.py [-h] filename bucket

delete an object from S3 bucket

positional arguments:
  filename    filename to delete in bucket
  bucket      bucket name. Valid options are:

optional arguments:
  -h, --help  show this help message and exit
```
 

 ### Comando download_from_s3.py
```
python download_from_s3.py nombre_archivo nombre_bucket --destination-path /home/user
```
  El primer parámetro es el nombre del archivo a descargar, el segundo corresponde al nombre del bucket en que se 
  encuentra el archivo, y por último, existe un parámetro opcional que permite definir la ruta donde se guardará el 
  archivo, si es omitido el archivo será guardado en el `current working directory` (dado por `os.getcwd()`)

 #### Ayuda
```
# consultar ayuda
python download_from_s3.py --help
 
usage: download_from_s3.py [-h] [--destination-path DESTINATION_PATH]
                           filename [filename ...] bucket

download one or more objects from S3 bucket

positional arguments:
  filename              one or more filenames
  bucket                bucket name

optional arguments:
  -h, --help            show this help message and exit
  --destination-path DESTINATION_PATH
                        path where files will be saved, if it is not provided
                        we will use current path

```
### Comando delete_bucket_from_s3.py
```
python delete_bucket_from_s3.py nombre_bucket
```
El parámetro es el nombre del bucket a eliminar. Este comando eliminará el bucket junto a todos sus archivos

```
usage: delete_bucket_from_s3.py [-h] bucket_name

delete S3 bucket

positional arguments:
  bucket_name  bucket name

optional arguments:
  -h, --help   show this help message and exit
```

### Comando move_bucket_from_s3.py
```
python move_bucket_from_s3.py bucket_origen bucket_destino --filename nombre_archivo --extension filtro_de_extensiones
```

El primer parámetro es el bucket desde donde se copiarán los archivos. El segundo comando es el bucket al que se copiarán los archivos.
El parámetro opcional nombre_archivo es para solo mover uno o más archivos según nombre de archivo. El último parámetro opcional, 
filtro_de_extensiones, permite mover los archivos que solo contengan las extensiones indicadas. Ej: .viajes


 #### Ayuda
```
# consultar ayuda
usage: move_bucket_from_s3.py [-h] [-f [FILENAME [FILENAME ...]]] [-e [EXTENSION_FILTER [EXTENSION_FILTER ...]]]
                              source_bucket target_bucket

move one or more objects from source S3 bucket to target S3 bucket

positional arguments:
  source_bucket         source bucket name
  target_bucket         target bucket name

optional arguments:
  -h, --help            show this help message and exit
  -f [FILENAME [FILENAME ...]], --filename [FILENAME [FILENAME ...]]
                        one or more filenames
  -e [EXTENSION_FILTER [EXTENSION_FILTER ...]], --extension [EXTENSION_FILTER [EXTENSION_FILTER ...]]
                        only files with this extension will be moved


```


### Comando update_objets_from_s3.py
```
python move_objets_from_s3.py bucket extension fecha_inicial fecha_final tuplas --destination-path directorio 
```

El primer parámetro es el bucket desde donde se actualizarán los archivos.
El segundo parámetro es la extensión del bucket, puede ser un patrón. Ej: .bip* o .trip.gz.
El tercer parámetro es la fecha inicial en formato YY-MM-DD. Ej: 2022-06-30.
El cuarto parámetro es la fecha final en formato YY-MM-DD. Ej: 2022-07-10.
El quinto parámetro es un listado de 3-tuplas para reemplazar valores en objectos, deben estar entre comillas y sin espacio entre los elementos de las tuplas. Ej: "[0,2,3] [8,LABORAL,FERIADO]"
El parámetro opcional directorio permite definir un directorio para almacenar las versiones locales de los archivos sin modificar. Ej: --destination-path data. En caso de no definirlo, los archivos locales se almacenaran en la misma carpeta del proyecto S3FileUploader.

#### Ayuda 
```
usage: update_objects_from_s3.py [-h] [--destination-path DESTINATION_PATH]
                                 bucket extension start_date end_date tuples

update one or more objects from S3 bucket

positional arguments:
  bucket                bucket name
  extension             Object bucket files extension, can be a pattern.
                        Example: .bip*
  start_date            End date to process in YYYY-MM-DD format .
  end_date              Start date to process in YYYY-MM-DD format .
  tuples                3-Tuples of values to be replaced on the files, must
                        be in quotes and the elements without separation.
                        Example: "[0,2,3] [2,METRO,BUS]"

optional arguments:
  -h, --help            show this help message and exit
  --destination-path DESTINATION_PATH
                        path where files will be saved, if it is not provided
                        we will use current path
```