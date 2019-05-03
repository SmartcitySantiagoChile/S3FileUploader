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
virtualenv -p python3 envname
```


Luego se debe activar el entorno virtual e instalar las dependencias.
 
```
# activar
source venv/bin/activate
 
# intalar dependencias
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
 
