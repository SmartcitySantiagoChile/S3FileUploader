# Mini programa para subir archivos a un bucket S3

El objetivo de este programa es enviar archivos almacenados en la máquina local a un bucket en el servicio S3 de AWS

Es importante destacar que para que funcione el envío, las credenciales dadas a boto3 (librería cliente de aws) deben tener permisos de escritura


# Instalación

Se recomienda la utilización de un entorno virtual, puedes crearlo dentro de la misma carpeta

```
virtualenv venv
```

Luego se debe activar e instalar las dependencias
 
 ```
 # activar
 source venv/bin/activate
 
 # intalar dependencias
 pip intall -r requirements.txt
 ```

El siguiente paso es generar el archivo que almacenará las llaves de acceso a aws. Este archivo debe llamarse `.env` y en su contenido es el siguiente:
```
AWS_ACCESS_KEY_ID='PUT_HERE_YOUR_ACCESS_KEY'
AWS_SECRET_ACCESS_KEY='PUT_HERE_YOUR_SECRET_ACCESS_KEY'
```
`PUT_HERE_YOUR_ACCESS_KEY` y `PUT_HERE_YOUR_SECRET_ACCESS_KEY` se obtienen de un usuario de aws (https://console.aws.amazon.com/iam/home?#/users), sección 'Credenciales de seguridad'

# Ejecutar pruebas 
Para comprobar que todo está en orden puede ejecutar los tests
 
 ```
 python -m test
 ```