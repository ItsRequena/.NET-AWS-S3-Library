using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace AWS3
{
    /* ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        LIBRERÍA PARA LAS FUNCIONALIDADES DE AMAZON S3
        Desarrollado por: itsRequena (GitHub: https://github.com/ItsRequena)
     --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- */
    public class S3
    {
        /* ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            METODO QUE SUBE ARCHIVOS A UN BUCKET DE AMAZON S3
            Para poder subir el archivo es necesario tener informacion sobre:
            - AccessKey y SecretAccessKey del usuario. 
            - ServiceURL (url donde se encuentra instanciado el bucket), Bucket (nombre del bucket) y Folder (ruta dentro del bucket donde se desea almacenar el archivo)
            - file (archivo que se desea almacenar)
            El método devolverá un 0 si todo ha ido correctamente, en caso contrario devolvera un numero distinto a 0
         --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- */
        public async static Task<int> addFile(string AccessKey, string SecretAccessKey, string ServiceURL, string Bucket, string Folder, HttpPostedFile file)
        {
            // comprobamos que el archivo no sea vacio
            if (file.ContentLength > 0)
            {
                try
                {
                    // Accedemos al cliente de Amazon S3 con nuestras credenciales
                    using (var amazonS3client = new AmazonS3Client(AccessKey, SecretAccessKey, new AmazonS3Config
                    {
                        ServiceURL = ServiceURL
                    }))
                    {
                        // Creamos la peticion con el contenido del fichero deseado
                        var request = new TransferUtilityUploadRequest
                        {
                            BucketName = Bucket,
                            Key = Folder + file.FileName,
                            InputStream = file.InputStream,
                            ContentType = file.ContentType,
                        };

                        // Realizamos la subida del archvio
                        var transferUtility = new TransferUtility(amazonS3client);
                        await transferUtility.UploadAsync(request);
                    }
                    return 0;
                }
                catch
                {
                    return 1; // error al subir el archivo a Amazon S3
                }
            }
            return 2; // error en el fichero

        }

        /* ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            METODO QUE ELIMINA ARCHIVOS DE UN BUCKET DE AMAZON S3
            Para poder eliminar el archivo es necesario tener informacion sobre:
            - AccessKey y SecretAccessKey del usuario. 
            - ServiceURL (url donde se encuentra instanciado el bucket), Bucket (nombre del bucket) y Folder (ruta dentro del bucket donde se encuentra el archivo)
            - fileName (nombre del archivo que se desea eliminar)
            El método devolverá un 0 si todo ha ido correctamente, en caso contrario devolvera un numero distinto a 0
         --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- */
        public async static Task<int> deleteFile(string AccessKey, string SecretAccessKey, string ServiceURL, string Bucket, string Folder, string fileName)
        {
            try
            {
                // Accedemos al cliente de Amazon S3 con nuestras credenciales
                using (var amazonS3client = new AmazonS3Client(AccessKey, SecretAccessKey, new AmazonS3Config
                {
                    ServiceURL = ServiceURL
                }))
                {
                    // Eliminamos el archivo indicando el bucket y el archivo
                    var transferUtility = new TransferUtility(amazonS3client);
                    await transferUtility.S3Client.DeleteObjectAsync(new DeleteObjectRequest()
                    {
                        BucketName = Bucket,
                        Key = Folder + fileName,
                    });
                }
                return 0;
            }
            catch
            {
                return 1; // error al eliminar el archivo a Amazon S3
            }

        }

        /* ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            METODO QUE DESCARGA ARCHIVOS DE UN BUCKET DE AMAZON S3
            Para poder descargar el archivo es necesario tener informacion sobre:
            - AccessKey y SecretAccessKey del usuario. 
            - ServiceURL (url donde se encuentra instanciado el bucket), Bucket (nombre del bucket) y Folder (ruta dentro del bucket donde se encuentra el archivo)
            - fileName (nombre del archivo que se desea descargar)
            El método devolverá el archivo si todo ha ido correctamente, en caso contrario devolvera un null
          --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- */
        public async static Task<Dictionary<string,Stream>> downloadFile(string AccessKey, string SecretAccessKey, string ServiceURL, string Bucket, string Folder, string fileName)
        {
            try
            {
                // Accedemos al cliente de Amazon S3 con nuestras credenciales
                using (var amazonS3client = new AmazonS3Client(AccessKey, SecretAccessKey, new AmazonS3Config
                {
                    ServiceURL = ServiceURL
                }))
                {
                    // Buscamos el fichero en el bucket de Amazon S3 para descargar
                    var transferUtility = new TransferUtility(amazonS3client);
                    var response = await transferUtility.S3Client.GetObjectAsync(new GetObjectRequest()
                    {
                        BucketName = Bucket,
                        Key = Folder + fileName
                    });
                    Dictionary<string, Stream> archivo = new Dictionary<string, Stream>();
                    string nombreArchivo = Path.GetFileName(response.Key);
                    archivo.Add(nombreArchivo, response.ResponseStream);
                    return archivo;
                }
            }
            catch
            {
                return null; // error al descargar el archivo a Amazon S3
            }

        }


        /* ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            METODO QUE DESCARGA TODOS LOS ARCHIVOS DE UNA CARPETA DENTRO DE UN BUCKET DE AMAZON S3
            Para poder descargar todos los archivos es necesario tener informacion sobre:
            - AccessKey y SecretAccessKey del usuario. 
            - ServiceURL (url donde se encuentra instanciado el bucket), Bucket (nombre del bucket) y Folder (ruta dentro del bucket donde se encuentra el archivo)
            El método devolverá una lista con todos los archivos si todo ha ido correctamente, en caso contrario devolvera un nulo
         --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- */
        public async static Task<Dictionary<string, Stream>> downloadAllFilesFromFolder(string AccessKey, string SecretAccessKey, string ServiceURL, string Bucket, string Folder)
        {
            Dictionary<string, Stream> listaArchivos = new Dictionary<string, Stream>();
            try
            {
                // Accedemos al cliente de Amazon S3 con nuestras credenciales
                using (var amazonS3client = new AmazonS3Client(AccessKey, SecretAccessKey, new AmazonS3Config
                {
                    ServiceURL = ServiceURL
                }))
                {
                    // Buscamos la carpeta dentro del bucket de Amazon S3 para descargar
                    var request = new ListObjectsV2Request
                    {
                        BucketName = Bucket,
                        Prefix = Folder
                    };

                    ListObjectsV2Response response;

                    do
                    {
                        response = await amazonS3client.ListObjectsV2Async(request);
                        // Áñadir archivos a la lista
                        foreach (var file in response.S3Objects)
                        {
                            // Obtenemos el fichero en dicha carpeta
                            var transferUtility = new TransferUtility(amazonS3client);
                            var respuesta = await transferUtility.S3Client.GetObjectAsync(new GetObjectRequest()
                            {
                                BucketName = Bucket,
                                Key = file.Key
                            });
                            Stream stream = respuesta.ResponseStream;
                            string fileName = Path.GetFileName(respuesta.Key);
                            // Descartamos archivos sin contenido (0 Bytes)
                            if (stream.Length != 0)
                            {
                                listaArchivos.Add(fileName, stream);
                            }
                        }
                        // Continua si hay más resultados
                        request.ContinuationToken = response.NextContinuationToken;
                    } while (response.IsTruncated);
                }
                return listaArchivos;
            }
            catch
            {
                return null; // error
            }

        }


        /* ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            METODO QUE DESCARGA TODOS LOS NOMBRES DE LOS ARCHIVOS DE UNA CARPETA DENTRO DE UN BUCKET DE AMAZON S3
            Para poder descargar todos nombres de los archivos es necesario tener informacion sobre:
            - AccessKey y SecretAccessKey del usuario. 
            - ServiceURL (url donde se encuentra instanciado el bucket), Bucket (nombre del bucket) y Folder (ruta dentro del bucket donde se encuentra el archivo)
            El método devolverá una lista con todos los nombres de los archivos si todo ha ido correctamente, en caso contrario devolvera un nulo
         --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- */
        public async static Task<List<string>> allFilenamesFromFolder(string AccessKey, string SecretAccessKey, string ServiceURL, string Bucket, string Folder)
        {
            List<string> listaArchivos = new List<string>();
            try
            {
                // Accedemos al cliente de Amazon S3 con nuestras credenciales
                using (var amazonS3client = new AmazonS3Client(AccessKey, SecretAccessKey, new AmazonS3Config
                {
                    ServiceURL = ServiceURL
                }))
                {
                    // Buscamos la carpeta dentro del bucket de Amazon S3 para descargar
                    var request = new ListObjectsV2Request
                    {
                        BucketName = Bucket,
                        Prefix = Folder
                    };

                    ListObjectsV2Response response;

                    do
                    {
                        response = await amazonS3client.ListObjectsV2Async(request);
                        // Áñadir archivos a la lista
                        foreach (var file in response.S3Objects)
                        {
                            // Obtenemos el nombre del fichero
                            string fileName = Path.GetFileName(file.Key);
                            // Descartamos archivos sin contenido (0 Bytes)
                            if (file.Size != 0)
                            {
                                listaArchivos.Add(fileName);
                            }
                        }
                        // Continua si hay más resultados
                        request.ContinuationToken = response.NextContinuationToken;
                    } while (response.IsTruncated);
                }
                return listaArchivos;
            }
            catch
            {
                return null; // error
            }

        }


        /* ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            METODO QUE BUSCA SI EXISTE EL ARCHIVO EN TODO EL BUCKET DE AMAZON S3
            Para poder buscar el archivo es necesario tener informacion sobre:
            - AccessKey y SecretAccessKey del usuario. 
            - ServiceURL (url donde se encuentra instanciado el bucket), Bucket (nombre del bucket)
            - fileName (nobre del archivo que se desea buscar)
            El método devolverá el archivo si todo a ido correctamente, en caso contrario devolvera un null
         --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- */
        public async static Task<Dictionary<string,Stream>> searchFile(string AccessKey, string SecretAccessKey, string ServiceURL, string Bucket, string fileName)
        {
            try
            {
                // Accedemos al cliente de Amazon S3 con nuestras credenciales
                using (var amazonS3client = new AmazonS3Client(AccessKey, SecretAccessKey, new AmazonS3Config
                {
                    ServiceURL = ServiceURL
                }))
                {
                    ListObjectsV2Request request = new ListObjectsV2Request
                    {
                        BucketName = Bucket
                    };
                    ListObjectsV2Response response;
                    do
                    {
                        response = await amazonS3client.ListObjectsV2Async(request);

                        foreach (var obj in response.S3Objects)
                        {
                            if (obj.Key.EndsWith(fileName))
                            {
                                // Devolvemos el archivo
                                var transferUtility = new TransferUtility(amazonS3client);
                                var result = await transferUtility.S3Client.GetObjectAsync(new GetObjectRequest()
                                {
                                    BucketName = Bucket,
                                    Key = obj.Key
                                });
                                Dictionary<string, Stream> archivo = new Dictionary<string, Stream>();
                                string nombreArchivo = Path.GetFileName(result.Key);
                                archivo.Add(nombreArchivo, result.ResponseStream);
                                return archivo;
                            }
                        }
                        request.ContinuationToken = response.ContinuationToken;
                    } while (response.IsTruncated);
                    return null; // no ha encontrado el archivo 
                }
            }
            catch
            {
                return null; // error al descargar el archivo a Amazon S3
            }
        }


        /* ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            METODO QUE BUSCA SI EXISTE EL ARCHIVO DE TODO EL BUCKET DE AMAZON S3 Y DEVUELVE LA URL DONDE SE UBICA
            Para poder buscar el archivo es necesario tener informacion sobre:
            - AccessKey y SecretAccessKey del usuario. 
            - ServiceURL (url donde se encuentra instanciado el bucket), Bucket (nombre del bucket)
            - fileName (nobre del archivo que se desea buscar)
            El método devolverá la ruta del archivo donde se encuentra o, en caso contrario, devolvera un null
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- */
        public async static Task<string> searchFileURL(string AccessKey, string SecretAccessKey, string ServiceURL, string Bucket, string fileName)
        {
            try
            {
                // Accedemos al cliente de Amazon S3 con nuestras credenciales
                using (var amazonS3client = new AmazonS3Client(AccessKey, SecretAccessKey, new AmazonS3Config
                {
                    ServiceURL = ServiceURL
                }))
                {
                    ListObjectsV2Request request = new ListObjectsV2Request
                    {
                        BucketName = Bucket
                    };
                    ListObjectsV2Response response;
                    do
                    {
                        response = await amazonS3client.ListObjectsV2Async(request);

                        foreach (var obj in response.S3Objects)
                        {
                            if (obj.Key.EndsWith(fileName))
                            {
                                // Devolvemos el archivo
                                var transferUtility = new TransferUtility(amazonS3client);
                                var result = await transferUtility.S3Client.GetObjectAsync(new GetObjectRequest()
                                {
                                    BucketName = Bucket,
                                    Key = obj.Key
                                });
                                return Convert.ToString(result.Key);
                            }
                        }
                        request.ContinuationToken = response.ContinuationToken;
                    } while (response.IsTruncated);
                    return null; // no ha encontrado el archivo 
                }
            }
            catch
            {
                return null; // error al descargar el archivo a Amazon S3
            }

        }


        /* ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            METODO QUE DEVUELVE EL TAMAÑO DE UN ARCHIVO DENTRO DE UN BUCKET DE AMAZON S3
            Para poder buscar el tamaño del archivo es necesario tener informacion sobre:
            - AccessKey y SecretAccessKey del usuario. 
            - ServiceURL (url donde se encuentra instanciado el bucket), Bucket (nombre del bucket)
            - fileName (nobre del archivo que se desea buscar)
            El método devolverá el tamaño del archivo en bytes
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- */
        public async static Task<double> getBytes(string AccessKey, string SecretAccessKey, string ServiceURL, string Bucket, string Folder, string fileName)
        {
            try
            {
                // Accedemos al cliente de Amazon S3 con nuestras credenciales
                using (var amazonS3client = new AmazonS3Client(AccessKey, SecretAccessKey, new AmazonS3Config
                {
                    ServiceURL = ServiceURL
                }))
                {
                    // Buscamos el fichero en el bucket de Amazon S3 para descargar
                    var transferUtility = new TransferUtility(amazonS3client);
                    var response = await transferUtility.S3Client.GetObjectAsync(new GetObjectRequest()
                    {
                        BucketName = Bucket,
                        Key = Folder + fileName
                    });
                    return response.ContentLength;
                }
            }
            catch
            {
                return 0; // error al descargar el archivo a Amazon S3
            }

        }


    }
}
