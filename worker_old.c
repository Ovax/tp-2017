#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <commons/config.h>
#include <commons/log.h>
#include "registros.h"
#include "sockets.h"
#include "funciones.h"

#define SIZE (1024*1024)

t_log* infoLogger;
t_log* traceLogger;
t_log* errorLogger;
t_config* cfg;

int main(int argc, char* argv[]){

	int fd_maximo, i, newfd, size;
	char* argumento;
	void* bloque = malloc(1024*1024);

	pid_t pid;

	int pipe_padreAHijo[2];
	int pipe_hijoAPadre[2];


	pipe(pipe_padreAHijo);
	pipe(pipe_hijoAPadre);

	char proceso;
	int status, sizeBuffer,codigo, tam_payload;
	char* buffer=malloc(SIZE);

	FILE * databin;

	Encabezado header;
	Paquete payload, paquete_respuesta, paquete;

	t_delegacion_transformacion data_transformacion;
	t_delegacion_reduccion_local data_reduccion_local;
	t_delegacion_reduccion_global data_reduccion_global;
	t_bloque_contenido contenido_archivo;

	fd_set master,read_fd;
	FD_ZERO(&master);
	FD_ZERO(&read_fd);


	/* Creo la instancia del Archivo de Configuracion */
	cfg = config_create("config/config.cfg");
	/* Creo la instancia de Log */
	infoLogger = log_create("log/worker.log", "Worker", false, LOG_LEVEL_INFO);

	// Logueando
    log_info(infoLogger, "Iniciando WORKER." );
    printf("Iniciando WORKER\n");

	// Creo el Servidor para escuchar conexiones

	int worker_fd=crearServidor(config_get_int_value(cfg,"NODO_PUERTO"));
	fd_maximo = worker_fd;
    log_info(infoLogger, "Escuchando conexiones" );
    printf("Escuchando conexiones\n");

	char* revbuf;

    while (1) {

		read_fd = master;

		if (select(fd_maximo + 1, &read_fd, NULL, NULL, NULL) == -1) {
			perror("select");
			exit(1);
		}


		for (i = 0; i <= fd_maximo; i++) {
			if (FD_ISSET(i, &read_fd)) { // ¡¡tenemos datos!!
				if (i == worker_fd) {
					// gestionar nuevas conexiones
					if ( (newfd = aceptarconexion(worker_fd) ) == -1) {
						perror("accept");
					} else {
						FD_SET(newfd, &master); // añadir al conjunto maestro
						if (newfd > fd_maximo) {    // actualizar el máximo
							fd_maximo = newfd;
						}
					}
				} else {
					header = recibir_header(&i);
	//				payload = recibir_payload(&i,&(header.tam_payload));

					if( (pid = fork()) == 0){ //creo un proceso hijo

						dup2(pipe_padreAHijo[0],STDIN_FILENO);
						dup2(pipe_hijoAPadre[1],STDOUT_FILENO);

						close( pipe_padreAHijo[1] );
						close( pipe_hijoAPadre[0] );
						close( pipe_hijoAPadre[1] );
						close( pipe_padreAHijo[0] );

						char *argv[] = {NULL};
						char *envp[] = {NULL};

						switch(header.cod_operacion){

							case COPIAR_TRANSFORMADOR:

								printf("Inicio de recepción del Archivo Transformador de Master.\n");
								log_info(infoLogger,"Inicio de recepción del Archivo Transformador de Master.");

								// Recibiendo archivo del MASTER
								paquete=recibir_payload(&i,&header.tam_payload);
								contenido_archivo=dsrlz_bloque_archivo(paquete.buffer);
								free(paquete.buffer);

								printf("Archivo recibido de Master.\n");
								log_info(infoLogger,"Archivo recibido de Master");

								// Persisto el Archivo en el Worker
								persistirArchivo("files/copia-transformador.sh",contenido_archivo.contenido_archivo, header.tam_payload-(sizeof(int)*2), infoLogger);

								printf("Archivo persistido en el Worker.\n");
								log_info(infoLogger,"Archivo persistido en el Worker");
								free(contenido_archivo.contenido_archivo);

								break;

							case INICIAR_TRANSFORMACION_ARCHIVO:
								data_transformacion = dsrlz_delegacionTransformacion(payload.buffer);
//								execve(data_transformacion.programa_transformacion, argv, envp);
								//	Falta persistir el script en disco, se necesita respuesta al envio de la transformacion para pasa transformador

									exit(1);

								//argumento = malloc(strlen("echo ") +
								//					strlen(bloque) +
								//					strlen(" | ") +
								//					strlen(data_transformacion.programa_transformacion)+
								//					strlen(" | sort > ") +
								//					strlen(data_transformacion.archivo_destino)
								//					);

								// NO SE SI EL PRIMER ARGUMENTO ESTA BIEN, POR PARTE DEL BLOQUE
								//argumento = concatenar("echo ", bloque);
								//argumento = concatenar(argumento, " | ");
								//argumento = concatenar(argumento, data_transformacion.programa_transformacion);
								//argumento = concatenar(argumento, " | sort > ");
								//argumento = concatenar(argumento, data_transformacion.archivo_destino);

								//system(argumento);


								break;
							case INICIAR_REDUCCION_ARCHIVO_LOCAL:
								log_info(infoLogger, " Mensaje recibido de Master : Realizar Reduccion Local " );
//								data_reduccion_local = dsrlz_delegacionReduccionLocal(payload.buffer);
								break;
							case INICIAR_REDUCCION_ARCHIVO_GLOBAL:
								log_info(infoLogger, " Mensaje recibido de Master : Realizar Reduccion Global " );
								//data_reduccion_global = dsrlz_delegacionReduccionGlobal(payload.buffer);
								break;
							case ALMACENAR_ARCHIVO:
								log_info(infoLogger, " Mensaje recibido de Master : Almacenar Archivo " );
								//data_almacenar_archivo = dsrlz_delegacionAlmacenarArchivo(payload.buffer);
								break;
						}

					}else{ //proceso padre

						close( pipe_padreAHijo[0] );
						close( pipe_hijoAPadre[1] );
						switch(header.cod_operacion){
							case INICIAR_TRANSFORMACION_ARCHIVO:
								data_transformacion = dsrlz_delegacionTransformacion(payload.buffer);
								log_info(infoLogger, " Mensaje recibido de Master : Realizar Transformación " );

// EL ARCHIVO data.bin LO CREA EL DATANODE Y EL WORKER SOLO LO LEE!!!
/*
								//PARA LA PRIMERA VEZ QUE CORRO EL PROGRAMA (creo el archivo data.bin)
								int X = 1024 * 1024 *20 - 1; //20 MB
								FILE *fp = fopen("./metadata/data.bin", "w");
								fseek(fp, X , SEEK_SET);
								fputc('\0', fp);
								fclose(fp);
*/
								//****************************************//

								databin = fopen("./metadata/data.bin", "r");
								fseek(databin, data_transformacion.bloque * SIZE ,SEEK_SET); //me posiciono en el bloque a leer
								fread(bloque,1,SIZE,databin); // lo leo en bloque
								fclose(databin);

								write( pipe_padreAHijo[1], bloque ,SIZE); //mandarle al hijo el bloque
								close( pipe_padreAHijo[1]);
								waitpid(pid,&status,0);

								read( pipe_hijoAPadre[0], bloque, SIZE ); //obtener en bloque el resultado de aplicar el prog
								close( pipe_hijoAPadre[0]);

								databin = fopen(data_transformacion.archivo_destino, "w"); //vuelvo a usar databin porque se cerro previamente
								fwrite(bloque, 1, SIZE, databin);
								fclose(databin);

								sizeBuffer = (sizeof(int) *2 + sizeof(char)); //INT COD OP, INT TAM PAYLOAD, CHAR PROCESO

								paquete_respuesta.tam_buffer = sizeBuffer;
								paquete_respuesta.buffer = malloc( sizeBuffer );

								proceso = 'W';
								codigo = TRANSFORMACION_TERMINADA_CON_EXITO;
								tam_payload = 0;

								//memcpy(dir de dónde copiaré, dir de lo que copiaré, tamaño de lo que copiaré)
								memcpy(paquete_respuesta.buffer                 ,&(proceso)         ,sizeof(char));
								memcpy(paquete_respuesta.buffer + sizeof(char)	,&(codigo)			,sizeof(int)); //copio el codigo
								memcpy(paquete_respuesta.buffer + sizeof(char)+ sizeof(int)	,&(tam_payload)			,sizeof(int));

								sendall(i, paquete_respuesta.buffer, paquete_respuesta.tam_buffer);

								break;
							case INICIAR_REDUCCION_ARCHIVO_LOCAL:
								break;
							case INICIAR_REDUCCION_ARCHIVO_GLOBAL:
								break;
							case ALMACENAR_ARCHIVO:
								break;
						}
					}
				}
			}
		}
	}

	/* ---------------------------------------- */
	/*  Libero Memoria de Log y Config			*/
	/* ---------------------------------------- */

	log_destroy(infoLogger);
	log_destroy(traceLogger);
	log_destroy(errorLogger);
	config_destroy(cfg);

	return 0;
}
