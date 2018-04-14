#include <arpa/inet.h>
#include <commons/config.h>
#include <commons/log.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include "funciones.h"
#include "registros.h"
#include "sockets.h"

/* ---------------------------------------- */
/*  Variables Globales 						*/
/* ---------------------------------------- */

t_log* infoLogger;
t_config* cfg;
int nuevo_fd;
Encabezado encabezado;
Paquete paquete;
bool respuesta,fallo;
t_bloque_contenido contenido_archivo;

/* ---------------------------------------- */

// Lista de Hilos

struct NodoHilo{
	pthread_t hilo;
	struct NodoHilo* sig;
};

///////////////HILOS///////////////

struct NodoHilo* hilos = NULL, *auxH;

//	Semáforos

pthread_mutex_t lockCont;

//			HEAD							//

void sigchld_handler(int s);
void avisoDeFinalizacion(bool* fallo, int nuevo_fd,t_log* infoLogger,char* etapa);
void* conectarWorker(void* worker);
void crearHilo();
void liberarHilos();


int main(int argc, char* argv[]){

	/* Creo la instancia del Archivo de Configuracion y del Log */
	cfg = config_create("config/config.cfg");
	infoLogger = log_create("log/worker.log", "Worker", false, LOG_LEVEL_INFO);

	log_info(infoLogger, "Iniciando WORKER" );
	printf("Iniciando WORKER\n");

    struct sockaddr_in their_addr; // información de la dirección de destino
    int sockfd,size,status;
    fd_set master,temporales;
    FD_ZERO(&master);
    FD_ZERO(&temporales);

    // Creo el Servidor para escuchar conexiones
    sockfd = crearServidor(config_get_int_value(cfg,"WORKER_PUERTO"));
    log_info(infoLogger, "Escuchando conexiones" );
    printf("Escuchando conexiones\n");

	struct sigaction sa;

	sa.sa_handler = sigchld_handler; // Eliminar procesos muertos
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_RESTART;

	if (sigaction(SIGCHLD, &sa, NULL) == -1) {
		perror("sigaction");
		exit(1);
	 }
//*/
	pid_t w;

	//	Cargo comandos
	char* 	cat		= malloc(strlen("cat ")				+1);
	char*	pipe	= malloc(strlen(" | ")				+1);
	char*	sort	= malloc(strlen("sort")				+1);
	char*	guardar	= malloc(strlen(" > ")				+1);
	char*	chmod = malloc(strlen("chmod +x ")			+1);
	strcpy(cat,		"cat "				);
	strcpy(pipe, 	" | "				);
	strcpy(sort,	"sort"				);
	strcpy(guardar,	" > "				);
	strcpy(chmod,	"chmod +x "			);

	while(1) { // main accept() loop
		size = sizeof(struct sockaddr_in);
		if ((nuevo_fd = accept(sockfd, (struct sockaddr *)&their_addr,&size)) == -1) {
			perror("accept");
			continue;
		}
		printf("Worker: existe conexión con %s\n", inet_ntoa(their_addr.sin_addr));

		if (!fork()) { // Este es el proceso hijo
		    log_info(infoLogger, "Proceso nuevo creado" );
			printf("Proceso nuevo creado\n");
			close(sockfd); // El hijo no necesita este descriptor

			while(1){	//	Hijo esclavo
				encabezado=recibir_header(&nuevo_fd);

				switch(encabezado.cod_operacion){


					case COPIAR_TRANSFORMADOR:

						printf("Inicio de recepción del Archivo Transformador de Master.\n");
						log_info(infoLogger,"Inicio de recepción del Archivo Transformador de Master.");

						// Recibiendo archivo del MASTER
						paquete=recibir_payload(&nuevo_fd,&encabezado.tam_payload);
						contenido_archivo=dsrlz_bloque_archivo(paquete.buffer);
						free(paquete.buffer);

						printf("Archivo recibido de Master.\n");
						log_info(infoLogger,"Archivo recibido de Master");

						char* nombreTransformador = "files/scripts/CopiaT-";
						removeN(contenido_archivo.nombre,6);		//Remuevo el /files (6 char)
						nombreTransformador = concatenar(nombreTransformador,contenido_archivo.nombre);

						// Persisto el Archivo en el Worker
						persistirArchivo(nombreTransformador,contenido_archivo.contenido_archivo, contenido_archivo.tamanio, infoLogger);
						printf("Nombre final del archivo: %s\n\n",nombreTransformador);

						printf("Archivo persistido en el Worker.\n");
						log_info(infoLogger,"Archivo persistido en el Worker");
						free(contenido_archivo.contenido_archivo);
						free(contenido_archivo.nombre);

					break;

					case INICIAR_TRANSFORMACION_ARCHIVO:

						printf("Inicio de recepción del la Transformacion de Master.\n");
						log_info(infoLogger,"Inicio de recepción de la Transformacion de Master.");

						t_delegacion_transformacion transformacion;
					    t_bloque_contenido* contenido_bloque_archivo = malloc(sizeof(t_bloque_contenido));
						char* comandoT = "";
						int tcomando = 0;

						paquete = recibir_payload(&nuevo_fd,&encabezado.tam_payload);
						transformacion = dsrlz_delegacionTransformacion(paquete.buffer);
						free(paquete.buffer);

						contenido_bloque_archivo->contenido_archivo = malloc(transformacion.num_bytes);
						char* nombreTempBloque = "";
						nombreTempBloque = concatenar(nombreTempBloque,transformacion.archivo_destino);
						removeN(nombreTempBloque,4);
						nombreTempBloque = concatenar("files/bloques",nombreTempBloque);
						transformacion.archivo_destino = concatenar("files",transformacion.archivo_destino);

						printf("\n\n\nBloque:\t%i\nBytes:\t%i\n\n",transformacion.bloque,transformacion.num_bytes);

						contenido_bloque_archivo = obtenerBloque(transformacion.bloque,transformacion.num_bytes,infoLogger);
						printf("Persistiendo archivo temporal con bloques:\n\t%s\ntamaño:\t%i\nDesde:\t%s\n",nombreTempBloque,transformacion.num_bytes,contenido_bloque_archivo->nombre);
						persistirArchivo(nombreTempBloque,contenido_bloque_archivo->contenido_archivo, transformacion.num_bytes, infoLogger);

						free(contenido_bloque_archivo->contenido_archivo);
						free(contenido_bloque_archivo->nombre);

						comandoT = concatenar(comandoT,chmod);
						comandoT = concatenar(comandoT,nombreTransformador);
						comandoT = concatenar(comandoT,";");
						comandoT = concatenar(comandoT,cat);
						comandoT = concatenar(comandoT,nombreTempBloque);
						comandoT = concatenar(comandoT,pipe);
						comandoT = concatenar(comandoT,"./");
						comandoT = concatenar(comandoT,nombreTransformador);
						comandoT = concatenar(comandoT,pipe);
						comandoT = concatenar(comandoT,sort);
						comandoT = concatenar(comandoT,guardar);
						comandoT = concatenar(comandoT,transformacion.archivo_destino);


						printf("\n#######################################################\n\nEjecutando comando:\n\t%s\n\n#######################################################\n",comandoT);
						if(system(comandoT) == -1){
							fallo = true;
							avisoDeFinalizacion(&fallo,nuevo_fd,infoLogger,"Transformación");
							exit(TRANSFORMACION_TERMINADA_CON_FRACASO);
						}
						free(comandoT);

						fallo = false;
						avisoDeFinalizacion(&fallo,nuevo_fd,infoLogger,"Transformación");
					exit(TRANSFORMACION_TERMINADA_CON_EXITO);

					case COPIAR_REDUCTOR:

						printf("Inicio de recepción del Archivo Reductor de Master.\n");
						log_info(infoLogger,"Inicio de recepción del Archivo Reductor de Master.");

						// Recibiendo archivo del MASTER
						paquete=recibir_payload(&nuevo_fd,&encabezado.tam_payload);
						contenido_archivo=dsrlz_bloque_archivo(paquete.buffer);
						free(paquete.buffer);

						char* nombreReductor = "files/scripts/CopiaR-";
						removeN(contenido_archivo.nombre,6);		//Remuevo el /files (6 char)
						nombreReductor = concatenar(nombreReductor,contenido_archivo.nombre);
						printf("Archivo recibido de Master.\n");
						log_info(infoLogger,"Archivo recibido de Master");

						// Persisto el Archivo en el Worker
						persistirArchivo(nombreReductor,contenido_archivo.contenido_archivo, contenido_archivo.tamanio, infoLogger);

						printf("Archivo persistido en el Worker.\n");
						log_info(infoLogger,"Archivo persistido en el Worker");
						free(contenido_archivo.contenido_archivo);

					break;

					case INICIAR_REDUCCION_ARCHIVO_LOCAL:

						printf("Inicio de recepción del la Reduccion Local de Master.\n");
						log_info(infoLogger,"Inicio de recepción de la Reduccion Local de Master.");

						t_delegacion_reduccion_local reduccionL;
						char* comandoL	=	"";

						paquete = recibir_payload(&nuevo_fd,&encabezado.tam_payload);
						reduccionL = dsrlz_delegacionReduccionLocal(paquete.buffer);
						free(paquete.buffer);

						printf("\nDestino: %s\nTemporales: %s\n\n",reduccionL.archivo_destino,reduccionL.lista_archivo_argumento->archivo_temporal_transformacion);

						comandoL = concatenar(comandoL,chmod);
						comandoL = concatenar(comandoL,nombreReductor);
						comandoL = concatenar(comandoL,";");
						if(reduccionL.lista_archivo_argumento->sig){
							comandoL = concatenar(comandoL,"sort -m ");
							while(reduccionL.lista_archivo_argumento){
								comandoL = concatenar(comandoL,"files");
								comandoL = concatenar(comandoL,reduccionL.lista_archivo_argumento->archivo_temporal_transformacion);
								comandoL = concatenar(comandoL," ");
								reduccionL.lista_archivo_argumento = reduccionL.lista_archivo_argumento->sig;
							}
						} else{
							comandoL = concatenar(comandoL,cat);
							comandoL = concatenar(comandoL,"files");
							comandoL = concatenar(comandoL,reduccionL.lista_archivo_argumento->archivo_temporal_transformacion);
						}
						comandoL = concatenar(comandoL,pipe);
						comandoL = concatenar(comandoL,"./");
						comandoL = concatenar(comandoL,nombreReductor);
						comandoL = concatenar(comandoL,guardar);
						comandoL = concatenar(comandoL,"files");
						comandoL = concatenar(comandoL,reduccionL.archivo_destino);

						printf("\n\n#######################################################\n\nEjecutando comando:\n\t%s\n\n#######################################################\n",comandoL);
						if(system(comandoL) == -1){
							fallo = true;
							avisoDeFinalizacion(&fallo,nuevo_fd,infoLogger,"Reducción Local");
							exit(TRANSFORMACION_TERMINADA_CON_FRACASO);
						}
						free(comandoL);

						fallo = false;
						avisoDeFinalizacion(&fallo,nuevo_fd,infoLogger,"Reducción Local");
					exit(REDUCCION_LOCAL_TERMINADA_CON_EXITO);

					case INICIAR_REDUCCION_ARCHIVO_GLOBAL:	//TODO

						printf("Inicio de recepción de la Reduccion Global de Master.\n");
						log_info(infoLogger,"Inicio de recepción de la Reduccion Global de Master.");
					    pthread_mutex_init(&lockCont,0);
						char* comandoG	=	"";

						t_delegacion_reduccion_global reduccionG;
						fallo = false;

						paquete = recibir_payload(&nuevo_fd,&encabezado.tam_payload);
						reduccionG = dsrlz_delegacionReduccionGlobal(paquete.buffer);
						free(paquete.buffer);


						printf("Los parametros de la reduccion global son:\n");
						printf("reduccionG.archivo_reduccion_global:%s\n", reduccionG.archivo_reduccion_global);
						printf("reduccionG.archivo_temporal_reduccion_local:%s\n", reduccionG.archivo_temporal_reduccion_local);
						printf("reduccionG.lista->nodo:%s\n", reduccionG.lista->nodo);
						printf("reduccionG.lista->worker_ip:%s\n", reduccionG.lista->worker_ip);
						printf("reduccionG.lista->worker_port:%d\n", reduccionG.lista->worker_port);
						printf("reduccionG.lista->archivos_temporales:%s\n", reduccionG.lista->archivos_temporales);

						comandoG = concatenar(comandoG,chmod);
						comandoG = concatenar(comandoG,nombreReductor);
						comandoG = concatenar(comandoG,";");
						comandoG = concatenar(comandoG,"sort -m ");
						comandoG = concatenar(comandoG,"files");
						comandoG = concatenar(comandoG,reduccionG.archivo_temporal_reduccion_local);
						comandoG = concatenar(comandoG," ");
						// Aca quiero consultar la lista y todos sus elementos

						while(reduccionG.lista){
							crearHilo();
							comandoG = concatenar(comandoG,reduccionG.lista->archivos_temporales);
							comandoG = concatenar(comandoG," ");
					    	pthread_mutex_lock(&lockCont);
							pthread_create(&(hilos->hilo),NULL,conectarWorker,(void*)reduccionG.lista);
							reduccionG.lista = reduccionG.lista->sig;
						}
						comandoG = concatenar(comandoG,pipe);
						comandoG = concatenar(comandoG,"./");
						comandoG = concatenar(comandoG,nombreReductor);
						comandoG = concatenar(comandoG,guardar);
						comandoG = concatenar(comandoG,"files");
						comandoG = concatenar(comandoG,reduccionG.archivo_reduccion_global);

						liberarHilos();
					    pthread_mutex_destroy(&lockCont);

						printf("\n\n#######################################################\n\nEjecutando comando:\n\t%s\n\n#######################################################\n",comandoG);
/*						if(system(comandoL) == -1){
							fallo = true;
							avisoDeFinalizacion(&fallo,nuevo_fd,infoLogger,"Reducción Local");
							exit(TRANSFORMACION_TERMINADA_CON_FRACASO);
						}//*/
						//############### Respuesta a master de reduccion global terminada ####################

						fallo = false;
						avisoDeFinalizacion(&fallo,nuevo_fd,infoLogger,"Reducción Global");

					exit(REDUCCION_GLOBAL_TERMINADA_CON_EXITO);

					case ENVIO_ARCHIVO_REDUCCION_GLOBAL:

						printf("Solicitud de almacenamiento final recibida\n");
						log_info(infoLogger,"Solicitud de almacenamiento final recibida.");

						t_delegacion_almacenar_archivo almacenado;

						paquete = recibir_payload(&nuevo_fd,&encabezado.tam_payload);
						almacenado = dsrlz_delegacionAlmacenarArchivo(paquete.buffer);
						free(paquete.buffer);

						printf("\n\n%s\n\n",almacenado.archivo);
						almacenado.archivo = concatenar("files",almacenado.archivo);
						printf("\n\n%s\n\n",almacenado.archivo);
						log_info(infoLogger,"Iniciar envio del archivo %s para la Reduccion Global para el Worker encargado.",almacenado.archivo);
						if(copiarArchivo_a_Worker("files/nombres-pequeno.csv", ALMACENAR_ARCHIVO, nuevo_fd, infoLogger)){
				        log_info(infoLogger, "Archivo %s enviado al Worker encargado",almacenado.archivo);
				        }

					break;

					case ALMACENADO_FINAL:
						fallo = false;
						avisoDeFinalizacion(&fallo,nuevo_fd,infoLogger,"Almacenado final");
					break;

				}
			}
		}
		close(nuevo_fd); // El proceso padre no lo necesita
	}

    /* ---------------------------------------- */
    /*  Libero Memoria de Log y Config          */
    /* ---------------------------------------- */
    log_destroy(infoLogger);
    config_destroy(cfg);

    close(sockfd);


    return 0;
}

void sigchld_handler(int s)
 {
 while(wait(NULL) > 0);
 }

void avisoDeFinalizacion(bool* fallo, int nuevo_fd,t_log* infoLogger,char* etapa){
	//############### Respuesta a master de transformacion terminada ####################

	void* envioRespuestaTransformacion = malloc(sizeof(bool));
	memcpy(envioRespuestaTransformacion,fallo,sizeof(bool));
	send(nuevo_fd,envioRespuestaTransformacion,sizeof(bool),0);
	free(envioRespuestaTransformacion);

	if(*fallo){
		printf("Envio de resultado de la %s a Master.\n%s finalizada de manera incorrecta\n",etapa,etapa);
		log_info(infoLogger,"Envio de resultado de la %s a Master.\n%s finalizada de manera incorrecta",etapa,etapa);
	} else {
		printf("Envio de resultado de la %s a Master.\n%s finalizada de manera correcta\n",etapa,etapa);
		log_info(infoLogger,"Envio de resultado de la %s a Master.\n%s finalizada de manera correcta",etapa,etapa);
	}
}

void crearHilo(){
	pthread_t h;

	if(hilos == NULL){	//Comparo si es el primer hilo
		hilos = malloc(sizeof(struct NodoHilo));
		hilos->hilo = h;
		hilos->sig = NULL;
	}else{
		auxH = malloc(sizeof(struct NodoHilo));
		auxH->hilo = h;
		auxH->sig = hilos;
		hilos = auxH;
	}
}

void liberarHilos(){

    while (hilos) {
		pthread_join(hilos->hilo,NULL);	// Espero a que finalicen todos los hilos para continuar
		hilos = hilos->sig;
		free(auxH);
		auxH = hilos;
	}

}

void* conectarWorker(void* w){
	lista_delegacion_reduccion_global* workerC = (lista_delegacion_reduccion_global*) w;
	lista_delegacion_reduccion_global worker;

	worker.archivos_temporales	=	malloc(strlen(workerC->archivos_temporales)	+1);
	strcpy(worker.archivos_temporales	,workerC->archivos_temporales	);
	worker.nodo					=	malloc(strlen(workerC->nodo)				+1);
	strcpy(worker.nodo					,workerC->nodo					);
	worker.worker_ip			=	malloc(strlen(workerC->worker_ip)			+1);
	strcpy(worker.worker_ip				,workerC->worker_ip				);
	worker.worker_port			=	workerC->worker_port;

	pthread_mutex_unlock(&lockCont);

	log_info(infoLogger, "Iniciando conexion con Worker");
	printf("\nIniciando conexion con el Worker del %s\n",worker.nodo);

	printf("IP: %s\nport: %d\n",worker.worker_ip,worker.worker_port);
	if((nuevo_fd = conectarseAservidor(worker.worker_ip,worker.worker_port)) == -1)	{
		fallo = true;
	}else{

		t_delegacion_almacenar_archivo* archivo	=	malloc(sizeof(t_delegacion_almacenar_archivo));
		archivo->archivo = worker.archivos_temporales;
		paquete = srlz_delegacionAlmacenarArchivo(archivo);
		send(nuevo_fd,paquete.buffer,paquete.tam_buffer,0);
		free(paquete.buffer);

		// Recibiendo archivo del Worker
		paquete=recibir_payload(&nuevo_fd,&encabezado.tam_payload);
		contenido_archivo=dsrlz_bloque_archivo(paquete.buffer);
		free(paquete.buffer);

		// Persisto el Archivo en el Worker ecargado
		persistirArchivo(worker.archivos_temporales,contenido_archivo.contenido_archivo, contenido_archivo.tamanio, infoLogger);

		printf("Archivo persistido en el Worker.\n");
		log_info(infoLogger,"Archivo persistido en el Worker");
		free(contenido_archivo.contenido_archivo);
	}

	return 0;
}
