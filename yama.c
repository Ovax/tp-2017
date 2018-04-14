#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <commons/config.h>
#include <commons/log.h>
#include "registros.h"
#include "sockets.h"
#include "funciones.h"
#include <commons/collections/list.h>

/* ---------------------------------------- */
/*  Variables Globales                      */
/* ---------------------------------------- */

t_log* infoLogger;
t_log* traceLogger;
t_log* errorLogger;
t_config* cfg;

// Datos globales del archivo de Configuracion para planificacion
int base;
char* algoritmo_balaceo;
int retardoPlanificacion = 0;
static volatile int atenderSignal = 0;

/* ---------------------------------------- */

void recargarConfig();
void sigHandler_recargarConfig(int signal);

int main(int argc, char *argv[]) {

	/* Creo la instancia del Archivo de Configuracion */
	cfg = config_create("config/config.cfg");
	/* Creo la instancia de Log */
	infoLogger = log_create("log/yama.log", "Yama", false, LOG_LEVEL_INFO);

	// Logueando
	log_info(infoLogger, "Iniciando YAMA" );
  	printf("Iniciando YAMA\n");


  	// Capturo la sign SIGUSR1
	struct sigaction sa;
	sa.sa_handler = sigHandler_recargarConfig;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_RESTART;
	if (sigaction(SIGUSR1, &sa, NULL) == -1){
		perror("sigaction");
		return 1;
	}

	int sockfd,escucha_master,fd_maximo,nuevo_fd,i,size;
	int cantbloque=0;
	fd_set master,temporales;
	FD_ZERO(&master);
	FD_ZERO(&temporales);
	t_list* listaEstados;
	t_list* listanodos;
	t_list* listamaster;
	listaEstados=list_create();
	listanodos=list_create();
	listamaster=list_create();
	t_list* listacopiaReplanificacion;
	listacopiaReplanificacion=list_create();
	int cant=0;
	int job=1;
	t_solicitud_reduccion_local solicitud_l;
	t_solicitud_reduccion_global solicitud_RG;
	t_solicitud_almacenado_final solicitud_af;
	t_solicitud_transformacion solicitud_transformacion;
	notificacionArchivo notificacion;
	worker_algoritmo* punteroclock;
	punteroclock=malloc(sizeof(worker_algoritmo));
	datomaster* masterbuscado;
	masterbuscado=malloc(sizeof(datomaster));
	Paquete paquete;
	Encabezado encabezado;


	//t_bloques_archivos bloque;



	worker_algoritmo* listaalgoritmo;
	listaalgoritmo=malloc(sizeof(worker_algoritmo));
	listaalgoritmo=NULL;

	char* nodoseleccionado=string_new();

	// seguir la pista del descriptor de fichero mayor
	struct sockaddr_in servidor_addr,my_addr,master_addr; // información de la dirección de destino

	// Creo conexión con FS
	if((sockfd= conectarseAservidor(config_get_string_value(cfg,"FS_IP"),config_get_int_value(cfg,"FS_PUERTO")))==-1){
		perror("Falló la conexión con FS");
		return 1;
	}
	log_info(infoLogger, "Conectado con FS" );
	printf("Conectado con FS\n");

	//---------------------------INFO DE LOS NODOS
		paquete = crearHeader('Y',INFO_NODO , 2);
		if(send(sockfd,paquete.buffer,paquete.tam_buffer,0)==-1){
			perror("fallo envio cantidad de bloques del archivo");
		}

		free(paquete.buffer);
		encabezado=recibir_header(&sockfd);
		cantbloque=encabezado.tam_payload;

		// Si no hay al menos 2 nodos conectados en FS, no me puedo conectar y salgo
		if(cantbloque < 2){
			log_info(infoLogger, "Desconectado con FS por no estar estable." );
			printf("Desconectado con FS por no estar estable.\n");
			return 0;
		}

		t_nodos nodoexistente;
		for( cant=0;cant<cantbloque;cant++){

			encabezado=recibir_header(&sockfd);
			paquete=recibir_payload(&sockfd,&encabezado.tam_payload);

			nodoexistente=dsrlz_datosNodo(paquete.buffer);
			agregardatosnodo(&listaalgoritmo,&nodoexistente,listanodos);

			free(paquete.buffer);
			free(nodoexistente.nodo);
			free(nodoexistente.ip);
		}


	//----------------

	//datos para planificacion

	base=config_get_int_value(cfg,"DISP_BASE");
	algoritmo_balaceo=config_get_string_value(cfg,"ALGORITMO_BALANCEO");
	retardoPlanificacion=config_get_int_value(cfg,"RETARDO_PLANIFICACION");
	printf("Algoritmo utilizado:%s - Retardo Planificación: %d - Base Planificación: %d\n",algoritmo_balaceo, retardoPlanificacion, base);

	//-----------------------------------

	// Creo el Servidor para escuchar conexiones
	escucha_master=crearServidor(config_get_int_value(cfg,"YAMA_PUERTO"));
	log_info(infoLogger, "Escuchando conexiones" );
	printf("Escuchando conexiones\n");
	FD_SET(sockfd,&master);
	FD_SET(escucha_master, &master);
	// seguir la pista del descriptor de fichero mayor
	fd_maximo = escucha_master;
	Solicitud_Master solicitud_master;
	t_list* listaaux;
	listaaux=list_create();
	t_estados* aux;
	aux=malloc(sizeof(t_estados));
	aux=NULL;

	dato_nodo* nodo;
	nodo=malloc(sizeof(dato_nodo));
	nodo=NULL;

	nodo_bloque bloquenodo;
	t_bloques_archivos solicitud_b ;


	int retval;

	//----------------

	while(1){

		//se comunica el master
		temporales=master;

		retval = select(fd_maximo+2,&temporales,NULL,NULL,NULL);

		if(retval < 0 ){

			// Si se interrumpio por una signal
			if (errno == EINTR){
				recargarConfig();
			}else{
				perror("select");
				exit (1);				
			}
		}


		for(i = 0; i <= fd_maximo; i++) {
			if (FD_ISSET(i, &temporales)) { // ¡¡tenemos datos!!
				if (i == escucha_master) {
					// gestionar nuevas conexiones
					size = sizeof(master_addr);
					if ((nuevo_fd = accept(escucha_master, (struct sockaddr *)&master_addr,
							&size)) == -1) {
						perror("accept");
					} else {
						FD_SET(nuevo_fd, &master); // añadir al conjunto maestro

						// Recibo de Master el ID que tiene
						encabezado=recibir_header(&nuevo_fd);

						agregarmasterAlaLista(listamaster,encabezado.tam_payload,nuevo_fd);
						printf("Master %i conectado.\n",encabezado.tam_payload);
						log_info(infoLogger, "Master %i conectado.",encabezado.tam_payload );

						if (nuevo_fd > fd_maximo) {    // actualizar el máximo
							fd_maximo = nuevo_fd;
						}
					}

				} else {

					encabezado=recibir_header(&i);

					switch(encabezado.cod_operacion){

						case INICIAR_REDUCCION_ARCHIVO_GLOBAL:
							paquete=recibir_payload(&i,&encabezado.tam_payload);
							notificacion=dsrlz_notificacionArchivo(paquete.buffer);
							//log_info(infoLogger,"Iniciando la reduccion global.");
							log_info(infoLogger,"Se recibio la notificacion del archivo R.local:%s.",notificacion.archivo);
							printf("se recibio la notificacin del archivo R.local:%s\n",notificacion.archivo);
							free(paquete.buffer);
							
							// Actualizacion de la Tabla de Estados
							actualizarListaEstado(&notificacion,listaEstados);
							masterbuscado=buscarIdMaster(i,listamaster);
							if(notificacion.fallo){

								abortarjob(masterbuscado,listamaster,listaEstados);
								close(i);
								FD_CLR(i,&master);
							}


							if (listoParaRG(listaEstados,&notificacion) && !notificacion.fallo){
								log_info(infoLogger,"Listo para la Reduccion Global.");
								printf("Listo para la Reduccion gloval\n");
								nodoseleccionado=elegirnodoencargado(listaalgoritmo);
								log_info(infoLogger,"El nodo encargado de la reduccion global es :%s.",nodoseleccionado);
								printf("El nodo encargado de la reduccion global es :%s\n",nodoseleccionado);


								listaaux=obtenerArchivosreduccionglobal(listaEstados,&notificacion);

								// Le envio a Master la Cant de Solicitudes de Reduccion
								 cant=list_size(listaaux);

								paquete = crearHeader('Y', 2,cant);

								if(send(i,paquete.buffer,paquete.tam_buffer,0)==-1){
									perror("fallo envio cantidad para reduccion global\n");
								}


							  log_info(infoLogger,"Se envió a Master la cantidad de Solicitudes de Reduccion. Total: %d.", list_size(listaaux));
							  printf("Se envió a Master la cantidad de Solicitudes de Reduccion. Total: %d.\n", list_size(listaaux));

								free(paquete.buffer);
								for( cant=0;cant<list_size(listaaux);cant++){

									solicitud_RG=crearSolicitudGlobal(list_get(listaaux,cant), listanodos,nodoseleccionado,masterbuscado);

									printf("nombre de la RG final:%s\n",solicitud_RG.archivo_reduccion_global);
									paquete=srlz_solicitudReduccionGlobal(&solicitud_RG);
									if(send(i,paquete.buffer,paquete.tam_buffer,0)==-1){
										perror("fallo envio RG\n");
									}

									//free(masterbuscado->nombrearchivoRGfinal);
									//free(paquete.buffer);
									usleep(retardoPlanificacion);
									/*free(solicitud_RG.archivo_tmp_reduccion_local);
									free(solicitud_RG.nodo);
									free(solicitud_RG.worker_ip);
									free(solicitud_RG.archivo_reduccion_global);*/

					                log_info(infoLogger,"Se envió a Master la Solicitudes de Reduccion Global. Nombre Tmp Reduccion Local: %s.", solicitud_RG.archivo_tmp_reduccion_local);
					                printf("Se envió a Master la Solicitudes de Reduccion Global. Nombre Tmp Reduccion Local: %s.\n", solicitud_RG.archivo_tmp_reduccion_local);
								}
								agregarEstadoRGALista(listaEstados,&solicitud_RG,"almacenado_final",masterbuscado,nodoseleccionado);
								free(solicitud_RG.archivo_tmp_reduccion_local);
								free(solicitud_RG.nodo);
								free(solicitud_RG.worker_ip);
								free(solicitud_RG.archivo_reduccion_global);
								free(paquete.buffer);
								free(nodoseleccionado);
							}


							free(notificacion.archivo);
							//free(masterbuscado->nombrearchivoRGfinal);
							break;

						case INICIAR_REDUCCION_ARCHIVO_LOCAL:
							paquete=recibir_payload(&i,&encabezado.tam_payload);
							notificacion=dsrlz_notificacionArchivo(paquete.buffer);
							free(paquete.buffer);

							log_info(infoLogger,"Se recibió de Master la notificación del archivo:%s.",notificacion.archivo);
							printf("Se recibió de Master la notificación del archivo:%s\n",notificacion.archivo);
							
							// Actualizacion de la Tabla de Estados
							actualizarListaEstado(&notificacion,listaEstados);
							masterbuscado=buscarIdMaster(i,listamaster);

							//replanificacion
							if(notificacion.fallo && existecopia(listacopiaReplanificacion,&notificacion)){

								log_info(infoLogger,"Inicio de Replanificación.");
								printf("Inicio de Replanificación\n");
								usleep(retardoPlanificacion);
								solicitud_b=replanificacion(listacopiaReplanificacion,&notificacion,&punteroclock,listaalgoritmo,base,masterbuscado->job);

								bloquenodo=algoritmoclock(&solicitud_b,listaalgoritmo,&punteroclock,base);

								solicitud_transformacion=crearsolicitudTransformacion(&solicitud_b,&bloquenodo,masterbuscado->job);

								usleep(retardoPlanificacion);
								printf("solicitud nueva de replanificacion:%s\n",solicitud_transformacion.archivo_tmp);
								printf("nodo :%s\n",solicitud_transformacion.nodo);
								printf("nodo port:%i\n",solicitud_transformacion.worker_port);
								paquete=srlz_solicitudTransformacion(&solicitud_transformacion);
								if(send(i,paquete.buffer,paquete.tam_buffer,0)==-1){
									perror("fallo de envio solicitud transformacion");
								}
								free(paquete.buffer);
								agregarEstadoALista(listaEstados,masterbuscado->job,&solicitud_transformacion,masterbuscado->master,"transformacion");

								log_info(infoLogger,"Se envió a Master una solicitud de Transformación por Replanificación.");
								printf("Se envió a Master una solicitud de Transformación por Replanificación\n");
								usleep(retardoPlanificacion);
								/*free(solicitud_transformacion.archivo_tmp);
								free(solicitud_transformacion.nodo);
								free(solicitud_transformacion.worker_ip);
								free(solicitud_b.copia1.ip);
								free(solicitud_b.copia1.nodo);
								free(solicitud_b.copia2.ip);
								free(solicitud_b.copia2.nodo);*/

							}else if(notificacion.fallo && !existecopia(listacopiaReplanificacion,&notificacion)){

								log_info(infoLogger,"Replanificación cancelada por no existir copia. Job abortado.");
								printf("Replanificación cancelada por no existir copia. Job abortado\n");

								abortarjob(masterbuscado,listamaster,listaEstados);
								close(i);
								FD_CLR(i,&master);

							}
							//-----------------------------------------

							if(listoparaReduccion(listaEstados,&notificacion) && !notificacion.fallo){


								listaaux=nodoArchivoListo(&notificacion,listaEstados);


								solicitud_l=crearSolicitudlocal(listaaux,listanodos);

								log_info(infoLogger,"Solicitud de Reducción Local creada con el nombre:%s.",solicitud_l.archivo_tmp_reduccion_local);
								printf("Solicitud de Reducción Local creada con el nombre:%s.\n",solicitud_l.archivo_tmp_reduccion_local);

								printf("datos de la solicitud de reduccion local\n");
								printf("port del worker:%i\n",solicitud_l.worker_port);
								printf("nodo:%s\n",solicitud_l.nodo);
								printf("ip del worker:%s\n",solicitud_l.worker_ip);
								//printf("nombre del archivo de reduccion local:%s\n",solicitud_l.archivo_tmp_reduccion_local);
								//printf("nombre del archivo transformacion:%s\n",solicitud_l.archivo_tmp_transformacion->archivo_temporal_transformacion);

								printf("preparando la serializacion\n");
								paquete=srlz_solicitudReduccionLocal(&solicitud_l);


								if(send(i,paquete.buffer,paquete.tam_buffer,0)==-1){
									perror("fallo envio solicitud local");
														}
								log_info(infoLogger,"se envio la solicitud local con nombre:%s.",solicitud_l.archivo_tmp_reduccion_local);
								printf("se envio la solicitud %s correctamente\n",solicitud_l.archivo_tmp_reduccion_local);
								//usleep(retardoPlanificacion);
								//masterbuscado=buscarIdMaster(i,listamaster);
								agregarEstadoRLALista(listaEstados,&solicitud_l,"reduccionLocal",masterbuscado);
								//free(notificacion.archivo);
								/*free(solicitud_l.archivo_tmp_reduccion_local);
								free(solicitud_l.nodo);
								free(solicitud_l.worker_ip);
								free(paquete.buffer);*/



							}

							/*free(solicitud_l.archivo_tmp_reduccion_local);
							free(solicitud_l.nodo);
							free(solicitud_l.worker_ip);
							free(paquete.buffer);*/
							//free(masterbuscado->nombrearchivoRGfinal);
							free(notificacion.archivo);
							//free(aux);


							break;

						case ALMACENADO_FINAL:
							paquete=recibir_payload(&i,&encabezado.tam_payload);
							notificacion=dsrlz_notificacionArchivo(paquete.buffer);
							//log_info(infoLogger,"Iniciando la reduccion global.");
							log_info(infoLogger,"Se recibio la notificacion del archivo final global:%s.",notificacion.archivo);
							printf("se recibio la notificacin del archivo final global:%s\n",notificacion.archivo);
							free(paquete.buffer);

							// Actualizacion de la Tabla de Estados
							actualizarListaEstado(&notificacion,listaEstados);
							masterbuscado=buscarIdMaster(i,listamaster);
							if(!notificacion.fallo){


								log_info(infoLogger,"termino correctamente el job %i en el master %i con archivo final %s\n",masterbuscado->job,masterbuscado->master,notificacion.archivo);
								printf("termino correctamente el job %i en el master %i con archivo final %s\n",masterbuscado->job,masterbuscado->master,notificacion.archivo);
								close(masterbuscado->socketmaster);
								FD_CLR(i,&master);


							}else{
								abortarjob(masterbuscado,listamaster,listaEstados);
								log_info(infoLogger,"Se aborto el job %i del master %i",masterbuscado->job,masterbuscado->master);
								printf("----------se aborto el job %i del master %i---------------\n",masterbuscado->job,masterbuscado->master);
								close(masterbuscado->socketmaster);
								FD_CLR(i,&master);

							}
							free(notificacion.archivo);

							//t_solicitud_almacenado_final solicitud_af;
							//inicializarEjemploAlmacenadoFinal(&solicitud_af);
							//paquete=srlz_solicitudAlmacenadoFinal(&solicitud_af);
							//send(i,paquete.buffer,paquete.tam_buffer,0);
							break;


						case INICIAR_TRANSFORMACION_ARCHIVO:

							log_info(infoLogger,"Iniciando Transformación...");
							printf("Iniciando Transformación...\n");

							paquete=recibir_payload(&i,&encabezado.tam_payload);
							solicitud_master=dsrlz_envio_transformacion(paquete.buffer);

							log_info(infoLogger,"El archivo %s fue recibido de Master.", solicitud_master.archivo);
							printf("El archivo %s fue recibido de Master.\n", solicitud_master.archivo);
							free(paquete.buffer);

	            			// Envio el nombre del archivo a FS
							paquete=srlz_envio_transformacion(&solicitud_master,'Y',INICIAR_TRANSFORMACION_ARCHIVO);

							log_info(infoLogger,"Envio a FS el nombre del archivo para iniciar Transformación.");
							printf("Envio a FS el nombre del archivo para iniciar Transformación.\n");

							if(send(sockfd,paquete.buffer,paquete.tam_buffer,0)==-1){
								perror("Fallo el envio del nombre del archivo a FS");
							}

							free(paquete.buffer);

	            			// Recibo de FS la cantidad de bloques del archivo consultado
							encabezado=recibir_header(&sockfd);
							cantbloque=encabezado.tam_payload;
							log_info(infoLogger,"Recepcion de FS de la cantidad de Bloques del archivo %s. Total Bloques: %d.", solicitud_master.archivo, cantbloque);
							printf("Recepcion de FS de la cantidad de Bloques del archivo %s. Total Bloques: %d.\n", solicitud_master.archivo, cantbloque);
							free(solicitud_master.archivo);

							// Envio a Master la cantidad total de bloques del archivo
							paquete = crearHeader('Y', 20, cantbloque);
							if(send(i,paquete.buffer,paquete.tam_buffer,0)==-1){
								perror("fallo envio cantidad de bloques del archivo");
							}
							free(paquete.buffer);

							log_info(infoLogger,"Se envió a Master la cantidad de bloques del archivo. Total bloques: %d.", cantbloque);
							printf("Se envió a Master la cantidad de bloques del archivo. Total bloques: %d.\n", cantbloque);

							// Si existen bloques para el archivo indicado
							if(cantbloque != 0){

								//inicio algoritmo
								punteroclock=aplicardisponibilidad(algoritmo_balaceo,listaalgoritmo,base);

								// Obtengo el ID del Master
								masterbuscado=buscarIdMaster(i,listamaster);

								agregarjobListaMaster(job,listamaster,i);

								// Recibo Todos los Bloques del Archivo del FS y armo la solicitud
								for( cant=0;cant<cantbloque;cant++){

									// Recibo de FS cada bloque del archivo
									encabezado=recibir_header(&sockfd);
									paquete=recibir_payload(&sockfd,&encabezado.tam_payload);

									log_info(infoLogger,"Recibiendo el Bloque %d de FS.", cant);
									printf("Recibiendo el Bloque %d de FS.\n", cant);

									// Deserializo el paquete de los bloques
									solicitud_b = dsrlz_bloquenodo(paquete.buffer);

									free(paquete.buffer);

									// Selecciono el Nodo menos ocupado
									//solicitud_transformacion=algoritmo(listaEstados,&solicitud_b,job);
									bloquenodo=algoritmoclock(&solicitud_b,listaalgoritmo,&punteroclock,base);

									solicitud_transformacion=crearsolicitudTransformacion(&solicitud_b,&bloquenodo,job);
									agregardatosparalaReplanificacion(solicitud_transformacion,&solicitud_b,listacopiaReplanificacion);


									//free(solicitud_b.copia1.ip);
									//free(solicitud_b.copia1.nodo);
									//free(solicitud_b.copia2.ip);
									//free(solicitud_b.copia2.nodo);

									//agrego datos del nodo a la lista de nodos
									agregardatosNodo(&solicitud_transformacion,listanodos);

									// Actualizo la Lista de Estado
									agregarEstadoALista(listaEstados,job,&solicitud_transformacion,masterbuscado->master,"transformacion");

									// Le mando a Master los Bloques
									paquete=srlz_solicitudTransformacion(&solicitud_transformacion);
									if(send(i,paquete.buffer,paquete.tam_buffer,0)==-1){
										perror("fallo de envio solicitud transformacion");
									}
									log_info(infoLogger,"Se envio a Master una solicitud de transformación.");
									printf("Se envio a Master una solicitud de transformación\n.");
									free(paquete.buffer);
									/*free(solicitud_transformacion.archivo_tmp);
									free(solicitud_transformacion.nodo);
									free(solicitud_transformacion.worker_ip);*/

									usleep(retardoPlanificacion);
								}

								free(solicitud_transformacion.archivo_tmp);
								free(solicitud_transformacion.nodo);
								free(solicitud_transformacion.worker_ip);
								/*free(solicitud_b.copia1.ip);
								free(solicitud_b.copia1.nodo);
								free(solicitud_b.copia2.ip);
								free(solicitud_b.copia2.nodo);*/
								job++;
							}

							break;

						case NOMBRE_ARCHIVO_FINAL_RG:
							paquete=recibir_payload(&i,&encabezado.tam_payload);
							solicitud_master=dsrlz_envio_transformacion(paquete.buffer);

							log_info(infoLogger,"Recepcion del nombre del archivo final del archivo global:%s", solicitud_master.archivo);
							printf("Recepcion del nombre del archivo final del archivo globalr:%s\n", solicitud_master.archivo);
							masterbuscado=buscarIdMaster(i,listamaster);
							actualizarListaMaster(solicitud_master.archivo,masterbuscado->master,listamaster);
							free(paquete.buffer);
							free(solicitud_master.archivo);
							//free(masterbuscado->nombrearchivoRGfinal);
							break;
					}
				}
			}
		}
	}


	/* ---------------------------------------- */
	/*  Libero Memoria de Log y Config          */
	/* ---------------------------------------- */
	free(aux);

	log_destroy(infoLogger);
	config_destroy(cfg);
	list_destroy(listaEstados);
	list_destroy(listanodos);
	list_destroy(listaaux);
	list_destroy(listamaster);
	list_destroy(listacopiaReplanificacion);


	close(sockfd);

	return 0;
}

void recargarConfig()
{

	if (atenderSignal) {
		atenderSignal = 0;
		log_info(infoLogger, "Señal SIGUSR1 recibida. Recargando la configuración.");
		printf("Señal SIGUSR1 recibida. Recargando la configuración.\n");

		base=config_get_int_value(cfg,"DISP_BASE");
		algoritmo_balaceo=config_get_string_value(cfg,"ALGORITMO_BALANCEO");
		retardoPlanificacion=config_get_int_value(cfg,"RETARDO_PLANIFICACION");

		printf("Algoritmo utilizado:%s - Retardo Planificación: %d - Base Planificación: %d\n",algoritmo_balaceo, retardoPlanificacion, base);
		log_info(infoLogger, "Señal SIGUSR1 recibida. Configuración actualizada.");
		log_info(infoLogger, "Algoritmo utilizado:%s - Retardo Planificación: %d - Base Planificación: %d",algoritmo_balaceo, retardoPlanificacion, base);
	}

}

void sigHandler_recargarConfig(int signal)
{
	if(signal == SIGUSR1)
	{
		atenderSignal = 1;
	}
}
