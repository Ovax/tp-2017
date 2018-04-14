#include <commons/config.h>
#include <commons/log.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "funciones.h"
#include "registros.h"
#include "sockets.h"


/* ---------------------------------------- */
/*  Variables Globales                      */
/* ---------------------------------------- */

int yama_fd,contT = 0,contRL = 0;

t_log* infoLogger;
t_log* traceLogger;
t_log* errorLogger;
t_config* cfg;

// variables YAMA

bool exitoTransformacion = true;

// Lista de Hilos

struct NodoHilo{
	pthread_t hilo;
	struct NodoHilo* sig;
};

///////////////HILOS///////////////

struct NodoHilo* hilos = NULL, *auxH;

//	Semáforos

pthread_mutex_t lockCont;

/* ---------------------------------------- */

//			Head
/* ---------------------------------------- */

void crearHilo();
void liberarHilos();
void* hiloDeTransformacion(void * solicitudTransformacion);
void* hiloDeRedLocal(void* solicitudReduccionLocal);
/* ---------------------------------------- */

/* ---------------------------------------- */
/*  Esructuras Locales                      */
/* ---------------------------------------- */

typedef struct{
	t_solicitud_transformacion solicitudTransformacion;
	char* transformador;
}Transformacion;

typedef struct{
	t_solicitud_reduccion_local solicitudRedLocal;
	char* reductor;
}Reduccion_Local;

/* ---------------------------------------- */////////////////////////////////////////////////////////////////////////////////


int main(int argc, char *argv[]) {

    /* Creo la instancia del Archivo de Configuracion */
    cfg = config_create("config/config.cfg");
    /* Creo la instancia de Log */
    infoLogger = log_create("log/master.log", "Master", false, LOG_LEVEL_INFO);
    // Logueando
    log_info(infoLogger, "Iniciando MASTER" );
    printf("########################################\n\t Inicinado Master\n########################################\n");

	char* transformador =	malloc(strlen(argv[1])+1);		strcpy(transformador,	argv[1]);
	char* reductor =		malloc(strlen(argv[2])+1);		strcpy(reductor,		argv[2]);
	char* archivo =			malloc(strlen(argv[3])+1);		strcpy(archivo,			argv[3]);
	char* destino =			malloc(strlen(argv[4])+1);		strcpy(destino,			argv[4]);

	//		Cequeo de que existan los archivos

	if(!(existeArchivo(transformador))){
        log_info(infoLogger, "Archivo Transformador no existe");
        printf("Archivo Transformador no existe.\n");
        return 0;}

	else if(!existeArchivo(reductor)){
		log_info(infoLogger, "Archivo Reductor no existe");
		printf("Archivo Reductor no existe.\n");
		return 0;}

	printf("\nTransformador:	%s\nReductor:	%s\nArchivo:	%s\nDestino:	%s\n\n", transformador, reductor, archivo, destino);


	clock_t t_ini, t_fin;
	double tTrans,tRedL,tRedG;

    // Inicializacion de los FD
    struct sockaddr_in servidor_addr,my_addr,master_addr; // información de la dirección de destino    
    int fd_maximo,nuevo_fd,i,size;
    fd_set master,temporales;
    FD_ZERO(&master);
    FD_ZERO(&temporales);

    // Creo la conexión con YAMA
    yama_fd = conectarseAservidor(config_get_string_value(cfg,"YAMA_IP"),config_get_int_value(cfg,"YAMA_PUERTO"));
    if(yama_fd == -1)
    	return 0;
    log_info(infoLogger, "Conectado con YAMA" );


	Paquete paquete1 = crearHeader('M', 19, config_get_int_value(cfg,"ID_MASTER"));
	send(yama_fd,paquete1.buffer,paquete1.tam_buffer,0);
	free(paquete1.buffer);

//****************************** Envío de solicitud a YAMA ************************************//


//Preparo solicitud para enviar
	Solicitud_Master solicitud,archivoRGfinal;
	solicitud.archivo = malloc(strlen(archivo)+1);
	strcpy(solicitud.archivo, archivo);

	archivoRGfinal.archivo = malloc(strlen(destino)+1);
	strcpy(archivoRGfinal.archivo, destino);
    log_info(infoLogger, "Creando solicitud para envir a YAMA" );
    log_info(infoLogger, "Datos de la Solicitud: Transformador: %s - Reductor: %s - Archivo: %s - Destino: %s", transformador, reductor, archivo, destino);

	Encabezado encabezado;
	Paquete paquete;
    int cantbloque, cant, cantSolicitudesRG;

    pthread_mutex_init(&lockCont,0);

	paquete = srlz_envio_transformacion(&solicitud,'M',	INICIAR_TRANSFORMACION_ARCHIVO);
	send(yama_fd,paquete.buffer,paquete.tam_buffer,0); // Envío la solicitud

	free(solicitud.archivo);
	free(paquete.buffer);
//envio a yama el nombre del archivo a utilizar en la reduccion global
	paquete = srlz_envio_transformacion(&archivoRGfinal,'M',NOMBRE_ARCHIVO_FINAL_RG);
	send(yama_fd,paquete.buffer,paquete.tam_buffer,0); // Envío la solicitud

	free(archivoRGfinal.archivo);
	free(paquete.buffer);


/////////////// Iniciando Rcepción

    log_info(infoLogger,"Inicio del Pedido de Tranformación del archivo %s.", archivo);
    printf("Inicio del Pedido de Tranformación del archivo %s.\n", archivo);

    // Recibo de Yama la cantidad de bloques de la solicitud
	encabezado = recibir_header(&yama_fd);
    cantbloque = encabezado.tam_payload;

    log_info(infoLogger,"Pedido de %d bloques a Tranformar", cantbloque);
    printf("Pedido de %i bloques a Tranformar \n", cantbloque);

    // Si se recibieron 0 bloques, entonces el archivo no existe en FS
    if(cantbloque == 0){
        log_info(infoLogger, "El archivo %s no existe en YamaFS", archivo);
        printf("El archivo %s no existe en YamaFS.\n", archivo);
        return 0;
    }


/////////// Recibo las Solicitudes de Yama ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    encabezado = recibir_header(&yama_fd);

///////////////////// Comienzo de la etapa de Transformación//////////////////////////////////////////////////////////////////////////
    if(encabezado.cod_operacion == INICIAR_TRANSFORMACION_ARCHIVO){
    log_info(infoLogger, "Empezando etapa de Transformación");
    printf("########################################\nEmpezando etapa de Transformación\n########################################\n");}

    while(encabezado.cod_operacion == INICIAR_TRANSFORMACION_ARCHIVO || encabezado.cod_operacion == REPLANIFICACION){

	  t_ini = clock();

    while(encabezado.cod_operacion == INICIAR_TRANSFORMACION_ARCHIVO || encabezado.cod_operacion == REPLANIFICACION){// TODO
        if(encabezado.cod_operacion == REPLANIFICACION){
        log_info(infoLogger, "Realizando replanificacion");
        printf("########################################\nRealizando replanificacion\n########################################\n");}
        Transformacion transformacion;	transformacion.transformador = transformador;

    	crearHilo();

    	pthread_mutex_lock(&lockCont);
    	paquete = recibir_payload(&yama_fd,&encabezado.tam_payload);

        transformacion.solicitudTransformacion = dsrlz_solicitudTransformacion(paquete.buffer);
    	free(paquete.buffer);

        log_info(infoLogger,"Detalle de la Solicitud: Nodo:%s - Bloque:%d - Archivo_tmp:%s", transformacion.solicitudTransformacion.nodo,transformacion.solicitudTransformacion.bloque,transformacion.solicitudTransformacion.archivo_tmp);
        printf("Detalle de la Solicitud: Nodo:%s - Bloque:%d - Archivo_tmp:%s\n", transformacion.solicitudTransformacion.nodo,transformacion.solicitudTransformacion.bloque,transformacion.solicitudTransformacion.archivo_tmp);
printf("puerto del nodo:%i\n",transformacion.solicitudTransformacion.worker_port);
printf("ip del nodo:%s\n",transformacion.solicitudTransformacion.worker_ip);
    	pthread_create(&(hilos->hilo),NULL,hiloDeTransformacion,(void*)&transformacion); // Realizo conexión con worker a traves de Hilos

    	//pthread_join(hilos->hilo,NULL);
        encabezado = recibir_header(&yama_fd);	// Espero mas datos para próximas instrucciones
    }

    log_info(infoLogger,"Iniciando espera y liberación de Hilos");
    liberarHilos();
    log_info(infoLogger,"Espera y liberación de Hilos finalizada");

	  t_fin = clock();

	  tTrans = (double)(t_fin - t_ini) / CLOCKS_PER_SEC;
	  printf("%.16g milisegundos\n", tTrans * 1000.0);


///////////////////// Comienzo de la etapa de Reducción Local//////////////////////////////////////////////////////*/
	  if(encabezado.cod_operacion == INICIAR_REDUCCION_ARCHIVO_LOCAL){
    log_info(infoLogger, "Empezando etapa de Reducción Local");
    printf("########################################\nEmpezando etapa de Reducción Local\n########################################\n");}

    t_ini = clock();

    while(encabezado.cod_operacion == INICIAR_REDUCCION_ARCHIVO_LOCAL){// TODO

        Reduccion_Local reduccion;	reduccion.reductor = reductor;

    	crearHilo();

    	pthread_mutex_lock(&lockCont);
    	paquete = recibir_payload(&yama_fd,&encabezado.tam_payload);

        reduccion.solicitudRedLocal = dsrlz_solicitudReduccionLocal(paquete.buffer);

        log_info(infoLogger,"Detalle de la Reduccion Local: Nodo:%s - Archivo_tmp:%s", reduccion.solicitudRedLocal.nodo,reduccion.solicitudRedLocal.archivo_tmp_reduccion_local);
        printf("Detalle de la Reduccion Local: Nodo:%s - Archivo_tmp:%s\n", reduccion.solicitudRedLocal.nodo,reduccion.solicitudRedLocal.archivo_tmp_reduccion_local);


        pthread_create(&(hilos->hilo),NULL,hiloDeRedLocal,(void*)&reduccion); // Realizo conexión con worker a traves de Hilos

    	free(paquete.buffer);

        encabezado = recibir_header(&yama_fd);	// Espero mas datos para próximas instrucciones
    }

    liberarHilos();

	  t_fin = clock();

	  tRedL = (double)(t_fin - t_ini) / CLOCKS_PER_SEC;
	  printf("%.16g milisegundos\n", tRedL * 1000.0);
    }	//Cierro replanificacion
///////////////////// Comienzo de la etapa de Reducción Global////////////////////////////////////////////////// 


    int bloques = encabezado.tam_payload;
	//cantidad de solicitudesL para la reduccion local
	printf("cantidad de solicitudes de reduccion locales listas para la RG:\t%i\n",encabezado.tam_payload);
    log_info(infoLogger,"cantidad de solicitudes de reduccion locales listo para la RG: %i", encabezado.tam_payload);
	encabezado = recibir_header(&yama_fd);

	t_ini = clock();
	if(encabezado.cod_operacion == INICIAR_REDUCCION_ARCHIVO_GLOBAL){
	log_info(infoLogger, "Empezando etapa de Reducción Global");
	printf("########################################\nEmpezando etapa de Reducción Global\n########################################\n");

	notificacionArchivo notificacion;
	int worker_fd;
	t_delegacion_reduccion_global delegacionGlobal;
	lista_delegacion_reduccion_global* punt	=	malloc(sizeof(lista_delegacion_reduccion_global));
	delegacionGlobal.lista = punt;
	punt->worker_port = 0;
	punt->archivos_temporales	=	"";
	punt->nodo					=	"";
	punt->worker_ip				=	"";

   while((encabezado.cod_operacion == INICIAR_REDUCCION_ARCHIVO_GLOBAL) && bloques){//TODO

    	t_solicitud_reduccion_global global;

    	paquete = recibir_payload(&yama_fd,&encabezado.tam_payload);
    	global = dsrlz_solicitudReduccionGlobal(paquete.buffer);
    	free(paquete.buffer);
        log_info(infoLogger, "Solicitud de Reducción Global recibida de Yama");

    	if(global.encargado){ // Si es el encargado

    		printf("-------------------------------------\nNodo Encargado:\t%s\n-------------------------------------\n",global.nodo);
        	if ((worker_fd = conectarseAservidor(global.worker_ip,global.worker_port))==-1) {	// Notifica fallo si no puedo conectar

        		notificacion.fallo = true;

        		paquete = srlz_notificacionArchivo(&notificacion,'M',REDUCCION_GLOBAL_TERMINADA_CON_FRACASO);

        		send(yama_fd,paquete.buffer,paquete.tam_buffer,0);
        		free(paquete.buffer);
                exit(1);

        	} else if(copiarArchivo_a_Worker(reductor, COPIAR_REDUCTOR, worker_fd, infoLogger)){// Envío reductor
                log_info(infoLogger, "Archivo Reductor enviado al Worker Encargado para aplicar a la Etapa Global");
            }

    		delegacionGlobal.archivo_reduccion_global 			=	malloc(strlen(global.archivo_reduccion_global)		+1);
    		strcpy(delegacionGlobal.archivo_reduccion_global,global.archivo_reduccion_global);
    		delegacionGlobal.archivo_reduccion_global[strlen(global.archivo_reduccion_global)]='\0';

    		delegacionGlobal.archivo_temporal_reduccion_local 	=	malloc(strlen(global.archivo_tmp_reduccion_local)	+1);
    		strcpy(delegacionGlobal.archivo_temporal_reduccion_local,global.archivo_tmp_reduccion_local);
    	    delegacionGlobal.archivo_temporal_reduccion_local[strlen(global.archivo_tmp_reduccion_local)]='\0';

    	} else{																					// Si no es el encargado

    		punt->nodo	=	malloc(strlen(global.nodo)	+1);
    		strcpy(punt->nodo,global.nodo);
    		punt->nodo[strlen(global.nodo)]='\0';


    		punt-> archivos_temporales	=	malloc(strlen(global.archivo_tmp_reduccion_local)	+1);
			strcpy(punt->archivos_temporales,global.archivo_tmp_reduccion_local);
			punt->archivos_temporales[strlen(global.archivo_tmp_reduccion_local)]='\0';


			punt->worker_ip	=	malloc(strlen(global.worker_ip)	+1);
			strcpy(punt->worker_ip,global.worker_ip);
			punt->worker_ip[strlen(global.worker_ip)]='\0';

    		punt->worker_port = global.worker_port;

    		punt->sig			=	malloc(sizeof(lista_delegacion_reduccion_global));
    		punt				=	punt->sig;
    	    punt->worker_port	=	0;
    	}
    	if(bloques-1)
    		encabezado = recibir_header(&yama_fd);	// Espero mas datos para próximas instrucciones
    	bloques--;
    }

    log_info(infoLogger, "Enviando Solicitud de Reducción Global al Worker Encargado");
    paquete = srlz_delegacionReduccionGlobal(&delegacionGlobal);
    send(worker_fd,paquete.buffer,paquete.tam_buffer,0);
    free(paquete.buffer);
    close(worker_fd);


    free(notificacion.archivo);
    void* recibir = malloc(sizeof(bool));
    recv(worker_fd,recibir,sizeof(bool),0);
    memcpy(&notificacion.fallo,recibir,sizeof(bool));
    free(recibir);
    notificacion.archivo	=	malloc(strlen(delegacionGlobal.archivo_reduccion_global)+1);
    strcpy(notificacion.archivo,delegacionGlobal.archivo_reduccion_global);

    printf("\n\n1\n\n");
    if(!notificacion.fallo){
        printf("\n\n1\n\n");
    	printf("Finalizo correctamente la etapa de Reduccion Global.\n");
        log_info(infoLogger, "Finalizo correctamente la etapa de Reduccion Global.");
    } else{
    	printf("\nFinalizo insatisfactoriamente la etapa de Reduccion Global.\n");
        log_info(infoLogger, "Finalizo insatisfactoriamente la etapa de Reduccion Global.");
    }

    paquete = srlz_notificacionArchivo(&notificacion,'M',ALMACENADO_FINAL);
    printf("\n\n2\n\n");
    send(yama_fd,paquete.buffer,paquete.tam_buffer,0);
    printf("\n\n2\n\n");
    free(paquete.buffer);
    free(notificacion.archivo);
    close(worker_fd);
	}
	t_fin = clock();

	tRedG = (double)(t_fin - t_ini) / CLOCKS_PER_SEC;
	printf("%.16g milisegundos\n", tRedG * 1000.0);

	//TODO	Almacenado final

	if(encabezado.cod_operacion == ALMACENADO_FINAL){

		t_solicitud_almacenado_final almacenado;

    	paquete = recibir_payload(&yama_fd,&encabezado.tam_payload);
    	almacenado = dsrlz_solicitudAlmacenadoFinal(paquete.buffer);

    	printf("gv");

	}

    /* ---------------------------------------- */
    /*  Libero Memoria de Log y Config          */
    /* ---------------------------------------- */
    pthread_mutex_destroy(&lockCont);
    log_destroy(infoLogger);
    config_destroy(cfg);
    free(transformador);
    free(reductor);
    free(archivo);
    free(destino);
    
    close(yama_fd);

    printf("\n#############################################################################################################");
    printf("\nEtapa de Transformacion   :   Tiempo transcurrido %.16g milisegundos  Hilos creados %i"	,tTrans * 1000.0	,contT);
    printf("\nEtapa de Reducción Local  :   Tiempo transcurrido %.16g milisegundos  Hilos creados %i"	,tRedL 	* 1000.0	,contRL);
    printf("\nEtapa de Reducción Global :   Tiempo transcurrido %.16g milisegundos"						,tRedG 	* 1000.0);
    printf("\nJob Master finalizado	    :   Tiempo transcurrido %.16g milisegundos"						,(tTrans+tRedL+tRedG)*1000.0);
    printf("\n#############################################################################################################\n");

    return 0;
};

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

void* hiloDeTransformacion(void* solicitud){

	contT++;

	Transformacion* transformacionC = (Transformacion*) solicitud;
	t_solicitud_transformacion transformacion;

	transformacion.bloque			=	transformacionC->solicitudTransformacion.bloque;
	transformacion.bytes_ocupados	=	transformacionC->solicitudTransformacion.bytes_ocupados;
	transformacion.worker_port		=	transformacionC->solicitudTransformacion.worker_port;

	transformacion.archivo_tmp		=	malloc(strlen(transformacionC->solicitudTransformacion.archivo_tmp)	+1);
	transformacion.nodo			=	malloc(strlen(transformacionC->solicitudTransformacion.nodo)			+1);
	transformacion.worker_ip		=	malloc(strlen(transformacionC->solicitudTransformacion.worker_ip)	+1);

	strcpy(transformacion.archivo_tmp,transformacionC->solicitudTransformacion.archivo_tmp);
	strcpy(transformacion.nodo,transformacionC->solicitudTransformacion.nodo);
	strcpy(transformacion.worker_ip,transformacionC->solicitudTransformacion.worker_ip);

	pthread_mutex_unlock(&lockCont);

	notificacionArchivo notificacion;
    Paquete paquete;

//***************************************Cargo los datos a Utilizar*******************************************************//
	//delegacion.programa_transformacion = malloc(strlen(transformacion->transformador));
	//delegacion.archivo_destino = malloc(strlen(transformacion->destino));

	notificacion.archivo = malloc(strlen(transformacion.archivo_tmp)+1);
	strcpy(notificacion.archivo,transformacion.archivo_tmp);

	int worker_fd ;

    log_info(infoLogger, "Iniciando conexion con Worker");
    printf("\nIniciando conexion con el Worker del %s\n",transformacion.nodo);

    printf("IP: %s\nport: %d\n",transformacion.worker_ip,transformacion.worker_port);
	if((worker_fd = conectarseAservidor(transformacion.worker_ip,transformacion.worker_port)) == -1)	{

		notificacion.fallo = true;

		paquete = srlz_notificacionArchivo(&notificacion,'M',INICIAR_REDUCCION_ARCHIVO_LOCAL);

	    log_info(infoLogger, "Worker desconectado: %s",transformacion.nodo);
	    printf("Worker desconectado: %s\n",transformacion.nodo);
		send(yama_fd,paquete.buffer,paquete.tam_buffer,0);
		free(paquete.buffer);
	    log_info(infoLogger, "Notificacion de desconexion del worker %s enviada a YAMA",transformacion.nodo);
	    printf("Notificacion de desconexion del worker %s enviada a YAMA\n",transformacion.nodo);

	}else{

//****************************** Envío del archivo Transformacion a Worker ************************************//

		log_info(infoLogger, "Iniciando envio del archivo Transformador: %s al Worker",transformacionC->transformador);
		if(copiarArchivo_a_Worker(transformacionC->transformador, COPIAR_TRANSFORMADOR, worker_fd, infoLogger)){
        	log_info(infoLogger, "Archivo Transformador enviado al Worker");
        }


//***************************************Cargo los datos a Utilizar*******************************************************//
		t_delegacion_transformacion delegacion;

		delegacion.archivo_destino	= malloc(strlen(transformacion.archivo_tmp)+1);

		delegacion.bloque = transformacion.bloque;
		delegacion.num_bytes = transformacion.bytes_ocupados;
		strcpy(delegacion.archivo_destino,transformacion.archivo_tmp);
		printf("\nArchivo destino:\t%s\nbloque:         \t%i\nnum_bytes:      \t%i\nTransformador:  \t%s\n",
		delegacion.archivo_destino, delegacion.bloque, delegacion.num_bytes,transformacionC->transformador);

//**********************************************************************************************************//

		paquete = srlz_delegacionTransformacion(&delegacion);
		send(worker_fd,paquete.buffer,paquete.tam_buffer,0);
		free(paquete.buffer);
		free(delegacion.archivo_destino);

//**********************************************************************************************//


		void* recibir = malloc(sizeof(bool));
		recv(worker_fd,recibir,sizeof(bool),0);
		memcpy(&notificacion.fallo,recibir,sizeof(bool));
		free(recibir);

		if(!notificacion.fallo){
			printf("\n %s  finalizo correctamente la etapa de Transformación.\n",transformacion.nodo);
            log_info(infoLogger, "%s finalizo correctamente la etapa de Transformación.",transformacion.nodo);
		} else{
			printf("\n %s  finalizo insatisfactoriamente la etapa de Transformación.\n",transformacion.nodo);
            log_info(infoLogger, "%s finalizo insatisfactoriamente la etapa de Transformación.", transformacion.nodo);
		}

		paquete = srlz_notificacionArchivo(&notificacion,'M',INICIAR_REDUCCION_ARCHIVO_LOCAL);
		send(yama_fd,paquete.buffer,paquete.tam_buffer,0);
		free(paquete.buffer);
		close(worker_fd);
	}
	free(notificacion.archivo);
	return 0;

}

void* hiloDeRedLocal(void* solicitud){

	contRL++;

	Reduccion_Local* reduccionC = (Reduccion_Local*) solicitud;
	t_solicitud_reduccion_local reduccion;
	reduccion.worker_port = reduccionC->solicitudRedLocal.worker_port;

	reduccion.archivo_tmp_reduccion_local	=	malloc(strlen(reduccionC->solicitudRedLocal.archivo_tmp_reduccion_local)	+1);
	reduccion.nodo							=	malloc(strlen(reduccionC->solicitudRedLocal.nodo)							+1);
	reduccion.worker_ip						=	malloc(strlen(reduccionC->solicitudRedLocal.worker_ip)						+1);

	strcpy(reduccion.archivo_tmp_reduccion_local	,reduccionC->solicitudRedLocal.archivo_tmp_reduccion_local	);
	strcpy(reduccion.nodo							,reduccionC->solicitudRedLocal.nodo							);
	strcpy(reduccion.worker_ip						,reduccionC->solicitudRedLocal.worker_ip					);

	lista_temporales_transformacion* punt	=	malloc(sizeof(lista_temporales_transformacion));
	reduccion.archivo_tmp_transformacion	=	punt;
	while(reduccionC->solicitudRedLocal.archivo_tmp_transformacion){
		punt->archivo_temporal_transformacion	=	malloc(strlen(reduccionC->solicitudRedLocal.archivo_tmp_transformacion->archivo_temporal_transformacion)+1);
		strcpy(punt->archivo_temporal_transformacion	,reduccionC->solicitudRedLocal.archivo_tmp_transformacion->archivo_temporal_transformacion);
		punt->sig = malloc(sizeof(lista_temporales_transformacion));
		if((reduccionC->solicitudRedLocal.archivo_tmp_transformacion = reduccionC->solicitudRedLocal.archivo_tmp_transformacion->sig)){
			punt = punt->sig;
		} else{
			punt->sig = NULL;
		}
	}
	pthread_mutex_unlock(&lockCont);

	notificacionArchivo notificacion;
	Paquete paquete;
	int worker_fd;

	notificacion.archivo = malloc(strlen(reduccion.archivo_tmp_reduccion_local)+1);
	strcpy(notificacion.archivo,reduccion.archivo_tmp_reduccion_local);

    log_info(infoLogger, "Iniciando conexion con Worker");
    printf("\nIniciando conexion con el Worker del %s\n",reduccion.nodo);

    printf("IP: %s\nport: %d\n",reduccion.worker_ip,reduccion.worker_port);

	if ((worker_fd = conectarseAservidor(reduccion.worker_ip,reduccion.worker_port))==-1) {

		notificacion.fallo = true;

		paquete = srlz_notificacionArchivo(&notificacion,'M',REDUCCION_LOCAL_TERMINADA_CON_FRACASO);

		send(yama_fd,paquete.buffer,paquete.tam_buffer,0);
		free(paquete.buffer);

	}else{

//****************************** Envío del archivo Rduccion Local a Worker ************************************//

		log_info(infoLogger, "Iniciando envio del archivo Reduccion Local al Worker");
		if(copiarArchivo_a_Worker(reduccionC->reductor, COPIAR_REDUCTOR, worker_fd, infoLogger)){
		        	log_info(infoLogger, "Archivo Reductor enviado al Worker para aplicar a la Etapa Local");
		        }

//***************************************Cargo los datos a Utilizar*******************************************************//
		t_delegacion_reduccion_local delegacion;

		delegacion.archivo_destino = malloc(strlen(reduccion.archivo_tmp_reduccion_local)+1);
		strcpy(delegacion.archivo_destino,reduccion.archivo_tmp_reduccion_local);
		delegacion.lista_archivo_argumento = reduccion.archivo_tmp_transformacion;
		paquete = srlz_delegacionReduccionLocal(&delegacion);
		send(worker_fd, paquete.buffer, paquete.tam_buffer,0);
		free(paquete.buffer);
		free(delegacion.archivo_destino);

//**********************************************************************************************//

		void* recibir = malloc(sizeof(bool));
		recv(worker_fd,recibir,sizeof(bool),0);
		memcpy(&notificacion.fallo,recibir,sizeof(bool));
		free(recibir);

		if(!notificacion.fallo){
			printf("\n %s  finalizo correctamente la etapa de Reducción Local.\n",reduccion.nodo);
            log_info(infoLogger, "%s  finalizo correctamente la etapa de Reducción Local.",reduccion.nodo);
		} else{
			printf("\n %s  finalizo insatisfactoriamente la etapa de Reducción Local.\n",reduccion.nodo);
            log_info(infoLogger, "%s  finalizo insatisfactoriamente la etapa de Reducción Local.",reduccion.nodo);
		}

		paquete = srlz_notificacionArchivo(&notificacion,'M',INICIAR_REDUCCION_ARCHIVO_GLOBAL);
		send(yama_fd,paquete.buffer,paquete.tam_buffer,0);
		free(paquete.buffer);

		close(worker_fd);
	}
	free(notificacion.archivo);
	return 0;
}
