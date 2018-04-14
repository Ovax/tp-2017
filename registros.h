#ifndef __REGISTROS_H__
#define __REGISTROS_H__

//******************************************

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// Defino los Codigo de Operaciones
enum cod_operacion {
	INICIAR_TRANSFORMACION_ARCHIVO = 20,
	INICIAR_REDUCCION_ARCHIVO_LOCAL = 1,
	INICIAR_REDUCCION_ARCHIVO_GLOBAL = 2,
	ALMACENADO_FINAL = 3,

	COPIAR_TRANSFORMADOR = 4,
	COPIAR_REDUCTOR = 5,

	TRANSFORMACION_TERMINADA_CON_EXITO = 6,
	TRANSFORMACION_TERMINADA_CON_FRACASO = 7,
	REDUCCION_LOCAL_TERMINADA_CON_EXITO = 8,
	REDUCCION_LOCAL_TERMINADA_CON_FRACASO = 9,
	REDUCCION_GLOBAL_TERMINADA_CON_EXITO = 10,
	REDUCCION_GLOBAL_TERMINADA_CON_FRACASO = 11,
	ALMACENADO_FINAL_CON_EXITO=12,
	ALMACENADO_FINAL_CON_FRACASO =13,

	REPLANIFICACION=14,
	FINALIZAR_JOB = 15,
	OBTENER_INFORMACION_ARCHIVO = 16,
	ALMACENAR_ARCHIVO = 17,
	LEER_ARCHIVO = 18,
	INFO_NODO = 19,
	NOTIFICAR_ARCHIVO=21,
	COPIAR_BLOQUES = 22,
	PEDIDO_BLOQUES = 23,
	NOMBRE_ARCHIVO_FINAL_RG=24,
	ARCHIVO_NO_EXISTE=25,
	ENVIO_ARCHIVO_REDUCCION_GLOBAL=26,
};



typedef struct lista_temporales_transformacion{
	char* archivo_temporal_transformacion;
	struct lista_temporales_transformacion* sig;
}lista_temporales_transformacion;

// MASTER-WORKER MENSAJES

	typedef struct{
		int bloque;
		int num_bytes;
		char * archivo_destino; //archivo donde se va a guardar el resultado
	}t_delegacion_transformacion;

	typedef struct{
		lista_temporales_transformacion * lista_archivo_argumento; //archivo sobre el cual se va a aplicar la reduccion
		char * archivo_destino; //archivo donde se va a guardar el resultado
	}t_delegacion_reduccion_local;

	typedef struct lista_delegacion_reduccion_global{
		char * nodo;
		char * worker_ip;
		int worker_port;
		char * archivos_temporales;
		struct lista_delegacion_reduccion_global* sig;
	}lista_delegacion_reduccion_global;

	typedef struct {
		char* archivo_reduccion_global;
		char* archivo_temporal_reduccion_local;
		lista_delegacion_reduccion_global* lista;
	}t_delegacion_reduccion_global;

	typedef struct{
		char* archivo;
	}t_delegacion_almacenar_archivo;
/************************/



// Definicion de Tabla de Estados (YAMA)
typedef struct {
	int id_job;
	int master;
	char* nodo;
	int bloque;
	char* etapa;
	char* archivo_tmp;
	int estado; // EN_PROCESO=1, ERROR=2, FINALIZADO=3
}t_estados;

typedef struct //dato nodo para la lista de datos de nodo de yama
{
	char* ip;
	int port;
	char* nodo;
	int cantidadtrabajo;
}dato_nodo;

typedef struct
{
	int socketmaster;
	int master;
	int job;
	char* nombrearchivoRGfinal;
}datomaster;

// Definicion de la Tabla de Directorios
typedef struct {
	int index;
	char nombre[255];
	int padre;
}t_directorios;

// Definicion de la Tabla​ ​de​ Archivos  - FALTA DEFINIR
typedef struct {
	int tamano;
	char tipo[255];
}t_archivos;

// Definicion de la Tabla​ ​de​ Nodos
typedef struct {
	int tamano;
	int libre;
	char* nodo;
	char* ip;	
	int port_worker;
	int port_datanode;
}t_nodos;

// Definicion de los Nodos y sus sockets
typedef struct {
	int socket;
	char* nodo;
}t_nodo_socket;

// Definicion de Tipo NodoBloque
typedef struct {
	char* nodo;
	int port;
	char* ip;	
	int bloque;
}nodo_bloque;

// Definicion de la Tabla​ ​de​ Bloques de Archivos
typedef struct  {
	int bloque;
	nodo_bloque copia1;
	nodo_bloque copia2;
	int bytes_ocupados;
}t_bloques_archivos;

// Definicion de Bitmap​ ​de​ ​Bloques​ ​por​ ​Nodo
typedef struct  {
	char* nodo;
	int bloque;
	int estado; // Libre o Ocupado
}t_bitmap_bloques_nodo;

// Definicion del contenido de un Bloque de un Archivo y su ubicacion
typedef struct {
	char* nombre;
	char* contenido_archivo;
	int tamanio;
	int bloque;
}t_bloque_contenido;

// Definicion de Solicitud de Transformacion
typedef struct  {
	char* nodo;
	char* worker_ip;
	int worker_port;
	int bloque;
	int bytes_ocupados;
	char* archivo_tmp;
}t_solicitud_transformacion;

/*/ Definicion de Solicitud de Reduccion Local
typedef struct  {
	char* nodo;
	char* worker_ip;
	int worker_port;
	char* archivo_tmp_transformacion;
	char* archivo_tmp_reduccion_local;
}t_solicitud_reduccion_local;
*/

typedef struct  {
	char* nodo;
	char* worker_ip;
	int worker_port;
	lista_temporales_transformacion* archivo_tmp_transformacion;
	char* archivo_tmp_reduccion_local;
}t_solicitud_reduccion_local;


// Definicion de Solicitud de Reduccion Global
typedef struct {
	char* nodo;
	char* worker_ip;
	int worker_port;
	char* archivo_tmp_reduccion_local;
	char* archivo_reduccion_global;
	bool encargado; // true = Si o false = no
}t_solicitud_reduccion_global;

// Definicion de Solicitud de Almacenado Final
typedef struct
{
	char* nodo;
	char* worker_ip;
	int worker_port;
	char* archivo_reduccion_global;
}t_solicitud_almacenado_final;


// Definicion de Solicitud de Reduccion Local
typedef struct  {
	int bloque;
	int tamano_bloque;
}t_solicitud_pedido_bloque;

//******************************************

typedef struct //Paquete
{
	void* buffer;
	int tam_buffer;
}Paquete;

typedef struct //Encabezado
{
	char proceso;
	int cod_operacion;
	int tam_payload;
}Encabezado;


//******************************************

//*************Master - YAMA****************

typedef struct
{
	char* archivo;

}Solicitud_Master;

typedef struct
{
	char* archivo;
	bool fallo;  //true -> (hay que replanificar si la etapa es Transformacion), false -> terminó ok

}notificacionArchivo;

typedef struct  worker_algoritmo
{
	int prioridad;
	char* nodo;
	int canttrabajo;
	struct worker_algoritmo* next;
}worker_algoritmo;

typedef struct
{
	int bytes_ocupados;
	char* nombrearchivo;
	nodo_bloque copia;

}copia_replanificacion;

typedef struct
{
	char contenido[200];
	int tamano;

}t_bitmap;

#endif
