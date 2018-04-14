#ifndef __FUNCIONES_H__
#define __FUNCIONES_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <math.h>
#include <string.h>
#include <dirent.h>
#include <commons/config.h>
#include <commons/log.h>
#include <commons/string.h>
#include <commons/collections/list.h>
#include <commons/collections/queue.h>
#include "registros.h"
#include "sockets.h"

/* ---------------------------------------- */
/*  Variables Globales 						*/
/* ---------------------------------------- */

#define TAM_BLOQUE 1048576 // 1 MB

/* ---------------------------------------- */
// Funciones de Serializacion/Deserializacion
/* ---------------------------------------- */

Paquete crearHeader(char proceso, int cod_operacion, int tamPayload);
Paquete srlz_solicitudTransformacion(t_solicitud_transformacion* solicitud);
Paquete srlz_solicitudReduccionGlobal(t_solicitud_reduccion_global* solicitud);
Paquete srlz_solicitudReduccionLocal(t_solicitud_reduccion_local* solicitud);
Paquete srlz_solicitudAlmacenadoFinal(t_solicitud_almacenado_final* solicitud);
Paquete srlz_delegacionTransformacion(t_delegacion_transformacion* delegacion);
Paquete srlz_delegacionReduccionLocal(t_delegacion_reduccion_local* delegacion);
Paquete srlz_delegacionReduccionGlobal(t_delegacion_reduccion_global* delegacion);
Paquete srlz_delegacionAlmacenarArchivo(t_delegacion_almacenar_archivo* delegacion);
Paquete srlz_tablaDirectorio(t_directorios* solicitud);
Paquete srlz_bloquenodo(t_bloques_archivos* solicitud);
Paquete srlz_datosNodo(t_nodos* datos_nodo);
Paquete srlz_notificacionArchivo(notificacionArchivo* notificacion,char proceso,int cod);
Paquete srlz_archivo(char proceso, int codigoOperacion, char* nombreArchivo);
Paquete srlz_bloque_archivo(char proceso, int codigoOperacion, char* nombreArchivo, int bytesDesde, int bytesHasta, int bloqueDestino);
Paquete srlz_bloque_contenido(char proceso, int codigoOperacion, char* contenidoBloque, int tamanoBloque, int bloque);
Paquete srlz_solicitud_pedido_bloque(char proceso, int codigoOperacion, int bloque, int tamanoBloque);

t_delegacion_transformacion dsrlz_delegacionTransformacion(void* buffer);
t_delegacion_reduccion_local dsrlz_delegacionReduccionLocal(void* buffer);
t_delegacion_reduccion_global dsrlz_delegacionReduccionGlobal(void* buffer);
t_delegacion_almacenar_archivo dsrlz_delegacionAlmacenarArchivo(void *buffer);
t_solicitud_transformacion dsrlz_solicitudTransformacion(void* buffer);
t_solicitud_reduccion_global dsrlz_solicitudReduccionGlobal(void* buffer) ;
t_solicitud_reduccion_local dsrlz_solicitudReduccionLocal(void* buffer);
t_solicitud_almacenado_final dsrlz_solicitudAlmacenadoFinal(void* buffer) ;
t_bloques_archivos dsrlz_bloquenodo(void* buffer);
t_nodos dsrlz_datosNodo(void* buffer);
notificacionArchivo dsrlz_notificacionArchivo(void* buffer);
char* dsrlz_archivo(void* buffer);
t_bloque_contenido dsrlz_bloque_archivo(void* buffer);
t_bloque_contenido dsrlz_bloque_contenido(void* buffer);
t_solicitud_pedido_bloque dsrlz_solicitud_pedido_bloque(void* buffer);


void free_paquete(Paquete *paquete);

void inicializarEjemploSolicitud(t_solicitud_transformacion* solicitud_transformacion);
int send_solicitudTransformacion(int socket, t_solicitud_transformacion* solicitud);

Paquete srlz_envio_transformacion(Solicitud_Master* solicitud,char proceso,int codigo);
Solicitud_Master dsrlz_envio_transformacion(void* buffer);

/* ---------------------------------------- */
/*  Funciones de DataNode 					*/
/* ---------------------------------------- */

void obtenerDatosNodo(t_nodos* datos_nodo);
void crearDataBin(char* pathDataBin);
void persistirBloque(int bloque, char* contenido, int tamanoContenido, t_log* infoLogger);
t_bloque_contenido* obtenerBloque(int bloque, int tamanoContenido, t_log* infoLogger);

/* ---------------------------------------- */
/*  Funciones de Master						*/
/* ---------------------------------------- */

bool copiarArchivo_a_Worker(char* archivoFrom, int codOperacion, int socketWorker, t_log* infoLogger);
void persistirArchivo(char* nombreArchivo, char* contenido, int tamanoContenido, t_log* infoLogger);
char* concatenar(const char *s1, const char *s2);
void removeN(char* cadena,int n);

/* ---------------------------------------- */
/*  Funciones de FS 						*/
/* ---------------------------------------- */

int countParametrosConsola(char * string);
int getIndexDirectorioYama(t_list* listaTablaDirectorios, t_log* infoLogger);
bool crearDirectorioYama(char* pathDirectorio, t_list* listaTablaDirectorios, t_log* infoLogger);
bool eliminarDirectorioYama(char* pathDirectorio, t_list* listaTablaDirectorios, t_log* infoLogger);
bool eliminarArchivoYama(char* pathArchivo, t_list* listaTablaDirectorios, t_list* listaTablaNodos, t_log* infoLogger);
void renombrarDirectorioYama(char* directorioOrigen, char* directorioDestino, t_list* listaTablaDirectorios, t_log* infoLogger);
void renombrarArchivoYama(char* pathArchivo, char* nombreArchivoDestino, t_list* listaTablaDirectorios, t_log* infoLogger);
t_list* listarDirectorioYama(char* pathDirectorio, t_list* listaTablaDirectorios);
void mostrarArchivosDirectorioFS(char* pathDirectorio, t_list* listaTablaDirectorios);
bool existeDirectorioYama(char* pathDirectorio, t_list* listaTablaDirectorios);
void showContenidolista(t_list* listaTablaDirectorios);
void persistirTablaDirectorio(t_list* listaTablaDirectorios, t_log* infoLogger);
void cargarTablaDirectorio(t_list* listaTablaDirectorios,t_log* infoLogger);
bool existeArchivo(char *filename);
int existeArchivoYama(char *pathArchivo, t_list* listaTablaDirectorios);
char* obtenerArchivoPath(char* pathArchivo);
void mostrarArchivoYama(char *pathArchivo, t_list* listaTablaDirectorios, t_list* listaTablaNodos, t_list* listaNodosConectados, t_log* infoLogger);
bool copiarArchivoYama(char* archivoFrom, char* archivoTo, char* tipoArchivo, t_list* listaTablaDirectorios, t_queue* colaNodos, t_list* listaTablaNodos, t_list* listaNodosConectados, t_queue* colaNodosConectados, t_log* infoLogger);
int cantidadDirectoriosPath(char* pathDirectorio);
bool estaVacioDirectorioYama(char* pathDirectorio, t_list* listaTablaDirectorios);
bool formatearYama(t_list* listaTablaDirectorios, t_log* infoLogger);
t_directorios* ExisteNodoDirectorioYama(t_list* listaTablaDirectorios, char* nodoHijo, char* nodoPadre);
int tamanoBloqueArchivoTexto(char* archivoFrom, int bytesDesde);
void cargarTablaNodos(t_list* listaTablaNodos, t_queue* colaNodos, t_log* infoLogger);
//void addDataNode (t_list* listaTablaNodos, t_nodos datanode, t_log* infoLogger);
bool existeDataNode(t_list* listaTablaNodos, char* nombreDataNode, t_log* infoLogger);
void persistirTablaNodo(t_list* listaTablaNodos, t_log* infoLogger);
void showContenidolistaNodos(t_list* listaTablaNodos);
void showContenidolistaNodosConectados(t_list* listaNodosConectados, t_queue* colaNodosConectados);
void crearBitmapNodo(t_list* listaTablaNodos, t_nodos* datos_nodo, t_log* infoLogger);
t_bloques_archivos* asignarBloquesNodos(bool es_worker, t_list* listaTablaNodos, int nro_bloque, int totalBytesBloque, char* nombreNodo1, char* nombreNodo2, int primerBloqueLibreNodo1, int primerBloqueLibreNodo2);
t_list* obtenerDatosBloquesArchivo(char* pathArchivo, bool es_worker, t_list* listaTablaDirectorios, t_list* listaTablaNodos, t_log* infoLogger);
int obtenerPrimerBloquesLibresNodo(t_list* listaTablaNodos, char* nombreNodo, int total_bloques);
void persistirBitmap(t_list* listaTablaNodos, t_bitmap* bitmap, char* nombreNodo, t_log* infoLogger);
t_bitmap* cargarBitmap(t_list* listaTablaNodos, char* nombreNodo);
void marcarBitmapBloqueOcupado(t_list* listaTablaNodos, t_bitmap* bitmap, char* nombreNodo, int bloque_inicio, int total_bloques, t_log* infoLogger);
void marcarBitmapBloqueLibre(t_list* listaTablaNodos, t_bitmap* bitmap, char* nombreNodo, int bloque, t_log* infoLogger);
int obtenerSocketNodo(t_list* listaNodosConectados, char* nombreNodo);
void cargarListaNodosConectados(t_list* listaNodosConectados, t_list* listaTablaNodos, t_queue* colaNodosConectados, t_nodo_socket* registroNodoSockes);
char* obtenerIPNodo(t_list* listaTablaNodos, char* nombreNodo);
int obtenerPortNodo(t_list* listaTablaNodos, char* nombreNodo, bool es_worker);
void actualizarListaTablaNodos(t_list* listaTablaNodos, t_nodos* registroTablaNodos);
int enviarBloqueNodo(t_list* listaNodosConectados, int bloque, char* nombreNodo, int bloqueDestino, char* archivoFrom, int bytesDesde, int bytesHasta, t_log* infoLogger);
char* recibirBloqueNodo(t_list* listaNodosConectados, char* nombreNodo, int bloque, int tamanoBloque, t_log* infoLogger);
char* obtenerMD5ArchivoYama(char *pathArchivo, t_list* listaTablaDirectorios, t_list* listaNodosConectados, t_log* infoLogger);
bool vaciarTablaArchivos(t_log* infoLogger);
bool vaciarBitmaps(t_log* infoLogger);
int obtenerEspacioLibreNodo(t_list* listaTablaNodos, char* nombreNodo);

/* ---------------------------------------- */
/*  Funciones de Yama 						*/
/* ---------------------------------------- */

//char* cualEsElNodoMenosOcupado(char* nodo1,char* nodo2,t_list* lista);
//char* MenosTrabajoRealizado(char* nodo1,char* nodo2,t_list* lista);

void cualEsElNodoMenosOcupado(t_bloques_archivos* bloque,t_list* lista,t_solicitud_transformacion* solicitud);
t_solicitud_transformacion MenosTrabajoRealizado(t_bloques_archivos* bloque,t_list* lista,int job);
t_solicitud_transformacion algoritmo(t_list* lista,t_bloques_archivos* bloque,int job);
void agregarEstadoALista(t_list* lista,int job,t_solicitud_transformacion* solicitud,int master,char* etapa);
void transformacion(t_solicitud_reduccion_local* solicitud);
t_list* buscarlosArchivosNOdo(char* nodo,t_list* lista);
t_solicitud_reduccion_local crearSolicitudlocal(t_list* listauxiliararchivos,t_list* listanodos );
void crearNOmbretemporalTransformacion(int job,t_solicitud_transformacion* solicitud);
void showContenidolistaEstados(t_list* listaEstados);
void actualizarListaEstado(notificacionArchivo* notificacion,t_list* lista);
t_list* nodoArchivoListo(notificacionArchivo* notificacion,t_list* lista);
void agregardatosNodo(t_solicitud_transformacion* solicitud,t_list* listanodo);
dato_nodo* obtenernodo(t_list* lista,t_estados* estado);
void agregarmasterAlaLista(t_list* listamaster,int master,int nuevo_fd);
datomaster* buscarIdMaster(int mastersocket,t_list* listamasters);
bool listoparaReduccion(t_list* listaestado,notificacionArchivo* notificacion );

void agregarEstadoRLALista(t_list* listaEstados,t_solicitud_reduccion_local* solicitud_l,char* etapa,datomaster* master);
void agregarEstadoRGALista(t_list* listaEstados,t_solicitud_reduccion_global* solicitud_g,char* etapa,datomaster* master,char* nodoseleccionado);
bool listoParaRG(t_list* listaestado,notificacionArchivo* notificacion);
t_list* obtenerArchivosreduccionglobal(t_list* listaestados,notificacionArchivo* notificacion);

//void agregarArchivos(t_list* listaArchivos,lista_temporales_transformacion* local);
void agregarArchivos(t_list* listaArchivos,t_solicitud_reduccion_local* local);

dato_nodo*  seleccionarnodo(t_list* listanodos);
t_solicitud_reduccion_global crearSolicitudGlobal(t_estados* estado,t_list* listanodos,char* nodoseleccionado,datomaster* masterbuscado);
void crearnombreRG(t_solicitud_reduccion_global* solicitudG ,char* nodo,int id_master,int job);

nodo_bloque algoritmoclock(t_bloques_archivos* bloque,worker_algoritmo* lista,worker_algoritmo** punteroclock,int base);
t_solicitud_transformacion crearsolicitudTransformacion(t_bloques_archivos* bloque,nodo_bloque* bloquenodo,int job);
int  funciondisponibilidadWclock(int base,int cargamaxima,int cargaactual);
int buscarelmaximocarga(worker_algoritmo* lista );
worker_algoritmo* aplicarfunciondisponibilidadWclock(worker_algoritmo* lista ,int base);
worker_algoritmo* aplicarfunciondisponibilidadClock(worker_algoritmo* lista ,int base);
worker_algoritmo* aplicardisponibilidad(char* nombrealgoritmo,worker_algoritmo* lista ,int base);
void agregardatosparalaReplanificacion(t_solicitud_transformacion solicitud_t,t_bloques_archivos* bloque,t_list* listaReplanificacion);
t_bloques_archivos replanificacion(t_list* listarp,notificacionArchivo* notificacion,worker_algoritmo** punteroclock,worker_algoritmo* lista,int base,int job);
char* elegirnodoencargado(worker_algoritmo* lista);
void agregardatosnodo(worker_algoritmo** lista ,t_nodos* nodoagregar,t_list* listanodos);
void actualizarListaMaster(char* archivofinalo,int master_id,t_list* listamaster);
void abortarjob(datomaster* masterbuscado,t_list* listamaster,t_list* listaEstados);
bool existecopia(t_list* lista,notificacionArchivo* notificacion);
//t_list*  analisarlistaestadoParaRL(t_list* listaestados);

/* ---------------------------------------- */
#endif
