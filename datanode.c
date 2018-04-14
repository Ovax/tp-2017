#include <arpa/inet.h>
#include <commons/collections/list.h>
#include <commons/config.h>
#include <commons/log.h>
#include <commons/string.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
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

/* ---------------------------------------- */

int main(int argc, char* argv[]){

	/* Creo la instancia del Archivo de Configuracion y del Log */
	cfg = config_create("config/config.cfg");
	infoLogger = log_create("log/datanode.log", "Datanode", false, LOG_LEVEL_INFO);

	Encabezado encabezado;
	Paquete paquete;

	log_info(infoLogger, "Iniciando DATANODE" );
	printf("Iniciando DATANODE\n");

    /* -------------------------------------------------------------------- */
    // Si no existe data.bin, lo creo
    /* -------------------------------------------------------------------- */

		if (!existeArchivo(config_get_string_value(cfg,"RUTA_DATABIN"))){
			crearDataBin(config_get_string_value(cfg,"RUTA_DATABIN"));
		}

    /* -------------------------------------------------------------------- */

    struct sockaddr_in servidor_addr,my_addr,master_addr; // información de la dirección de destino
    int sockfd, numbytes,escucha_master,fd_maximo,nuevo_fd,i,size;
    fd_set master,temporales;
    FD_ZERO(&master);
    FD_ZERO(&temporales);


    // Creo conexión con FS
	int fs_fd = conectarseAservidor(config_get_string_value(cfg,"FS_IP"),config_get_int_value(cfg,"FS_PUERTO"));
    log_info(infoLogger, "Conectado con FS" );
    printf("Conectado con FS\n");

    // Le envio al FS los datos del Nodo
    t_nodos datos_nodo;
	obtenerDatosNodo(&datos_nodo);
	paquete=srlz_datosNodo(&datos_nodo);
	send(fs_fd,paquete.buffer,paquete.tam_buffer,0);

	// Libero recursos
	free(paquete.buffer);
	free(datos_nodo.nodo);
	free(datos_nodo.ip);

    // Creo el Servidor para escuchar conexiones
    escucha_master=crearServidor(config_get_int_value(cfg,"NODO_PUERTO"));
    log_info(infoLogger, "Escuchando conexiones" );
    printf("Escuchando conexiones\n");


	FD_SET(fs_fd,&master);
	FD_SET(escucha_master, &master);

    // seguir la pista del descriptor de fichero mayor
    fd_maximo = escucha_master; // por ahora es éste

    t_bloque_contenido contenido_bloque;
    t_bloque_contenido* contenido_bloque_archivo;
    t_solicitud_pedido_bloque solicitud_pedido_bloque;

    while(1){
    	//se comunica el master
    	temporales=master;

    	if(select(fd_maximo+2,&temporales,NULL,NULL,NULL)==-1){
    		perror("select");
    		exit (1);
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
                        if (nuevo_fd > fd_maximo) {    // actualizar el máximo
                            fd_maximo = nuevo_fd;
                        }
						log_info(infoLogger, "Conexion nueva recibida" );
						printf("Conexion nueva recibida\n");
                    }
                } else {

					encabezado=recibir_header(&i);

					switch(encabezado.cod_operacion){

						case PEDIDO_BLOQUES:
							printf("Pedido de Bloque por FS.\n");
							log_info(infoLogger,"Pedido de Bloque por FS.");

							// Recibiendo solicitud de bloque de FS
							paquete=recibir_payload(&i,&encabezado.tam_payload);							
							solicitud_pedido_bloque=dsrlz_solicitud_pedido_bloque(paquete.buffer);
							free(paquete.buffer);

							printf("Detalle de la Solicitud: Nro de Bloque: %d - Tamaño del Bloque: %d\n", solicitud_pedido_bloque.bloque, solicitud_pedido_bloque.tamano_bloque );
							log_info(infoLogger,"Detalle de la Solicitud: Nro de Bloque: %d - Tamaño del Bloque: %d", solicitud_pedido_bloque.bloque, solicitud_pedido_bloque.tamano_bloque);

							// Obtengo el Bloque de data.bin
							contenido_bloque_archivo = obtenerBloque(solicitud_pedido_bloque.bloque, solicitud_pedido_bloque.tamano_bloque, infoLogger);

							// Serializo el bloque a enviar
							paquete =srlz_bloque_contenido('D', PEDIDO_BLOQUES, contenido_bloque_archivo->contenido_archivo, solicitud_pedido_bloque.tamano_bloque, contenido_bloque_archivo->bloque);

							send(fs_fd,paquete.buffer,paquete.tam_buffer,0);
							free(paquete.buffer);
							free(contenido_bloque_archivo->contenido_archivo);
							free(contenido_bloque_archivo->nombre);
							free(contenido_bloque_archivo);

							printf("Bloque enviado a FS.\n");
							log_info(infoLogger,"Bloque enviado a FS");

							break;

						case COPIAR_BLOQUES: 
							printf("Inicio de recepción de archivo de FS.\n");
							log_info(infoLogger,"Inicio de recepción de archivo de FS.");

							// Recibiendo bloque de FS
							paquete=recibir_payload(&i,&encabezado.tam_payload);
							contenido_bloque=dsrlz_bloque_archivo(paquete.buffer);
							free(paquete.buffer);

							printf("Bloque recibido de FS.\n");
							log_info(infoLogger,"Bloque recibido de FS");

							// Persisto el Bloque en data.bin
							persistirBloque(contenido_bloque.bloque, contenido_bloque.contenido_archivo, contenido_bloque.tamanio, infoLogger);
							free(contenido_bloque.contenido_archivo);
							free(contenido_bloque.nombre);

							printf("Bloque persistido en data.bin.\n");
							log_info(infoLogger,"Bloque persistido en data.bin");

							break;
					}

                }
            }
        }
    }

    /* ---------------------------------------- */
    /*  Libero Memoria de Log y Config          */
    /* ---------------------------------------- */
    log_destroy(infoLogger);
    config_destroy(cfg);

    close(sockfd);

    return 0;
}
