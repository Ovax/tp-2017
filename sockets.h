#include <sys/select.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <netinet/in.h>
#include <asm/poll.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <arpa/inet.h>
#include <time.h>
#include <sys/wait.h>
#include <signal.h>
#include <commons/config.h>
#include <commons/log.h>
#include "registros.h"
#include "funciones.h"
#define MAXDATASIZE 100 // máximo número de bytes que se pueden leer de una vez
#define BACKLOG 10

int crearServidor(int puerto);
int conectarseAservidor(char *ip,int puerto);

void enviarString(int socket, char* buf);
void recibirString(int socket, char* buf);

int sendall(int, void *, int);

int aceptarconexion(int socket_escucha);
void enviar_estructura(int* socket ,t_solicitud_transformacion* solicitud);
Encabezado recibir_header(int* socket);
Paquete recibir_payload(int* socket,int* tam);

