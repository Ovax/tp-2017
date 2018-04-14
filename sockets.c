#include "sockets.h"

int crearServidor(int puerto){
	int sockfd;
	struct sockaddr_in my_addr;
	int yes=1;
	if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1){
		perror("socket");
		return -1;
	}
	if (setsockopt(sockfd,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(int)) == -1){
		perror("setsockopt");
		return -1;
	}

	my_addr.sin_family = AF_INET;
	my_addr.sin_port = htons(puerto);
	my_addr.sin_addr.s_addr = INADDR_ANY;
	memset(&(my_addr.sin_zero), '\0', 8);

	if (bind(sockfd, (struct sockaddr *)&my_addr, sizeof(struct sockaddr)) == -1) {
		perror("bind");
		return -1;
	}
	if (listen(sockfd, BACKLOG)==-1){
		perror("listen");
		return -1;
	}
	return sockfd;
}		

int	conectarseAservidor(char *ip,int puerto){
	int sockfd ;
	struct sockaddr_in their_addr; // información de la dirección 
	if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror("socket");
		return -1;
	}

	their_addr.sin_family = AF_INET; // Ordenación de bytes de la máquina
	their_addr.sin_port = htons(puerto); // short, Ordenación de bytes de la red
	their_addr.sin_addr.s_addr = inet_addr(ip); //ip del CDE
	memset(&(their_addr.sin_zero),'\0', 8); // poner a cero el resto de la estructura

	if (connect(sockfd, (struct sockaddr *)&their_addr, sizeof(struct sockaddr)) == -1) {
		perror("connect");
		return -1;
	}
	return sockfd;
}

void enviarString(int socket, char* buf){

	printf ("escribir mensaje\n");
	scanf("%s",buf);
	if (send(socket,buf,strlen(buf),0)==-1){
		perror("error al enviar");
	}
	bzero(buf,sizeof(buf));
}

void recibirString(int socket, char* cadena){

	int numbytes;
	//printf("recibir %d bytes que son esta cadena %s en el socket %d\n",size,cadena, socket);
	if ((numbytes=recv(socket, cadena, 50, 0)) == -1) {
		perror("recv");
		close(socket);
		exit(1);
	}
}

int sendall(int socket, void *buf, int size){
	int enviado = 0; // cuántos bytes hemos enviado
	int totalAenviar = size; // cuántos se han quedado pendientes
	int parcial;
	while(enviado < totalAenviar){
		parcial = send(socket, buf+enviado, totalAenviar, 0);
		if (parcial == -1)
			break;
		enviado += parcial;
		totalAenviar -= enviado;
	}
	return enviado; // devuelve -1 si hay fallo, 0 en otro caso
}

int aceptarconexion (int socket_escucha){
	struct sockaddr_in cliente_addr;
	int newfd;
	int size =sizeof(cliente_addr);
	if ((  newfd = accept(socket_escucha, (struct sockaddr *)&cliente_addr, &size)) == -1) {
		perror("error aceptar\n");
		exit(1);
	}
	printf("conectado\n");
	return newfd;
}
void enviar_estructura(int* socket ,t_solicitud_transformacion* solicitud){
	Paquete paquete;
	t_solicitud_transformacion solicitud_transformacion =*solicitud;
    int socket1=*socket;
	paquete=srlz_solicitudTransformacion(&solicitud_transformacion);
	if (send(socket1,paquete.buffer,paquete.tam_buffer,0)==-1){
		perror("fallo envio");
	}
}

Encabezado recibir_header(int* socket){
	int num,tam_buf,codigo_ope;
	char proceso;
	Encabezado encabezado;

	void* buf=malloc(sizeof(int)*2+sizeof(char));
	bzero(buf,sizeof(int)*2+sizeof(char));

	//Recibo el header
	if ((num=recv(*socket,buf,sizeof (int)*2+sizeof(char),0))==-1){
		perror("fallo recepcion");
	}

	// Deserializo el header
	memcpy(&proceso,buf,sizeof(char));
	memcpy(&codigo_ope,buf+sizeof(char),sizeof(int));
	memcpy(&tam_buf,buf+sizeof(char)+sizeof(int),sizeof(int));


	
	// Armo el Encabezado para devolverlo
	encabezado.proceso = proceso;
	encabezado.cod_operacion = codigo_ope;
	encabezado.tam_payload = tam_buf;
	free(buf);

	return encabezado;
}
Paquete recibir_payload(int* socket,int* tam){
	int num;
	Paquete paquete;
	int valor=*tam;
	void* buf=malloc(valor);
	bzero(buf,valor);
	int socket_envio=*socket;
	if ((num=recv(socket_envio,buf,valor,MSG_WAITALL))==-1){
		perror("fallo recepcion");

	}
	paquete.buffer=buf;
	paquete.tam_buffer=valor;
	//free(buf);
	return paquete;

}

