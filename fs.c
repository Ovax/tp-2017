#include <arpa/inet.h>
#include <commons/collections/list.h>
#include <commons/config.h>
#include <commons/log.h>
#include <commons/string.h>
#include <netinet/in.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <readline/rltypedefs.h>
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
t_list* listaTablaDirectorios;
t_list* listaTablaNodos;
t_list* listaNodosConectados;
t_queue* colaNodos;
t_queue* colaNodosConectados;

/* ---------------------------------------- */

void sigchld_handler(int s)
{
	while(wait(NULL) > 0);
}

/* ---------------------------------------- */
/*  Consola interactiva del FS  			*/
/* ---------------------------------------- */

int quitConsola = false;

void consola_interactiva(char* comandoConsola) {

	char** parametrosConsola;
	char** parametrosConsolaOriginal;
	char* comandoConsolaOriginal;
	bool comandoAceptado = false;
	//t_nodos* registroTablaNodos;

	if(NULL == comandoConsola){
		quitConsola = true;
		return;
	}
	if(strlen(comandoConsola) > 0) add_history(comandoConsola);

	//genero una copia del comando ingresado
	comandoConsolaOriginal = malloc(sizeof(char) * strlen(comandoConsola)+1);
	strcpy(comandoConsolaOriginal, comandoConsola);

	//convierto lo escrito en mayuscula para poder comparar
	string_to_upper(comandoConsola);

	// Elimino espacios a izquierda y derecha
	string_trim(&comandoConsola);

    /* -------------------------------------------------------------------- */
    // Defino los comandos aceptados por la Consola Interactiva
    /* -------------------------------------------------------------------- */

		// Reconozco los parametros ingresados
		parametrosConsola = string_split(comandoConsola, " ");
		parametrosConsolaOriginal = string_split(comandoConsolaOriginal, " ");
		free(comandoConsolaOriginal);

	 	if(string_starts_with(comandoConsola,"HELP")){
			comandoAceptado = true;

			printf("Los comandos aceptados por esta consola son:\n\n");
			printf("\tformat \t- Formatear el Filesystem. \n\n");
			printf("\n\trm [path_archivo] ó rm -d [path_directorio] ó rm -b [path_archivo] [nro_bloque] [nro_copia] \t- Eliminar un Archivo/Directorio/Bloque. Si un directorio a eliminar no se encuentra vacío, la operación debe fallar. Además, si el bloque a eliminar fuera la última copia del mismo, se deberá abortar la operación informando lo sucedido. \n\n");
			printf("\n\trename [path_original] [nombre_final] \t- Renombra un Archivo o Directorio \n\n");
			printf("\n\tmv [path_original] [path_final] \t- Mueve un Archivo o Directorio \n\n");
			printf("\n\tcat [path_archivo] \t- Muestra el contenido del archivo como texto plano. \n\n");
			printf("\n\tmkdir [path_dir] \t- Crea un directorio. \n\n");
			printf("\n\tcpfrom [path_archivo_origen] [directorio_yamafs] [tipo_archivo]\t- Copia un archivo local al YamaFS. El tipo_archivo puede ser binario (b) o texto (t)\n\n");
			printf("\n\tcpto [path_archivo_yamafs] [directorio_filesystem] \t- Copia un archivo local al YamaFS. \n\n");
			printf("\n\tcpblock [path_archivo] [nro_bloque] [id_nodo] \t- Crea una copia de un bloque de un archivo en el nodo dado. \n\n");
			printf("\n\tmd5 [path_archivo_yamafs] \t- Solicitar el MD5 de un archivo en YamaFS. \n\n");
			printf("\n\tls [path_directorio] \t- Lista los archivos de un directorio. Si no se indica un directorio, se muestra la Tabla de Directorios y la Tabla de Nodos.\n\n");
			printf("\n\tinfo [path_archivo_yamafs] \t- Muestra toda la información del archivo, incluyendo tamaño, bloques, ubicación de los bloques, etc. \n\n");

	 	}

	 	if(string_starts_with(comandoConsola,"MKDIR")){
			comandoAceptado = true;

			// Si se ingreso un solo parametro
			if(countParametrosConsola(comandoConsola) == 1){

				// Convierto el parametro en minuscula
				string_to_lower(parametrosConsola[1]);

				// Si no existe el directorio en YamaFS
				if(!existeDirectorioYama(parametrosConsola[1], listaTablaDirectorios)){

					// Creo el Directorio en el YamaFS
					if(crearDirectorioYama(parametrosConsola[1], listaTablaDirectorios, infoLogger)){
						printf("Directorio %s creado\n", parametrosConsola[1]);
					}else{
						printf("[Error] No se pudo crear el directorio\n");
					}
				}else{
					printf("[Error] El directorio %s ya existe\n", parametrosConsola[1]);
				}

			}else{
				printf("[Error] Cantidad de parámetros incorrectos\n");
			}
	 	}

	 	if(string_starts_with(comandoConsola,"RM")){
			comandoAceptado = true;

			// Analizo la cantidad de parametros ingresados
			switch (countParametrosConsola(comandoConsola)) {
				case 1: // Para el caso de borrar archivos

					// Convierto el parametro en minuscula
					string_to_lower(parametrosConsola[1]);

					// Si existe el archivo
					if(existeArchivoYama(parametrosConsolaOriginal[1], listaTablaDirectorios) != -100 ){

						char respuesta;

						printf("Está seguro que desea eliminar el archivo %s? (S/N): ", parametrosConsolaOriginal[1]);
						fflush(stdin);
						scanf(" %c", &respuesta);

						// Si se confirmo la operación
						if(respuesta == 's' || respuesta == 'S'){

							// Eliminar el Archivo en el YamaFS
							if(eliminarArchivoYama(parametrosConsola[1], listaTablaDirectorios, listaTablaNodos, infoLogger)){
								printf("Archivo %s eliminado\n", parametrosConsola[1]);
							}else{
								printf("[Error] No se pudo eliminar el archivo\n");
							}
						}

					}else{
						printf("[Error] El archivo %s no existe.\n", parametrosConsolaOriginal[1]);
					}



					// TODO
					break;
				case 2: // Para el caso de borrar directorios

					// Convierto el parametro en minuscula
					string_to_lower(parametrosConsola[1]);
					string_to_lower(parametrosConsola[2]);

					// Si se desea borrar un directorio
					if(strcmp(parametrosConsola[1],"-d") == 0){

						// Si existe el directorio
						if(existeDirectorioYama(parametrosConsola[2], listaTablaDirectorios)){
							
							if(estaVacioDirectorioYama(parametrosConsola[2], listaTablaDirectorios)){

								// Eliminar el Directorio en el YamaFS
								if(eliminarDirectorioYama(parametrosConsola[2], listaTablaDirectorios, infoLogger)){
									printf("Directorio %s eliminado\n", parametrosConsola[2]);
								}else{
									printf("[Error] No se pudo eliminar el directorio\n");
								}
							}else{
								printf("[Error] No se pudo eliminar el directorio por no estar vacio.\n");
							}

						}else{
							printf("[Error] El directorio %s no existe en YamaFS.\n", parametrosConsola[2]);
						}
					}else{
						printf("[Error] Flag incorrecto\n");
					}
					break;

				case 4: // Para el caso de borrar bloques de nodos

					// Convierto el parametro en minuscula
					string_to_lower(parametrosConsola[1]);
					string_to_lower(parametrosConsola[2]);
					string_to_lower(parametrosConsola[3]);

					if(strcmp(parametrosConsola[1],"-b") == 0){
						// TODO
					}else{
						printf("[Error] Flag incorrecto\n");
					}
					break;
				default:
					printf("[Error] Cantidad de parámetros incorrectos\n");
					break;
			}

		}

	 	if(string_starts_with(comandoConsola,"LS")){
			comandoAceptado = true;

			// Analizo la cantidad de parametros ingresados
			switch (countParametrosConsola(comandoConsola)) {
				case 0:
					// Si no hay parametros, muestro toda la Tabla de Direcotrios y Nodos
					showContenidolista(listaTablaDirectorios);
					showContenidolistaNodos(listaTablaNodos);
					showContenidolistaNodosConectados(listaNodosConectados, colaNodosConectados);
					break;
				case 1: // Listo los archivos de un directorio

					// Convierto el parametro en minuscula
					string_to_lower(parametrosConsola[1]);

					// Si existe el directorio
					if(existeDirectorioYama(parametrosConsola[1], listaTablaDirectorios)){

						mostrarArchivosDirectorioFS(parametrosConsola[1], listaTablaDirectorios);
					}else{
						printf("[Error] No existe el directorio %s\n",parametrosConsola[1]);
					}


					break;
				default:
					printf("[Error] Cantidad de parámetros incorrectos\n");
					break;
			}
		}


	 	if(string_starts_with(comandoConsola,"FORMAT")){
			comandoAceptado = true;

			char respuesta;

			printf("Está seguro que desea formatear YamaFS? (S/N): ");
			fflush(stdin);
			scanf(" %c", &respuesta);

			// Si se confirmo la operación
			if(respuesta == 's' || respuesta == 'S'){

				if(formatearYama(listaTablaDirectorios, infoLogger)){
					printf("YamaFS formateado\n");
				}else{
					printf("[Error] No se pudo formatear YamaFS\n");
				}
			}else{
				printf("Operación cancelada.\n");
			}
		}


	 	if(string_starts_with(comandoConsola,"RENAME")){
			comandoAceptado = true;

			// Analizo la cantidad de parametros ingresados
			switch (countParametrosConsola(comandoConsola)) {
				case 2: // Para el caso de renombrar archivos o directorios

					// Convierto el parametro en minuscula
					string_to_lower(parametrosConsola[1]);
					string_to_lower(parametrosConsola[2]);

					bool existeDirectorio = false;
					bool existeArchivo = false;

					// Si la cantidad de nodos de ambos path de directorios son iguales
					if(cantidadDirectoriosPath(parametrosConsola[1]) != cantidadDirectoriosPath(parametrosConsola[2])){

						printf("La cantidad de directorios de ambos path no coinciden\n");
						break;
					}

					// Si existe el directorio
					if(existeDirectorioYama(parametrosConsola[1], listaTablaDirectorios)){

						existeDirectorio = true;

						// Renombrar el Directorio en el YamaFS
						renombrarDirectorioYama(parametrosConsola[1], parametrosConsola[2], listaTablaDirectorios, infoLogger);

						printf("Directorio %s renombrado por %s\n", parametrosConsola[1],parametrosConsola[2]);
					}

					// Si existe el Archivo en YamaFS
					if(existeArchivoYama(parametrosConsola[1], listaTablaDirectorios) != -100){

						existeArchivo = true;

						// Renombrar el Directorio en el YamaFS
						renombrarArchivoYama(parametrosConsola[1], parametrosConsola[2], listaTablaDirectorios, infoLogger);

						printf("Archivo %s renombrado por %s\n", parametrosConsola[1],parametrosConsola[2]);
					}

					// Si el Archivo/Directorio ingresado no existe
					if(!existeDirectorio && !existeArchivo){
						printf("[Error] %s no existe en YamaFS.\n", parametrosConsola[1]);
					}
					break;

				default:
					printf("[Error] Cantidad de parámetros incorrectos\n");
					break;
			}
		}

	 	if(string_starts_with(comandoConsola,"MV")){
			comandoAceptado = true;
			// TODO
		}

	 	if(string_starts_with(comandoConsola,"CAT")){
			comandoAceptado = true;


			// Analizo la cantidad de parametros ingresados
			switch (countParametrosConsola(comandoConsola)) {
				case 1:

					// Convierto el parametro en minuscula
					string_to_lower(parametrosConsola[1]);

					// Si existe el archivo
					if(existeArchivoYama(parametrosConsolaOriginal[1], listaTablaDirectorios) != -100 ){

						printf("Contenido del archivo %s:\n", parametrosConsolaOriginal[1]);
						mostrarArchivoYama(parametrosConsolaOriginal[1], listaTablaDirectorios, listaTablaNodos, listaNodosConectados, infoLogger);

					}else{
						printf("[Error] El archivo %s no existe.\n", parametrosConsolaOriginal[1]);
					}
					break;

				default:
					printf("[Error] Cantidad de parámetros incorrectos\n");
					break;
			}
		}

	 	if(string_starts_with(comandoConsola,"CPFROM")){
			comandoAceptado = true;

			// Analizo la cantidad de parametros ingresados
			switch (countParametrosConsola(comandoConsola)) {
				case 3:

					// Convierto el parametro en minuscula
					string_to_lower(parametrosConsola[1]);
					string_to_lower(parametrosConsola[2]);
					string_to_lower(parametrosConsola[3]);

					// Si existe el archivo local
					if(existeArchivo(parametrosConsolaOriginal[1])){

						// Si existe el directorioYama
						if(existeDirectorioYama(parametrosConsola[2], listaTablaDirectorios)){

							char* pathCompletoYama = string_new();
							char* pathAux = string_new();
							pathAux = obtenerArchivoPath(parametrosConsolaOriginal[1]);

							if(strcmp(parametrosConsolaOriginal[2], "/") == 0 ){
								pathCompletoYama = malloc(strlen(string_from_format("/%s", pathAux)) +1 );
								strcpy(pathCompletoYama, string_from_format("/%s", pathAux) );
							}else{
								pathCompletoYama = malloc(strlen(string_from_format("%s/%s", parametrosConsolaOriginal[2], pathAux)) +1 );
								strcpy(pathCompletoYama, string_from_format("%s/%s", parametrosConsolaOriginal[2], pathAux) );
							}
							free(pathAux);

							// Si no existe el archivo en YamaFS
							if(existeArchivoYama(pathCompletoYama, listaTablaDirectorios) == -100){



								log_info(infoLogger, "Iniciando CPFROM: origen: %s - destino: %s", parametrosConsolaOriginal[1], parametrosConsolaOriginal[2]);

								// Copiar el archivo a YamaFS
								if(copiarArchivoYama(parametrosConsolaOriginal[1], parametrosConsola[2], parametrosConsola[3],listaTablaDirectorios, colaNodos, listaTablaNodos, listaNodosConectados, colaNodosConectados, infoLogger)){
									printf("Archivo %s copiado a YamaFS en el directorio %s\n", parametrosConsolaOriginal[1],parametrosConsola[2]);
								}else{
									printf("No se pudo copiar el archivo %s a YamaFS en el directorio %s\n", parametrosConsolaOriginal[1],parametrosConsola[2]);
								}

							}else{
								printf("[Error] El archivo %s ya existe en YamaFS.\n", pathCompletoYama);
							}

							free(pathCompletoYama);


						}else{
							printf("[Error] El directorio %s no existe en YamaFS.\n", parametrosConsola[2]);
						}
					}else{
						printf("[Error] El archivo local %s no existe.\n", parametrosConsolaOriginal[1]);
					}
					break;

				default:
					printf("[Error] Cantidad de parámetros incorrectos\n");
					break;
			}

		}

	 	if(string_starts_with(comandoConsola,"CPTO")){
			comandoAceptado = true;
			// TODO
		}

	 	if(string_starts_with(comandoConsola,"CPBLOCK")){
			comandoAceptado = true;
			// TODO
		}

	 	if(string_starts_with(comandoConsola,"MD5")){
			comandoAceptado = true;

			// Analizo la cantidad de parametros ingresados
			switch (countParametrosConsola(comandoConsola)) {
				case 1:

					// Convierto el parametro en minuscula
					string_to_lower(parametrosConsola[1]);

					// Si existe el archivo local
					if(existeArchivoYama(parametrosConsolaOriginal[1], listaTablaDirectorios) != -100){

						printf("%s\n", obtenerMD5ArchivoYama(parametrosConsolaOriginal[1], listaTablaDirectorios, listaNodosConectados, infoLogger) );

					}else{
						printf("[Error] No existe el archivo indicado\n");
					}
					break;
				default:
					printf("[Error] Cantidad de parámetros incorrectos\n");
					break;	
			}
		}

	 	if(string_starts_with(comandoConsola,"INFO")){
			comandoAceptado = true;

			// Analizo la cantidad de parametros ingresados
			switch (countParametrosConsola(comandoConsola)) {
				case 1:

					// Convierto el parametro en minuscula
					string_to_lower(parametrosConsola[1]);

					// Si existe el archivo local
					if(existeArchivoYama(parametrosConsolaOriginal[1], listaTablaDirectorios) != -100){

						mostrarInfoArchivoYama(parametrosConsolaOriginal[1]);

					}else{
						printf("[Error] No existe el archivo indicado\n");
					}
					break;
				default:
					printf("[Error] Cantidad de parámetros incorrectos\n");
					break;	
			}
		}

    /* -------------------------------------------------------------------- */

	// Si el comando no es reconocido por la consola
	if(!comandoAceptado){
		printf("Comando no reconocido. Escriba 'help' para obtener ayuda.\n");
	}

	free(comandoConsola);
	free(parametrosConsola);
	free(parametrosConsolaOriginal);
}

/* ---------------------------------------- */

int main(int argc, char* argv[]){

	/* Creo la instancia del Archivo de Configuracion */
	cfg = config_create("config/config.cfg");

	/* Creo la instancia de Log */
	infoLogger = log_create("log/fs.log", "Fs", false, LOG_LEVEL_INFO);

	bool flagClean = false;

	/*************************************************
	 *
	 * Si se inicia el FS con el  parametro --clean
	 *
	 ************************************************/
    if(argc > 1){
    	// Si el Flag es --clean, inicializo FS
    	if(strcmp(argv[1],"--clean") == 0){
    	    log_info(infoLogger, "Iniciando FS con parámetro --clean" );
    	    flagClean = true;
    	}else{
            log_info(infoLogger, "Iniciando FS" );
    	}
    }else{
        log_info(infoLogger, "Iniciando FS" );
    }

    // Creo la lista de tabla de Directorios, Nodos y Archivos
    listaTablaDirectorios = list_create();
    listaTablaNodos = list_create();
    listaNodosConectados = list_create();
    colaNodos = queue_create();
    colaNodosConectados = queue_create();

    /* -------------------------------------------------------------------- */
    // Si existe directorios.dat, reestablezco sus datos
    /* -------------------------------------------------------------------- */

		if (existeArchivo("metadata/directorios.dat")){

			// Cargo la lista de tabla de Directorios
			if(!flagClean){
				cargarTablaDirectorio(listaTablaDirectorios, infoLogger);
			}else{
				// Si no existe el Root, lo inicializo
				crearDirectorioYama("/", listaTablaDirectorios, infoLogger);
			}
		}else{
			// Si no existe el Root, lo inicializo
			crearDirectorioYama("/", listaTablaDirectorios, infoLogger);
		}

    /* -------------------------------------------------------------------- */
    // Si existe nodos.bin, reestablezco sus datos
    /* -------------------------------------------------------------------- */

		// Cargo la lista de Nodos
		if(!flagClean){
			if (existeArchivo("metadata/nodos.bin")){
				cargarTablaNodos(listaTablaNodos, colaNodos, infoLogger);
			}
		}else{
			// Elimino el archivo nodos.bin
			unlink("metadata/nodos.bin");
		}

    /* -------------------------------------------------------------------- */
    // Vacio la Tabla de Archivos
    /* -------------------------------------------------------------------- */

		if(flagClean){
			if(vaciarTablaArchivos(infoLogger) != -1 ){
				log_info(infoLogger, "Iniciando la Tabla de Archivos" );
			}else{
				log_info(infoLogger, "No se pudo Inicializar la Tabla de Archivos" );
			}
		}

    /* -------------------------------------------------------------------- */
    // Vacio los Bitmaps
    /* -------------------------------------------------------------------- */

		if(flagClean){
			if(vaciarBitmaps(infoLogger) != -1 ){
				log_info(infoLogger, "Iniciando Bitmaps" );
			}else{
				log_info(infoLogger, "No se pudo Inicializar los Bitmaps" );
			}
		}

    /* -------------------------------------------------------------------- */


	int servidor, newfd;  // Escuchar sobre sock_fd, nuevas conexiones sobre new_fd
	struct sockaddr_in servidor_addr;    // información sobre mi dirección
	struct sockaddr_in cliente_addr; // información sobre la dirección del cliente
	int size,i,j;
	char buf[50];
	struct sigaction sa;
	int yes=1,fd_maximo,nbytes;
	fd_set master,temporales;
	FD_ZERO(&master);
	FD_ZERO(&temporales);

	// Variables para la Consola Interactiva
	const char* promptConsola = "#> "; // Prompt
	struct timeval to; // Timer

	// Mensaje de Bienvenida de la Consola
	printf("Bienvenido a la Consola de YamaFS 1.0\n");
	printf("Para obtener ayuda de los comandos permitidos, escriba 'help'.\n");

	log_info(infoLogger, "Inicio de la Consola Interactiva" );
	rl_callback_handler_install(promptConsola, (rl_vcpfunc_t*) &consola_interactiva);
	to.tv_sec = 0;
	to.tv_usec = 1;

	// String que captura la Consola con ReadLine
	char * comandoConsola;

	// Creo el Servidor para escuchar conexiones
	servidor=crearServidor(config_get_int_value(cfg,"FS_PUERTO"));
	log_info(infoLogger, "Escuchando conexiones" );
	
	FD_SET(servidor, &master);
	fd_maximo = servidor;

	// Defino las estrucutras para recibir de los procesos
	Encabezado encabezado;
	Paquete paquete;
	t_nodos* registroTablaNodos;
	t_nodo_socket registroNodoSocket;

	// Estructura para obtener los datos de los nodos
	t_nodos datos_nodo;
	Solicitud_Master nombre_archivo_procesar;

	int totalNodosConectados = 0;

	while (1) {
		// Si se salio de la consola
		if(quitConsola){
			log_info(infoLogger, "Cierre de la Consola Interactiva" );
			break;
		}

		temporales = master;
		if (select(fd_maximo + 1, &temporales, NULL, NULL, &to) == -1) {
			perror("select");
			exit(1);
		}

		// Para la Consola Interactiva
		rl_callback_read_char();

		for (i = 0; i <= fd_maximo; i++) {
			if (FD_ISSET(i, &temporales)) { // ¡¡tenemos datos!!
                            
				if (i == servidor) { // gestionar nuevas conexiones

					size = sizeof(cliente_addr);
					if ((newfd = accept(servidor, (struct sockaddr *)&cliente_addr, &size)) == -1) 
					{
						perror("accept");
						log_info(infoLogger, "Conexion fallada" );
					} else {
						FD_SET(newfd, &master); // añadir al conjunto maestro
						if (newfd > fd_maximo) {    // actualizar el máximo
							fd_maximo = newfd;
						}

						log_info(infoLogger, "Conexion nueva recibida" );

						encabezado=recibir_header(&newfd);

						// Si el mensaje proviene de DataNode
						if(encabezado.proceso == 'D'){
							switch(encabezado.cod_operacion){
								case INFO_NODO: // 19

									// Recibo los datos del Nodo
									paquete=recibir_payload(&newfd,&encabezado.tam_payload);
									datos_nodo = dsrlz_datosNodo(paquete.buffer);
									free(paquete.buffer);

									log_info(infoLogger,"Datos del Nodo %s recibido. Tamaño:%i Libre:%i", datos_nodo.nodo, datos_nodo.tamano, datos_nodo.libre);

									// ====================================================
									//  Cargo el Registro del Nodo
									// ====================================================

										registroTablaNodos = malloc( (3*sizeof(int)) + strlen(datos_nodo.nodo)+1 + strlen(datos_nodo.ip)+1);

										// obtener estos datos por mensajeria con datanode
										registroTablaNodos->tamano = datos_nodo.tamano;
										registroTablaNodos->libre = datos_nodo.libre;
										
										registroTablaNodos->nodo = malloc(strlen(datos_nodo.nodo)+1);
										strcpy( registroTablaNodos->nodo ,datos_nodo.nodo);
										registroTablaNodos->nodo[strlen(datos_nodo.nodo)] = '\0';

										registroTablaNodos->ip = malloc(strlen(datos_nodo.ip)+1);
										strcpy( registroTablaNodos->ip ,datos_nodo.ip);
										registroTablaNodos->ip[strlen(datos_nodo.ip)] = '\0';

										registroTablaNodos->port_worker = datos_nodo.port_worker;
										registroTablaNodos->port_datanode = datos_nodo.port_datanode;

									// ====================================================

									// cargo el Nodo a la Lista de Nodos
									addDataNode(listaNodosConectados, newfd, listaTablaNodos, colaNodos, colaNodosConectados, registroTablaNodos, infoLogger);


									totalNodosConectados++;


									// Si no existe el bitmaps del Nodo, lo creo
									if (!existeArchivo(string_from_format("metadata/bitmaps/%s.dat", registroTablaNodos->nodo))){

										// creo el BitMap del Nodo
										crearBitmapNodo(listaTablaNodos, registroTablaNodos, infoLogger);		
									}

									//free(registroTablaNodos); //DA Invalid read of size 4
									free(datos_nodo.nodo);
									free(datos_nodo.ip);
								break;
							}
						}

						// Si el mensaje proviene de Yama
						if(encabezado.proceso == 'Y'){
							switch(encabezado.cod_operacion){

								case INFO_NODO: // 19

									// Si no existen al menos 2 nodos conectados, rechazo la conexión
									if(totalNodosConectados < 2){

										// Le Envio a Yama la cantidad de 0 Nodos Conectados para que no se conecte
										paquete = crearHeader('F', 19, 0);
										send(newfd,paquete.buffer,paquete.tam_buffer,0);
										free(paquete.buffer);

										// Cierro la conexión con Yama
										close(newfd);
										FD_CLR(newfd, &master); // elimino al socket de la bolsa

										log_info(infoLogger,"Intento de conexión fallida de Yama por no estar estable el FS.");
										break;
									}

									log_info(infoLogger,"Solicitud de Yama de la información de los Nodos Conectados.");

									// Envio a Yama la cantidad de Nodos Conectados
									paquete = crearHeader('F', 19, list_size(listaTablaNodos));
									send(newfd,paquete.buffer,paquete.tam_buffer,0);
									free(paquete.buffer);

								    // Le envio a Yama los datos de todos los Nodo Conectados
									if(list_size(listaTablaNodos) > 0){

									    void _each_elemento_(t_nodos* registroTablaNodos)
										{
									    	// Serializo el registro para poder enviarlo
											Paquete paquete=srlz_datosNodo(registroTablaNodos);
											send(newfd,paquete.buffer,paquete.tam_buffer,0);
											free(paquete.buffer);
										}
									    list_iterate(listaTablaNodos, (void*)_each_elemento_);
									}
									log_info(infoLogger,"Envio a Yama la información de los Nodos Conectados.");
								break;
							}
						}


					}
				} else { // Atiendo las conexiones ya establecidas, es decir, clientes

					log_info(infoLogger, "Conexion recibida");
					encabezado=recibir_header(&i);

					if(encabezado.proceso == 'Y'){
						switch(encabezado.cod_operacion){

							case INICIAR_TRANSFORMACION_ARCHIVO: // 20

								log_info(infoLogger,"Solicitud de Yama de Bloques de un Archivo.");

								// Recibo el nombre del archivo
								paquete=recibir_payload(&i,&encabezado.tam_payload);
								nombre_archivo_procesar = dsrlz_envio_transformacion(paquete.buffer);
								free(paquete.buffer);

								log_info(infoLogger,"El nombre del archivo %s fue recibido de Yama.", nombre_archivo_procesar.archivo);


								// Obtengo todos los bloques de un archivo
								t_list* listaBloques = list_create();
								listaBloques = obtenerDatosBloquesArchivo(nombre_archivo_procesar.archivo, true,listaTablaDirectorios, listaTablaNodos, infoLogger);

								// Envio a Yama la cantidad de bloques del archivo
								paquete = crearHeader('F', 20, list_size(listaBloques));
								send(i,paquete.buffer,paquete.tam_buffer,0);
								free(paquete.buffer);

								log_info(infoLogger,"Envio de cantidad de Bloques del archivo %s a Yama. Total Bloques: %d.", nombre_archivo_procesar.archivo, list_size(listaBloques));

								if(list_size(listaBloques) > 0){

									// Envio todos los bloques del archivo a Yama
									void _each_elemento_(t_bloques_archivos* solicitud)
									{
										// Serializo el registro para poder enviarlo
										 //printf("hola\n");
										  paquete = srlz_bloquenodo(solicitud);
										//printf("hola\n");
										send(i,paquete.buffer,paquete.tam_buffer,0);
									//	printf("hola\n");
									free(paquete.buffer);
									}
									list_iterate(listaBloques, (void*)_each_elemento_);
								}

								log_info(infoLogger,"Envio finalizado a Yama de todos los bloques. Total Bloques: %d", list_size(listaBloques));

								// Libero recursos
								list_destroy(listaBloques);
								free(nombre_archivo_procesar.archivo);
							break;
						}

					}

				}
			}

		}
	}

	// Para Cerrar la Consola Interactiva
	rl_callback_handler_remove();
	close(servidor);

	/* ---------------------------------------- */
	/*  Libero Memoria de Log y Config			*/
	/* ---------------------------------------- */
	log_destroy(infoLogger);
	config_destroy(cfg);
	list_destroy(listaTablaDirectorios);
	list_destroy(listaTablaNodos);
	list_destroy(listaNodosConectados);
	queue_destroy(colaNodos);
	queue_destroy(colaNodosConectados);

	free(registroTablaNodos);

	close(servidor);

    return 0;
}
