#include "funciones.h"

//*********************** SERIALIZADO Y DESERIALIZADO ********************************//

void inicializarEjemploSolicitud(t_solicitud_transformacion* solicitud_transformacion){
    //Ejemplo de Solicitud de Transformacion

	solicitud_transformacion->nodo = malloc (strlen("nodo1"));
	solicitud_transformacion->worker_ip = malloc (strlen("10.0.0.1"));
	//solicitud_transformacion->worker_port = malloc (sizeof(int));
	solicitud_transformacion->archivo_tmp = malloc (strlen("archivo_ejemplo.csv"));

    strcpy( solicitud_transformacion->nodo ,"nodo1");
    strcpy( solicitud_transformacion->worker_ip, "10.0.0.1");
    solicitud_transformacion->worker_port= 8081;
    solicitud_transformacion->bloque = 1;
    solicitud_transformacion->bytes_ocupados = 12345;
    strcpy( solicitud_transformacion->archivo_tmp, "archivo_ejemplo.csv");
    return;
}

void inicializarEjemploReduccionG(t_solicitud_reduccion_global* solicitud_transformacion){
    //Ejemplo de Solicitud de Transformacion

	solicitud_transformacion->nodo = malloc (strlen("nodo1"));
	solicitud_transformacion->worker_ip = malloc (strlen("10.0.0.1"));
	//solicitud_transformacion->worker_port = malloc (sizeof(int));
	solicitud_transformacion->archivo_tmp_reduccion_local = malloc (strlen("archivo_ejemplo.csv"));
    solicitud_transformacion->archivo_reduccion_global=malloc(strlen("archivo_reduccionG.csv"));

    strcpy( solicitud_transformacion->nodo ,"nodo1");
    strcpy( solicitud_transformacion->worker_ip, "10.0.0.1");
    solicitud_transformacion->worker_port= 8081;
    solicitud_transformacion->encargado = 1;
    strcpy(solicitud_transformacion->archivo_reduccion_global,"archivo_reduccionG.csv");
    strcpy( solicitud_transformacion->archivo_tmp_reduccion_local, "archivo_ejemplo.csv");
    return;
}
/*
void inicializarEjemploReduccionL(t_solicitud_reduccion_local* solicitud_transformacion){
    //Ejemplo de Solicitud de Transformacion

	solicitud_transformacion->nodo = malloc (strlen("nodo1"));
	solicitud_transformacion->worker_ip = malloc (strlen("10.0.0.1"));
	//solicitud_transformacion->worker_port = malloc (sizeof(int));
	solicitud_transformacion->archivo_tmp_reduccion_local = malloc (strlen("archivo_reduccionL.csv"));
    solicitud_transformacion->archivo_tmp_transformacion=malloc(strlen("archivo_transformacion.csv"));

    strcpy( solicitud_transformacion->nodo ,"nodo1");
    strcpy( solicitud_transformacion->worker_ip, "10.0.0.1");
    solicitud_transformacion->worker_port= 8081;
    strcpy(solicitud_transformacion->archivo_tmp_transformacion,"archivo_transformacion.csv");
    strcpy( solicitud_transformacion->archivo_tmp_reduccion_local, "archivo_reduccionL.csv");
    return;
}*/

void inicializarEjemploAlmacenadoFinal(t_solicitud_almacenado_final* solicitud_transformacion){
    //Ejemplo de Solicitud de Transformacion

	solicitud_transformacion->nodo = malloc (strlen("nodo1"));
	solicitud_transformacion->worker_ip = malloc (strlen("10.0.0.1"));
	solicitud_transformacion->archivo_reduccion_global = malloc (strlen("archivo_reduccion.csv"));

    strcpy( solicitud_transformacion->nodo ,"nodo2");
    strcpy( solicitud_transformacion->worker_ip, "10.0.0.1");
    solicitud_transformacion->worker_port= 8090;
    strcpy( solicitud_transformacion->archivo_reduccion_global, "archivo_reduccion.csv");
    return;
}

void inicializarEjemploBloque(t_bloques_archivos* solicitud){
    //Ejemplo de Solicitud de Transformacion

	solicitud->copia1.nodo = malloc (strlen("nodo1"));
	solicitud->copia2.nodo = malloc (strlen("nodo2"));


    strcpy( solicitud->copia1.nodo ,"nodo1");
    strcpy(solicitud->copia2.nodo, "nodo2");
    solicitud->bloque= 2;
    solicitud->bytes_ocupados = 190;
    solicitud->copia1.bloque=8;
    solicitud->copia2.bloque=9;

    return;
}

Paquete crearHeader(char proceso, int cod_operacion, int tamPayload){

	int posicion;
	int sizeBuffer = 0;
	Paquete paquete;
	sizeBuffer = (sizeof(int) * 2) + sizeof(char);
	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );

	//memcpy(dir de dónde copiaré, dir de lo que copiaré, tamaño de lo que copiaré)
	memcpy(paquete.buffer                                           ,&proceso               ,sizeof(char));
	memcpy(paquete.buffer + (posicion  = sizeof(proceso))			,&cod_operacion			,sizeof(int));
	memcpy(paquete.buffer + (posicion += sizeof(cod_operacion))		,&tamPayload			,sizeof(int));	

	return paquete;
}

Paquete srlz_archivo(char proceso, int codigoOperacion, char* nombreArchivo){

	int posicion = 0;//int para ir guiando desde donde se copia
	int sizeBuffer = 0;
	int tamString = 0;
	int tamPayload = 0;

	FILE *fs = fopen(nombreArchivo, "r");

	struct stat s;
	stat(nombreArchivo, &s);

	char contenidoArchivo[s.st_size]; 
	bzero(contenidoArchivo, s.st_size); 
	int fs_block_sz; 

	fs_block_sz = fread(contenidoArchivo, sizeof(char), s.st_size, fs);

	Paquete paquete;
	sizeBuffer =sizeof(char)+
			(sizeof(int)*3) // 2 son para el codigo y el "tam_buffer"; y otro para tamString
			+ s.st_size; // tamano del archivo

	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - (sizeof(int)*2) - sizeof(char);

	memcpy(paquete.buffer                                                   ,&(proceso)                     ,sizeof(char));
	memcpy(paquete.buffer + (posicion=sizeof(char))							,&(codigoOperacion)				,sizeof(int)); 
	memcpy(paquete.buffer + (posicion += sizeof(int))						,&(tamPayload)					,sizeof(int));
	memcpy(paquete.buffer + (posicion += sizeof(int))						,&(s.st_size)					,sizeof(int));
	memcpy(paquete.buffer + (posicion += sizeof(int))						,contenidoArchivo				,s.st_size); 

	fclose(fs);

	return paquete;
}

Paquete srlz_solicitud_pedido_bloque(char proceso, int codigoOperacion, int bloque, int tamanoBloque){

	int posicion = 0;//int para ir guiando desde donde se copia
	int sizeBuffer = 0;
	int tamString = 0;
	int tamPayload = 0;

	Paquete paquete;
	sizeBuffer =sizeof(char)+
			(sizeof(int)*4); // tamano del archivo

	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - (sizeof(int)*2) - sizeof(char);

	memcpy(paquete.buffer                                                   ,&(proceso)                     ,sizeof(char));
	memcpy(paquete.buffer + (posicion=sizeof(char))							,&(codigoOperacion)				,sizeof(int)); 
	memcpy(paquete.buffer + (posicion += sizeof(int))						,&(tamPayload)					,sizeof(int));
	memcpy(paquete.buffer + (posicion += sizeof(int))						,&(bloque)						,sizeof(int));
	memcpy(paquete.buffer + (posicion += sizeof(int))						,&(tamanoBloque)				,sizeof(int)); 

	return paquete;
}

Paquete srlz_bloque_contenido(char proceso, int codigoOperacion, char* contenidoBloque, int tamanoBloque, int bloque){

	int posicion = 0;//int para ir guiando desde donde se copia
	int sizeBuffer = 0;
	int tamString = 0;
	int tamPayload = 0;

	Paquete paquete;
	sizeBuffer =sizeof(char)+
			(sizeof(int)*4) // 2 son para el codigo y el "tam_buffer"; y otro para tamString
			+ tamanoBloque; // tamano del archivo

	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - (sizeof(int)*2) - sizeof(char);

	memcpy(paquete.buffer                                                   ,&(proceso)                     ,sizeof(char));
	memcpy(paquete.buffer + (posicion=sizeof(char))							,&(codigoOperacion)				,sizeof(int)); 
	memcpy(paquete.buffer + (posicion += sizeof(int))						,&(tamPayload)					,sizeof(int));
	memcpy(paquete.buffer + (posicion += sizeof(int))						,&(tamanoBloque)				,sizeof(int));
	memcpy(paquete.buffer + (posicion += sizeof(int))						,contenidoBloque				,tamanoBloque); 
	memcpy(paquete.buffer + (posicion += tamanoBloque)						,&(bloque)						,sizeof(int)); 

	return paquete;
}


Paquete srlz_bloque_archivo(char proceso, int codigoOperacion, char* nombreArchivo, int bytesDesde, int bytesHasta, int bloqueDestino){

	int posicion = 0;//int para ir guiando desde donde se copia
	int sizeBuffer = 0;
	int tamString = 0;
	int tamPayload = 0;
	int tamanoBloque = bytesHasta - bytesDesde + 1;

	FILE *fs = fopen(nombreArchivo, "r");

	char contenidoArchivo[tamanoBloque]; 
	bzero(contenidoArchivo, tamanoBloque); 
	int fs_block_sz; 

	// Me posiciono al comienzo del bloque a copiar
	fseek( fs, bytesDesde, SEEK_SET );

	// Leo el bloque completo
	fs_block_sz = fread(contenidoArchivo, sizeof(char), tamanoBloque, fs);

	Paquete paquete;
	sizeBuffer =sizeof(char)+
			(sizeof(int)*4) // 2 son para el codigo y el "tam_buffer"; y otro para tamString
			+ tamanoBloque	// tamano del archivo
			+ sizeof(int)
			+ strlen(nombreArchivo);
	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - (sizeof(int)*2) - sizeof(char);

	memcpy(paquete.buffer                                                   ,&(proceso)                     ,sizeof(char));
	memcpy(paquete.buffer + (posicion=sizeof(char))							,&(codigoOperacion)				,sizeof(int));
	memcpy(paquete.buffer + (posicion += sizeof(int))						,&(tamPayload)					,sizeof(int));

	tamString = strlen(nombreArchivo);
	memcpy(paquete.buffer + (posicion += sizeof(tamPayload))				,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int) )						,nombreArchivo					,strlen(nombreArchivo)); //se copia el nombre

	memcpy(paquete.buffer + (posicion += strlen(nombreArchivo))				,&(tamanoBloque)				,sizeof(int));
	memcpy(paquete.buffer + (posicion += sizeof(int))						,contenidoArchivo				,tamanoBloque);
	memcpy(paquete.buffer + (posicion += tamanoBloque)						,&(bloqueDestino)				,sizeof(int));

	fclose(fs);
	//free(contenidoArchivo);

	return paquete;
}

Paquete srlz_solicitudTransformacion(t_solicitud_transformacion* solicitud)
{
	int codigo = INICIAR_TRANSFORMACION_ARCHIVO;
	char proceso='Y';
	int posicion = 0;//int para ir guiando desde donde se copia
	int sizeBuffer = 0;
	int tamString = 0;
	int tamPayload = 0;


	Paquete paquete;
	sizeBuffer =sizeof(char)+
			(sizeof(int)*8) // 2 son para el codigo y el "tam_buffer"; otros 3 para bloque,worker_port y bytes_ocupados; y los otros 3 para tamString
			+ strlen(solicitud->nodo)
			+ strlen(solicitud->worker_ip)
			+ strlen(solicitud->archivo_tmp);

	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - (sizeof(int)*2) - sizeof(char);

	//memcpy(dir de dónde copiaré, dir de lo que copiaré, tamaño de lo que copiaré)
	memcpy(paquete.buffer                                                   ,&(proceso)                     ,sizeof(char));
	memcpy(paquete.buffer + (posicion=sizeof(proceso))						,&(codigo)						,sizeof(int)); //copio el codigo
	memcpy(paquete.buffer + (posicion += sizeof(codigo))					,&(tamPayload)					,sizeof(tamPayload)); //se copia el tamaño del buffer
	memcpy(paquete.buffer + (posicion += sizeof(tamPayload))		,&(solicitud->bytes_ocupados)			,sizeof(solicitud->bytes_ocupados)); //se copia el campo bytes ocupados
	memcpy(paquete.buffer + (posicion += sizeof(solicitud->bytes_ocupados))	,&(solicitud->bloque)			,sizeof(solicitud->bloque)); //se copia el campo bloque

	tamString = strlen(solicitud->worker_ip);
	memcpy(paquete.buffer + (posicion += sizeof(solicitud->bloque))			,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int))						,solicitud->worker_ip			,strlen(solicitud->worker_ip)); //se copia el campo ip


	tamString = strlen(solicitud->nodo);
	memcpy(paquete.buffer + (posicion += strlen(solicitud->worker_ip))		,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int) )						,solicitud->nodo				,strlen(solicitud->nodo)); //se copia el campo nodo
	tamString = strlen(solicitud->archivo_tmp);
	memcpy(paquete.buffer + (posicion += strlen(solicitud->nodo))			,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int) )						,solicitud->archivo_tmp			,strlen(solicitud->archivo_tmp)); //se copia el campo archivo tmp
	//tamString = strlen(solicitud->worker_port);
	//memcpy(paquete.buffer + (posicion += strlen(solicitud->archivo_tmp))	,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += strlen(solicitud->archivo_tmp) )						,&(solicitud->worker_port)			,sizeof(solicitud->worker_port)); //se copia el campo port

	return paquete;
}

Paquete srlz_solicitudReduccionGlobal(t_solicitud_reduccion_global* solicitud)
{
	int codigo = INICIAR_REDUCCION_ARCHIVO_GLOBAL;
	char proceso='Y';
	int posicion = 0;//int para ir guiando desde donde se copia
	int sizeBuffer = 0;
	int tamString = 0;
	int tamPayload = 0;


	Paquete paquete;
	sizeBuffer =sizeof(char)+
			(sizeof(int)*7) // 2 son para el codigo y el "tam_buffer"; otro para worker port; y los otros 4 para tamString
			+ strlen(solicitud->nodo)
			+ strlen(solicitud->worker_ip)
			+ strlen(solicitud->archivo_tmp_reduccion_local)
			+ strlen(solicitud->archivo_reduccion_global)
			+ sizeof(bool);//Para worker encargado

	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - (sizeof(int)*2) - sizeof(char);

	//memcpy(dir de dónde copiaré, dir de lo que copiaré, tamaño de lo que copiaré)
	memcpy(paquete.buffer                                                   ,&(proceso)                     ,sizeof(char));
	memcpy(paquete.buffer + (posicion  = sizeof(proceso))					,&(codigo)						,sizeof(int)); //copio el codigo
	memcpy(paquete.buffer + (posicion += sizeof(codigo))					,&(tamPayload)					,sizeof(tamPayload)); //se copia el tamaño del buffer
	memcpy(paquete.buffer + (posicion += sizeof(tamPayload))				,&(solicitud->worker_port)		,sizeof(solicitud->worker_port)); //se copia el campo bytes ocupados
	memcpy(paquete.buffer + (posicion += sizeof(solicitud->worker_port))	,&(solicitud->encargado)			,sizeof(solicitud->encargado)); //se copia el campo bloque

	tamString = strlen(solicitud->worker_ip);
	memcpy(paquete.buffer + (posicion += sizeof(solicitud->encargado))			,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int))						,solicitud->worker_ip			,strlen(solicitud->worker_ip)); //se copia el campo ip


	tamString = strlen(solicitud->nodo);
	memcpy(paquete.buffer + (posicion += strlen(solicitud->worker_ip))		,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int) )						,solicitud->nodo				,strlen(solicitud->nodo)); //se copia el campo nodo

	tamString = strlen(solicitud->archivo_tmp_reduccion_local);
	memcpy(paquete.buffer + (posicion += strlen(solicitud->nodo))			,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int) )						,solicitud->archivo_tmp_reduccion_local			,strlen(solicitud->archivo_tmp_reduccion_local)); //se copia el campo archivo tmp

	tamString = strlen(solicitud->archivo_reduccion_global);
	memcpy(paquete.buffer + (posicion += strlen(solicitud->archivo_tmp_reduccion_local))	,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int) )						,solicitud->archivo_reduccion_global			,strlen(solicitud->archivo_reduccion_global)); //se copia el campo port

	return paquete;
}

Paquete srlz_solicitudReduccionLocal(t_solicitud_reduccion_local* solicitud)
{
	int codigo = INICIAR_REDUCCION_ARCHIVO_LOCAL;
	char proceso='Y';
	int posicion = 0;//int para ir guiando desde donde se copia
	int sizeBuffer = 0;
	int tamString = 0;
	int tamPayload = 0;
	int tamList = 0;

	lista_temporales_transformacion* tempList;
	tempList=malloc(sizeof(lista_temporales_transformacion));
	tempList= solicitud->archivo_tmp_transformacion;


	/*do{

		tamList += (sizeof(int) + strlen(tempList->archivo_temporal_transformacion));

	}while((tempList = tempList->sig));*/
	while(tempList != NULL){

		tamList += (sizeof(int) + strlen(tempList->archivo_temporal_transformacion));

		tempList=tempList->sig;


	}


	tempList = solicitud->archivo_tmp_transformacion;

	Paquete paquete;
	sizeBuffer =sizeof(char)+
			(sizeof(int)*6) // 2 son para el codigo y el "tam_buffer"; otros 2 para bloque y bytes_ocupados; y los otros 4 para tamString
			+ strlen(solicitud->nodo)
			+ strlen(solicitud->worker_ip)
			+ strlen(solicitud->archivo_tmp_reduccion_local)
			+ tamList
			+ sizeof(int);// Uso 0 para decir que no existen mas archivos temporales de transformacion

	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - (sizeof(int)*2) - sizeof(char);


	//memcpy(dir de dónde copiaré, dir de lo que copiaré, tamaño de lo que copiaré)
	memcpy(paquete.buffer                                                   ,&(proceso)                     ,sizeof(char));
	memcpy(paquete.buffer + (posicion  = sizeof(proceso))					,&(codigo)						,sizeof(int)); //copio el codigo
	memcpy(paquete.buffer + (posicion += sizeof(codigo))					,&(tamPayload)					,sizeof(tamPayload)); //se copia el tamaño del buffer
	memcpy(paquete.buffer + (posicion += sizeof(tamPayload))				,&(solicitud->worker_port)		,sizeof(solicitud->worker_port)); //se copia el campo bytes ocupados
	//memcpy(paquete.buffer + (posicion += sizeof(solicitud->worker_port))	,&(solicitud->encargado)			,sizeof(solicitud->encargado)); //se copia el campo bloque

	tamString = strlen(solicitud->worker_ip);
	memcpy(paquete.buffer + (posicion += sizeof(solicitud->worker_port))			,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int))						,solicitud->worker_ip			,strlen(solicitud->worker_ip)); //se copia el campo ip


	tamString = strlen(solicitud->nodo);
	memcpy(paquete.buffer + (posicion += strlen(solicitud->worker_ip))		,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int) )						,solicitud->nodo				,strlen(solicitud->nodo)); //se copia el campo nodo

	tamString = strlen(solicitud->archivo_tmp_reduccion_local);
	memcpy(paquete.buffer + (posicion += strlen(solicitud->nodo))			,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int) )						,solicitud->archivo_tmp_reduccion_local			,strlen(solicitud->archivo_tmp_reduccion_local)); //se copia el campo archivo tmp

	posicion += strlen(solicitud->archivo_tmp_reduccion_local);

	do {
		tamString = strlen(tempList->archivo_temporal_transformacion);
		memcpy(paquete.buffer + posicion	,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
		memcpy(paquete.buffer + (posicion += sizeof(int) )					,tempList->archivo_temporal_transformacion			,strlen(tempList->archivo_temporal_transformacion)); //se copia el campo port
		posicion += strlen(tempList->archivo_temporal_transformacion);
	} while ((tempList = tempList->sig));

	tamString = 0;
	memcpy(paquete.buffer + posicion		,&tamString						,sizeof(int));

/*	tamString = strlen(solicitud->archivo_tmp_transformacion);
	memcpy(paquete.buffer + (posicion += strlen(solicitud->archivo_tmp_reduccion_local))	,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int) )						,solicitud->archivo_tmp_transformacion			,strlen(solicitud->archivo_tmp_transformacion)); //se copia el campo port
//*/

	return paquete;
}

Paquete srlz_solicitudAlmacenadoFinal(t_solicitud_almacenado_final* solicitud)
{
	int codigo = ALMACENADO_FINAL;
	char proceso='Y';
	int posicion = 0;//int para ir guiando desde donde se copia
	int sizeBuffer = 0;
	int tamString = 0;
	int tamPayload = 0;

	Paquete paquete;
	sizeBuffer =sizeof(char)+
			(sizeof(int)*6) // 2 son para el codigo y el "tam_buffer"; otros 3 para bloque,worker_port y bytes_ocupados; y los otros 3 para tamString
			+ strlen(solicitud->nodo)
			+ strlen(solicitud->worker_ip)
			+ strlen(solicitud->archivo_reduccion_global);

	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - (sizeof(int)*2) - sizeof(char);

	//memcpy(dir de dónde copiaré, dir de lo que copiaré, tamaño de lo que copiaré)
	memcpy(paquete.buffer                                                   ,&(proceso)                     ,sizeof(char));
	memcpy(paquete.buffer + (posicion=sizeof(proceso))						,&(codigo)						,sizeof(int)); //copio el codigo
	memcpy(paquete.buffer + (posicion += sizeof(codigo))					,&(tamPayload)					,sizeof(tamPayload)); //se copia el tamaño del buffer
	memcpy(paquete.buffer + (posicion += sizeof(tamPayload))				,&(solicitud->worker_port)		,sizeof(solicitud->worker_port)); //se copia el campo bytes ocupados

	tamString = strlen(solicitud->worker_ip);
	memcpy(paquete.buffer + (posicion += sizeof(solicitud->worker_port))			,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int))						,solicitud->worker_ip			,strlen(solicitud->worker_ip)); //se copia el campo ip


	tamString = strlen(solicitud->nodo);
	memcpy(paquete.buffer + (posicion += strlen(solicitud->worker_ip))		,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int) )						,solicitud->nodo				,strlen(solicitud->nodo)); //se copia el campo nodo

	tamString = strlen(solicitud->archivo_reduccion_global);
	memcpy(paquete.buffer + (posicion += strlen(solicitud->nodo))			,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int) )						,solicitud->archivo_reduccion_global			,strlen(solicitud->archivo_reduccion_global)); //se copia el campo archivo tmp
	//tamString = strlen(solicitud->worker_port);
	//memcpy(paquete.buffer + (posicion += strlen(solicitud->archivo_tmp))	,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	//memcpy(paquete.buffer + (posicion += strlen(solicitud->archivo_tmp) )						,&(solicitud->worker_port)			,sizeof(solicitud->worker_port)); //se copia el campo port

	return paquete;
}

Paquete srlz_bloquenodo(t_bloques_archivos* solicitud)
{
	int codigo = INICIAR_TRANSFORMACION_ARCHIVO;
	char proceso='F';
	int posicion = 0;//int para ir guiando desde donde se copia
	int sizeBuffer = 0;
	int tamString = 0;
	int tamPayload=0;

	Paquete paquete;
	sizeBuffer =sizeof(char)+
			(sizeof(int)*12) // 2 son para el codigo y el "tam_buffer"; otros 2 para bloque y bytes_ocupados; y los otros 4 para tamString
			+ strlen((solicitud->copia1).nodo)
			+strlen((solicitud->copia2).nodo)
	+strlen((solicitud->copia2).ip) 			+strlen((solicitud->copia2).ip);

	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - (sizeof(int)*2) - sizeof(char);

	//memcpy(dir de dónde copiaré, dir de lo que copiaré, tamaño de lo que copiaré)
	memcpy(paquete.buffer                                                   ,&(proceso)                     ,sizeof(char));
	memcpy(paquete.buffer + (posicion  = sizeof(proceso))					,&(codigo)						,sizeof(int)); //copio el codigo
	memcpy(paquete.buffer + (posicion += sizeof(codigo))					,&(tamPayload)			,sizeof(tamPayload)); //se copia el tamaño del buffer
	memcpy(paquete.buffer + (posicion += sizeof(tamPayload))		,&(solicitud->bloque)	,sizeof(solicitud->bloque)); //se copia el campo bytes ocupados
	memcpy(paquete.buffer + (posicion += sizeof(solicitud->bloque))	,&(solicitud->bytes_ocupados)			,sizeof(solicitud->bytes_ocupados)); //se copia el campo bloque

	tamString = strlen(solicitud->copia1.nodo);
	memcpy(paquete.buffer + (posicion += sizeof(solicitud->bytes_ocupados))			,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int))						,solicitud->copia1.nodo			,strlen(solicitud->copia1.nodo)); //se copia el campo ip


	tamString = strlen(solicitud->copia2.nodo);
	memcpy(paquete.buffer + (posicion += strlen(solicitud->copia1.nodo))		,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int) )						,solicitud->copia2.nodo				,strlen(solicitud->copia2.nodo)); //se copia el campo nodo


	memcpy(paquete.buffer + (posicion += strlen(solicitud->copia2.nodo))			,&(solicitud->copia1.bloque)					,sizeof(solicitud->copia1.bloque) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int) )						,&(solicitud->copia2.bloque)			,sizeof(solicitud->copia2.bloque)); //se copia el campo archivo tmp

	memcpy(paquete.buffer + (posicion += sizeof(solicitud->copia2.bloque))		,&(solicitud->copia1.port)	,sizeof(int)); //se copia el campo bytes ocupados
	memcpy(paquete.buffer + (posicion += sizeof(int))	,&(solicitud->copia2.port)			,sizeof(int)); //se copia el campo bloque

	tamString = strlen(solicitud->copia1.ip);
	memcpy(paquete.buffer + (posicion += sizeof(int))			,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int))						,solicitud->copia1.ip			,strlen(solicitud->copia1.ip)); //se copia el campo ip

	tamString = strlen(solicitud->copia2.ip);
	memcpy(paquete.buffer + (posicion += strlen(solicitud->copia1.ip))			,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int))						,solicitud->copia2.ip			,strlen(solicitud->copia2.ip)); //se copia el campo ip


	return paquete;
}

Paquete srlz_tablaDirectorio(t_directorios* solicitud){

	int posicion = 0;//int para ir guiando desde donde se copia
	int sizeBuffer = 0;
	int tamString = 0;

	Paquete paquete;
	sizeBuffer = (sizeof(int)*3) + strlen(solicitud->nombre);

	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );

	//memcpy(dir de dónde copiaré, dir de lo que copiaré, tamaño de lo que copiaré)
	memcpy(paquete.buffer													,&(solicitud->index)		,sizeof(solicitud->index));
	memcpy(paquete.buffer + (posicion = sizeof(solicitud->index))			,&(solicitud->padre)		,sizeof(solicitud->padre));

	tamString = strlen(solicitud->nombre);
	memcpy(paquete.buffer + (posicion += sizeof(solicitud->padre))			,&(tamString)				,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int))						,solicitud->nombre			,strlen(solicitud->nombre));

	return paquete;
}

Paquete srlz_delegacionTransformacion(t_delegacion_transformacion* delegacion){
	int codigo = INICIAR_TRANSFORMACION_ARCHIVO;
	char proceso='M';
	int posicion = 0;//int para ir guiando desde donde se copia
	int sizeBuffer = 0;
	int tamString = 0;
	int tamPayload = 0;

	Paquete paquete;
	sizeBuffer =sizeof(char)+
			(sizeof(int)*5) // 2 son para el codigo y el "tam_buffer"; otros 2 para bloque y bytes_ocupados; y los otros 2 para tamString
			+ strlen(delegacion->archivo_destino);

	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - (sizeof(int)*2) - sizeof(char);

	//memcpy(dir de dónde copiaré, dir de lo que copiaré, tamaño de lo que copiaré)
	memcpy(paquete.buffer                                                   ,&(proceso)                     ,sizeof(char));
	memcpy(paquete.buffer + (posicion=sizeof(proceso))						,&(codigo)						,sizeof(int)); //copio el codigo
	memcpy(paquete.buffer + (posicion += sizeof(codigo))					,&(tamPayload)					,sizeof(tamPayload)); //se copia el tamaño del buffer
	memcpy(paquete.buffer + (posicion += sizeof(tamPayload))				,&(delegacion->bloque)			,sizeof(delegacion->bloque)); //se copia el campo bytes ocupados
	memcpy(paquete.buffer + (posicion += sizeof(delegacion->bloque))		,&(delegacion->num_bytes)		,sizeof(delegacion->num_bytes)); //se copia el campo bloque

	tamString = strlen(delegacion->archivo_destino);
	memcpy(paquete.buffer + (posicion += sizeof(delegacion->num_bytes))		,&(tamString)					,sizeof(tamString) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(tamString))					,delegacion->archivo_destino	,strlen(delegacion->archivo_destino)); //se copia el campo ip

	return paquete;

}
/*			*/
Paquete srlz_delegacionReduccionLocal(t_delegacion_reduccion_local* delegacion){//TODO	Probar
	int codigo = INICIAR_REDUCCION_ARCHIVO_LOCAL;
	char proceso='M';
	int posicion = 0;//int para ir guiando desde donde se copia
	int sizeBuffer = 0;
	int tamString = 0;
	int tamPayload = 0;
	int tamList = 0;
	lista_temporales_transformacion* tempList = delegacion->lista_archivo_argumento;

	do{
		tamList += (sizeof(int) + strlen(tempList->archivo_temporal_transformacion));
	}while((tempList = tempList->sig));
	tempList = delegacion->lista_archivo_argumento;

	Paquete paquete;
	sizeBuffer =sizeof(char)+
			(sizeof(int)*4) // 2 son para el codigo y el "tam_buffer"; y los otros 3 para tamString
			+ strlen(delegacion->archivo_destino)
			+ tamList;

	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - (sizeof(int)*2) - sizeof(char);

	//memcpy(dir de dónde copiaré, dir de lo que copiaré, tamaño de lo que copiaré)
	memcpy(paquete.buffer                                                   ,&(proceso)                     ,sizeof(char));
	memcpy(paquete.buffer + (posicion=sizeof(proceso))						,&(codigo)						,sizeof(int)); //copio el codigo
	memcpy(paquete.buffer + (posicion += sizeof(codigo))					,&(tamPayload)					,sizeof(paquete.tam_buffer)); //se copia el tamaño del buffer

	tamString = strlen(delegacion->archivo_destino);
	memcpy(paquete.buffer + (posicion += sizeof(tamPayload))				,&(tamString)				,sizeof(tamString) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(tamString) )				,delegacion->archivo_destino	,strlen(delegacion->archivo_destino)); //se copia el campo nodo

	posicion += strlen(delegacion->archivo_destino);

	do {
		tamString = strlen(tempList->archivo_temporal_transformacion);
		memcpy(paquete.buffer + posicion	,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
		memcpy(paquete.buffer + (posicion += sizeof(int) )					,tempList->archivo_temporal_transformacion			,strlen(tempList->archivo_temporal_transformacion)); //se copia el campo port
		posicion += strlen(tempList->archivo_temporal_transformacion);
	} while ((tempList = tempList->sig));

	tamString = 0;
	memcpy(paquete.buffer + posicion		,&tamString						,sizeof(int));

/*
	tamString = strlen(delegacion->programa_reduccion_local);
	memcpy(paquete.buffer + (posicion += strlen(delegacion->archivo_destino)),&(tamString)					,sizeof(tamString) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(tamString) )				,delegacion->programa_reduccion_local,strlen(delegacion->programa_reduccion_local)); //se copia el campo nodo
//*/
	return paquete;
}



Paquete srlz_notificacionArchivo(notificacionArchivo* notificacion,char proceso, int cod_operacion){
	int codigo = cod_operacion;//NOTIFICAR_ARCHIVO;

	int posicion = 0;//int para ir guiando desde donde se copia
	int sizeBuffer = 0;
	int tamString = 0;
	int tamPayload = 0;

	Paquete paquete;
	sizeBuffer =sizeof(char)+
			(sizeof(int)*3) // 2 son para el codigo y el "tam_buffer"; y los otros 3 para tamString
			+ strlen(notificacion->archivo)
			+ sizeof(bool);


	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - (sizeof(int)*2) - sizeof(char);

	//memcpy(dir de dónde copiaré, dir de lo que copiaré, tamaño de lo que copiaré)
	memcpy(paquete.buffer                                                   ,&(proceso)                     ,sizeof(char));
	memcpy(paquete.buffer + (posicion=sizeof(proceso))						,&(codigo)						,sizeof(int)); //copio el codigo
	memcpy(paquete.buffer + (posicion += sizeof(codigo))					,&(tamPayload)					,sizeof(paquete.tam_buffer)); //se copia el tamaño del buffer

	tamString = strlen(notificacion->archivo);
	memcpy(paquete.buffer + (posicion+=sizeof(tamPayload))					,&(tamString)					,sizeof(tamString));
	memcpy(paquete.buffer + (posicion+=sizeof(tamString))					,notificacion->archivo					,strlen(notificacion->archivo));
	memcpy(paquete.buffer + (posicion+=strlen(notificacion->archivo))					,&(notificacion->fallo)					,sizeof(bool));
	return paquete;
}
notificacionArchivo dsrlz_notificacionArchivo(void* buffer){

	int posicion = 0; //int para ir guiando desde donde se copia
	int tamString = 0;
	notificacionArchivo notificacion;

	//salteo los primeros 2 ints que contienen el cod_operacion y el tamano de PayLoad
	memcpy(&tamString 		,buffer+posicion					,sizeof(int));
	notificacion.archivo=malloc(sizeof(char)*tamString +1);
	memcpy(notificacion.archivo 				,buffer+(posicion+=sizeof(int))						,sizeof(char)* tamString);
	notificacion.archivo [tamString] = '\0';
	memcpy(&(notificacion.fallo)				  		,buffer+(posicion+=sizeof(char)*tamString)						,sizeof(bool));
	return notificacion;
}

Paquete srlz_delegacionReduccionGlobal(t_delegacion_reduccion_global* delegacion){

	int codigo = INICIAR_REDUCCION_ARCHIVO_GLOBAL;
	char proceso='M';
	int posicion = 0;//int para ir guiando desde donde se copia
	int sizeBuffer = 0;
	int tamString = 0;
	int tamPayload = 0;
	int tamList = 0;
	lista_delegacion_reduccion_global* tempList = delegacion->lista;

    while(tempList->worker_port){
		tamList += 	(sizeof(int) + strlen(tempList->archivos_temporales	));
		tamList +=  (sizeof(int) + strlen(tempList->nodo				));
		tamList +=	(sizeof(int) + strlen(tempList->worker_ip			));
		tamList +=  (sizeof(tempList->worker_port						));
		tempList = tempList->sig;
	}
	tempList = delegacion->lista;

	Paquete paquete;
	sizeBuffer	+=	sizeof(char);
	sizeBuffer	+=(sizeof(int)*5); // 2 son para el codigo y el "tam_buffer"; y los otros 3 para tamString
	sizeBuffer	+=strlen(delegacion->archivo_reduccion_global);
	sizeBuffer	+=strlen(delegacion->archivo_temporal_reduccion_local);
	sizeBuffer	+=tamList;

	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - (sizeof(int)*2) - sizeof(char);

	//memcpy(dir de dónde copiaré, dir de lo que copiaré, tamaño de lo que copiaré)
	memcpy(paquete.buffer                                                   ,&(proceso)                     ,sizeof(char));
	memcpy(paquete.buffer + (posicion=sizeof(proceso))						,&(codigo)						,sizeof(int)); //copio el codigo
	memcpy(paquete.buffer + (posicion += sizeof(codigo))					,&(tamPayload)					,sizeof(paquete.tam_buffer)); //se copia el tamaño del buffer

	tamString = strlen(delegacion->archivo_reduccion_global);
	memcpy(paquete.buffer + (posicion += sizeof(tamPayload))				,&(tamString)										,sizeof(tamString) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(tamString) )				,delegacion->archivo_reduccion_global				,strlen(delegacion->archivo_reduccion_global)); //se copia el campo nodo

	tamString = strlen(delegacion->archivo_temporal_reduccion_local);
	memcpy(paquete.buffer + (posicion += strlen(delegacion->archivo_reduccion_global))				,&(tamString)				,sizeof(tamString) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(tamString) )				,delegacion->archivo_temporal_reduccion_local		,strlen(delegacion->archivo_temporal_reduccion_local)); //se copia el campo nodo

	posicion += strlen(delegacion->archivo_temporal_reduccion_local);

	while(tempList->worker_port) {
		tamString = strlen(tempList->archivos_temporales);
		memcpy(paquete.buffer + posicion												,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
		memcpy(paquete.buffer + (posicion += sizeof(int) )								,tempList->archivos_temporales			,strlen(tempList->archivos_temporales)); //se copia el campo port

		tamString = strlen(tempList->nodo);
		memcpy(paquete.buffer + (posicion += strlen(tempList->archivos_temporales))		,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
		memcpy(paquete.buffer + (posicion += sizeof(int) )								,tempList->nodo					,strlen(tempList->nodo)); //se copia el campo port

		tamString = strlen(tempList->worker_ip);
		memcpy(paquete.buffer + (posicion += strlen(tempList->nodo))					,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
		memcpy(paquete.buffer + (posicion += sizeof(int) )								,tempList->worker_ip			,strlen(tempList->worker_ip)); //se copia el campo port

		memcpy(paquete.buffer + (posicion += strlen(tempList->worker_ip))				,&tempList->worker_port			,sizeof(tempList->worker_port));

		posicion += sizeof(tempList->worker_port);
		tempList = tempList->sig;
	}

	tamString = 0;
	memcpy(paquete.buffer + posicion		,&tamString						,sizeof(int));

	return paquete;
}

t_delegacion_transformacion dsrlz_delegacionTransformacion(void* buffer){
	int posicion = 0; //int para ir guiando desde donde se copia
	int tamString = 0;
	t_delegacion_transformacion delegacion;


	memcpy(&delegacion.bloque				,buffer+posicion									,sizeof(int));
	memcpy(&delegacion.num_bytes			,buffer+(posicion+=sizeof(int))						,sizeof(int));

	memcpy(&tamString				 		,buffer+(posicion+=sizeof(int))						,sizeof(int));
	delegacion.archivo_destino = malloc((sizeof(char) * tamString) + 1);
	memcpy(delegacion.archivo_destino 	,buffer+(posicion+=sizeof(int))							,sizeof(char) * tamString);
	delegacion.archivo_destino [tamString] = '\0';

	return delegacion;
}

t_delegacion_reduccion_local dsrlz_delegacionReduccionLocal(void* buffer){// TODO		Probar
	int posicion = 0; //int para ir guiando desde donde se copia
	int tamString = 0;
	t_delegacion_reduccion_local delegacion;
	lista_temporales_transformacion* temp = malloc(sizeof(lista_temporales_transformacion));
	delegacion.lista_archivo_argumento = temp;

	memcpy(&(tamString)					 	,buffer		,sizeof(int));
	delegacion.archivo_destino = malloc((sizeof(char) * tamString)+1);
	memcpy(delegacion.archivo_destino		,buffer+(posicion+=sizeof(int))						,sizeof(char)*tamString);
	delegacion.archivo_destino[tamString] = '\0';

	memcpy(&(tamString)					    ,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));

	while (tamString){
		temp->archivo_temporal_transformacion = malloc((sizeof(char) * tamString) + 1);


		memcpy(temp->archivo_temporal_transformacion	        ,buffer+(posicion+=sizeof(int))					    ,sizeof(char)*tamString);
		temp->archivo_temporal_transformacion[tamString]='\0';

		memcpy(&(tamString)					    ,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));

		if(tamString){
			temp->sig = malloc(sizeof(lista_temporales_transformacion));
			temp = temp->sig;
			temp->sig = NULL;
		} else{
			temp->sig = NULL;
		}
	}

	return delegacion;
}

t_delegacion_reduccion_global dsrlz_delegacionReduccionGlobal(void* buffer){

	int posicion = 0; //int para ir guiando desde donde se copia
	int tamString = 0;
	t_delegacion_reduccion_global delegacion;
	lista_delegacion_reduccion_global* temp = malloc(sizeof(lista_delegacion_reduccion_global));
	delegacion.lista = temp;

	memcpy(&(tamString)					 	,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));
	delegacion.archivo_reduccion_global = malloc(sizeof(char) * tamString+1);
	memcpy(delegacion.archivo_reduccion_global		,buffer+(posicion+=sizeof(int))						,sizeof(char)*tamString);
	delegacion.archivo_reduccion_global[tamString] = '\0';

	memcpy(&(tamString)					 	,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));
	delegacion.archivo_temporal_reduccion_local = malloc(sizeof(char) * tamString+1);
	memcpy(delegacion.archivo_temporal_reduccion_local		,buffer+(posicion+=sizeof(int))						,sizeof(char)*tamString);
	delegacion.archivo_temporal_reduccion_local[tamString] = '\0';


	memcpy(&(tamString)					   			    ,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));

	while (tamString){
		temp->archivos_temporales = malloc((sizeof(char) * tamString) + 1);
		memcpy(temp->archivos_temporales	        	,buffer+(posicion+=sizeof(int))					    ,sizeof(char)*tamString);
		temp->archivos_temporales[tamString]='\0';

		memcpy(&(tamString)								,buffer+(posicion+=strlen(temp->archivos_temporales))		,sizeof(int));
		temp->nodo = malloc((sizeof(char) * tamString) + 1);
		memcpy(temp->nodo								,buffer+(posicion+=sizeof(int))					    ,sizeof(char)*tamString);
		temp->nodo[tamString]='\0';


		memcpy(&(tamString)							    ,buffer+(posicion+=strlen(temp->nodo))		,sizeof(int));
		temp->worker_ip = malloc((sizeof(char) * tamString) + 1);
		memcpy(temp->worker_ip				  	        ,buffer+(posicion+=sizeof(int))					    ,sizeof(char)*tamString);
		temp->worker_ip[tamString]='\0';

		memcpy(&(temp->worker_port)					    ,buffer+(posicion+=strlen(temp->worker_ip))		,sizeof(int));

		memcpy(&(tamString)					    		,buffer+(posicion+=sizeof(temp->worker_port))		,sizeof(int));

		if(tamString){
			temp->sig = malloc(sizeof(lista_temporales_transformacion));
			temp = temp->sig;
		} else{
			temp->sig = NULL;
		}
	}

	return delegacion;
}

char* dsrlz_archivo(void* buffer){
	int posicion = 0; //int para ir guiando desde donde se copia
	int tamanoArchivo = 0;

	memcpy(&tamanoArchivo 					,buffer+posicion					,sizeof(int));

	char* contenidoArchivo;
	contenidoArchivo = malloc(tamanoArchivo);
	memcpy(contenidoArchivo 				,buffer+(posicion+=sizeof(int))						,tamanoArchivo);

	return contenidoArchivo;
}

t_solicitud_pedido_bloque dsrlz_solicitud_pedido_bloque(void* buffer){

	int posicion = 0; //int para ir guiando desde donde se copia
	t_solicitud_pedido_bloque solicitud;

	memcpy(&solicitud.bloque 						,buffer+posicion									,sizeof(int));
	memcpy(&solicitud.tamano_bloque 				,buffer+(posicion+=sizeof(int))						,sizeof(int));

	return solicitud;
}

t_bloque_contenido dsrlz_bloque_contenido(void* buffer){

	int posicion = 0; //int para ir guiando desde donde se copia
	int tamanoArchivo = 0;
	t_bloque_contenido solicitud;

	memcpy(&tamanoArchivo 						,buffer+posicion									,sizeof(int));

//printf("tamanoArchivo: %d", tamanoArchivo);

	solicitud.contenido_archivo = malloc(tamanoArchivo);
	memcpy(solicitud.contenido_archivo 			,buffer+(posicion+=sizeof(int))						,tamanoArchivo);
	memcpy(&solicitud.bloque 					,buffer+(posicion+=tamanoArchivo)					,sizeof(int));

	return solicitud;
}

t_bloque_contenido dsrlz_bloque_archivo(void* buffer){
	int posicion = 0; //int para ir guiando desde donde se copia
	int tamString = 0;
	t_bloque_contenido solicitud;

	memcpy(&(tamString)					 	,buffer+posicion										,sizeof(int));
	solicitud.nombre = malloc(sizeof(char) * tamString+1);
	memcpy(solicitud.nombre			   	    ,buffer+(posicion+=sizeof(int))							,sizeof(char)*tamString);
	solicitud.nombre[tamString]='\0';

	memcpy(&solicitud.tamanio 						,buffer+(posicion+=tamString)						,sizeof(int));

	solicitud.contenido_archivo = malloc(solicitud.tamanio);
	memcpy(solicitud.contenido_archivo 			,buffer+(posicion+=sizeof(int))						,solicitud.tamanio);
	memcpy(&solicitud.bloque 					,buffer+(posicion+=solicitud.tamanio)					,sizeof(int));

	return solicitud;
}


t_solicitud_transformacion dsrlz_solicitudTransformacion(void* buffer) //este buffer es el buffer original - cod de op - tam buffer
{
	int posicion = 0; //int para ir guiando desde donde se copia
	int tamString = 0;
	t_solicitud_transformacion solicitud;

	//salteo los primeros 2 ints que contienen el cod_operacion y el tamano de PayLoad
	memcpy(&solicitud.bytes_ocupados 		,buffer+posicion					,sizeof(int));
	memcpy(&solicitud.bloque 				,buffer+(posicion+=sizeof(int))						,sizeof(int));

	memcpy(&tamString				  		,buffer+(posicion+=sizeof(int))						,sizeof(int));
	solicitud.worker_ip = malloc(sizeof(char) * tamString+1);
	memcpy(solicitud.worker_ip		   	   	,buffer+(posicion+=sizeof(int))				  		,sizeof(char) * tamString);
    solicitud.worker_ip[tamString]='\0';

	memcpy(&(tamString)					 	,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));
	solicitud.nodo = malloc(sizeof(char) * tamString+1);
	memcpy(solicitud.nodo			   	    ,buffer+(posicion+=sizeof(int))						,sizeof(char)*tamString);
	solicitud.nodo[tamString]='\0';

	memcpy(&(tamString)					    ,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));
	solicitud.archivo_tmp = malloc(sizeof(char) * tamString+1);
	memcpy(solicitud.archivo_tmp	        ,buffer+(posicion+=sizeof(int))					    ,sizeof(char)*tamString);
	solicitud.archivo_tmp[tamString]='\0';

	//memcpy(&(tamString)					    ,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));
	//solicitud.worker_port = malloc(sizeof(char) * tamString);
	memcpy(&solicitud.worker_port	        ,buffer+(posicion+=sizeof(char) * tamString)					    ,sizeof(int));


	return solicitud;
}

t_solicitud_reduccion_global dsrlz_solicitudReduccionGlobal(void* buffer)
{
	int posicion = 0; //int para ir guiando desde donde se copia
	int tamString = 0;
	t_solicitud_reduccion_global solicitud;

	//salteo los primeros 2 ints que contienen el cod_operacion y el tamano de PayLoad
	memcpy(&solicitud.worker_port 		,buffer+posicion					,sizeof(int));

printf("\nsolicitud.worker_port:%d\n", solicitud.worker_port);

	memcpy(&solicitud.encargado 				,buffer+(posicion+=sizeof(int))						,sizeof(bool));

printf("solicitud.encargado:%d\n", solicitud.encargado);

	memcpy(&tamString				  		,buffer+(posicion+=sizeof(bool))						,sizeof(int));
	solicitud.worker_ip = malloc(sizeof(char) * tamString + 1);
	memcpy(solicitud.worker_ip		   	   	,buffer+(posicion+=sizeof(int))				  		,sizeof(char) * tamString);
    solicitud.worker_ip[tamString]='\0';

printf("solicitud.worker_ip:%s\n", solicitud.worker_ip);

	memcpy(&(tamString)					 	,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));
	solicitud.nodo = malloc(sizeof(char) * tamString + 1);
	memcpy(solicitud.nodo			   	    ,buffer+(posicion+=sizeof(int))						,sizeof(char)*tamString);
	solicitud.nodo[tamString]='\0';

printf("solicitud.nodo:%s\n", solicitud.nodo);

	memcpy(&(tamString)					    ,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));
	solicitud.archivo_tmp_reduccion_local = malloc(sizeof(char) * tamString + 1);
	memcpy(solicitud.archivo_tmp_reduccion_local	        ,buffer+(posicion+=sizeof(int))					    ,sizeof(char)*tamString);
	solicitud.archivo_tmp_reduccion_local[tamString]='\0';

printf("solicitud.archivo_tmp_reduccion_local:%s\n", solicitud.archivo_tmp_reduccion_local);

	memcpy(&(tamString)					    ,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));
	solicitud.archivo_reduccion_global = malloc(sizeof(char) * tamString + 1);
	memcpy(solicitud.archivo_reduccion_global	        ,buffer+(posicion+=sizeof(int))					    ,sizeof(char)*tamString);
	solicitud.archivo_reduccion_global[tamString]='\0';

printf("solicitud.archivo_reduccion_global:%s\n\n", solicitud.archivo_reduccion_global);

	return solicitud;
}
// TODO
t_solicitud_reduccion_local dsrlz_solicitudReduccionLocal(void* buffer)
{
	int posicion = 0; //int para ir guiando desde donde se copia
	int tamString = 0;
	t_solicitud_reduccion_local solicitud;
	lista_temporales_transformacion* temp = malloc(sizeof(lista_temporales_transformacion));
	solicitud.archivo_tmp_transformacion = temp;

	//salteo los primeros 2 ints que contienen el cod_operacion y el tamano de PayLoad
	memcpy(&solicitud.worker_port 		,buffer+posicion					,sizeof(int));

	//memcpy(&solicitud.encargado 				,buffer+(posicion+=sizeof(int))						,sizeof(int));

	memcpy(&tamString				  		,buffer+(posicion+=sizeof(int))						,sizeof(int));
	solicitud.worker_ip = malloc((sizeof(char) * tamString) +1);
	memcpy(solicitud.worker_ip		   	   	,buffer+(posicion+=sizeof(int))				  		,sizeof(char) * tamString);
    solicitud.worker_ip[tamString]='\0';

	memcpy(&(tamString)					 	,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));
	solicitud.nodo = malloc((sizeof(char) * tamString) +1);
	memcpy(solicitud.nodo			   	    ,buffer+(posicion+=sizeof(int))						,sizeof(char)*tamString);
	solicitud.nodo[tamString]='\0';

	memcpy(&(tamString)					    ,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));
	solicitud.archivo_tmp_reduccion_local = malloc((sizeof(char) * tamString) +1);
	memcpy(solicitud.archivo_tmp_reduccion_local	        ,buffer+(posicion+=sizeof(int))					    ,sizeof(char)*tamString);
	solicitud.archivo_tmp_reduccion_local[tamString]='\0';

	memcpy(&(tamString)					    ,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));

	while (tamString){
		temp->archivo_temporal_transformacion = malloc((sizeof(char) * tamString) + 1);


		memcpy(temp->archivo_temporal_transformacion	        ,buffer+(posicion+=sizeof(int))					    ,sizeof(char)*tamString);
		temp->archivo_temporal_transformacion[tamString]='\0';

		memcpy(&(tamString)					    ,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));

		if(tamString){
			temp->sig = malloc(sizeof(lista_temporales_transformacion));
			temp = temp->sig;
			temp->sig = NULL;
		} else{
			temp->sig = NULL;
		}
	}

	return solicitud;
}

t_solicitud_almacenado_final dsrlz_solicitudAlmacenadoFinal(void* buffer)
{
	int posicion = 0; //int para ir guiando desde donde se copia
	int tamString = 0;
	t_solicitud_almacenado_final solicitud;

	//salteo los primeros 2 ints que contienen el cod_operacion y el tamano de PayLoad
	memcpy(&solicitud.worker_port 		,buffer+posicion					,sizeof(int));
	//memcpy(&solicitud.bloque 				,buffer+(posicion+=sizeof(int))						,sizeof(int));

	memcpy(&tamString				  		,buffer+(posicion+=sizeof(int))						,sizeof(int));
	solicitud.worker_ip = malloc(sizeof(char) * tamString);
	memcpy(solicitud.worker_ip		   	   	,buffer+(posicion+=sizeof(int))				  		,sizeof(char) * tamString);
    solicitud.worker_ip[tamString]='\0';

	memcpy(&(tamString)					 	,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));
	solicitud.nodo = malloc(sizeof(char) * tamString);
	memcpy(solicitud.nodo			   	    ,buffer+(posicion+=sizeof(int))						,sizeof(char)*tamString);
	solicitud.nodo[tamString]='\0';

	memcpy(&(tamString)					    ,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));
	solicitud.archivo_reduccion_global = malloc(sizeof(char) * tamString);
	memcpy(solicitud.archivo_reduccion_global	        ,buffer+(posicion+=sizeof(int))					    ,sizeof(char)*tamString);
	solicitud.archivo_reduccion_global[tamString]='\0';

	//memcpy(&(tamString)					    ,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));
	//solicitud.worker_port = malloc(sizeof(char) * tamString);
	//memcpy(&solicitud.worker_port	        ,buffer+(posicion+=sizeof(char) * tamString)					    ,sizeof(int));


	return solicitud;
}

t_bloques_archivos dsrlz_bloquenodo(void* buffer)
{
	int posicion = 0; //int para ir guiando desde donde se copia
	int tamString = 0;
	t_bloques_archivos solicitud;
printf("1\n");
	//salteo los primeros 2 ints que contienen el cod_operacion y el tamano de PayLoad
	memcpy(&solicitud.bloque		,buffer+posicion					,sizeof(int));



	memcpy(&solicitud.bytes_ocupados 				,buffer+(posicion+=sizeof(int))						,sizeof(int));


	memcpy(&tamString				  		,buffer+(posicion+=sizeof(int))						,sizeof(int));

	solicitud.copia1.nodo = malloc((sizeof(char) * tamString)+1);

	memcpy(solicitud.copia1.nodo		   	   	,buffer+(posicion+=sizeof(int))				  		,sizeof(char) * tamString);
   solicitud.copia1.nodo[tamString]='\0';

	memcpy(&tamString					 	,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));

	solicitud.copia2.nodo = malloc((sizeof(char) * tamString)+1);
	memcpy(solicitud.copia2.nodo			   	    ,buffer+(posicion+=sizeof(int))						,sizeof(char)*tamString);
	solicitud.copia2.nodo[tamString]='\0';


	memcpy(&solicitud.copia1.bloque					    ,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));

	memcpy(&solicitud.copia2.bloque	        ,buffer+(posicion+=sizeof(int))					    ,sizeof(int));


	memcpy(&solicitud.copia1.port			,buffer+(posicion+=sizeof(int)) 					,sizeof(int));

	memcpy(&solicitud.copia2.port			,buffer+(posicion+=sizeof(int)) 					,sizeof(int));


	memcpy(&tamString					 	,buffer+(posicion+=sizeof(int))		,sizeof(int));
	solicitud.copia1.ip = malloc((sizeof(char) * tamString)+1);

	memcpy(solicitud.copia1.ip			   	    ,buffer+(posicion+=sizeof(int))						,sizeof(char)*tamString);

	solicitud.copia1.ip[tamString]='\0';


	memcpy(&tamString					 	,buffer+(posicion+=sizeof(char) * tamString)		,sizeof(int));

	solicitud.copia2.ip = malloc((sizeof(char) * tamString)+1);
	memcpy(solicitud.copia2.ip			   	    ,buffer+(posicion+=sizeof(int))						,sizeof(char)*tamString);

	solicitud.copia2.ip[tamString]='\0';


	return solicitud;
}

//**************************************************************************//
//**********************Envios de Master************************************//	No probados
//**************************************************************************//
// Envio a YAMA
Paquete srlz_envio_transformacion(Solicitud_Master * solicitud,char proceso,int codigo){



	int posicion = 0, sizeBuffer = 0,  tamPayload = 0, tamStr = 0;
	Paquete paquete;


	sizeBuffer =sizeof(char)	// Para proceso
			+ (sizeof(int)*3)	// Para el código, el "tam_buffer" y el tamaño del archivo
			+ strlen(solicitud->archivo);

	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - ((sizeof(int)*2) + sizeof(char));


	//memcpy(dir a dónde copiaré, dir de lo que copiaré, tamaño de lo que copiaré)
	memcpy(paquete.buffer                               ,					&(proceso),						sizeof(char));
	memcpy(paquete.buffer + (posicion = sizeof(proceso)),					&(codigo),						sizeof(int)); //copio el codigo
	memcpy(paquete.buffer + (posicion += sizeof(codigo)),					&(tamPayload),					sizeof(tamPayload)); //se copia el tamaño del buffer
	tamStr = strlen(solicitud->archivo);
	memcpy(paquete.buffer + (posicion += sizeof(tamPayload)),				&(tamStr),					sizeof(int));
	memcpy(paquete.buffer + (posicion += sizeof(int)),						solicitud->archivo,		sizeof(char) * (strlen(solicitud->archivo)) );

	return paquete;
}

Solicitud_Master dsrlz_envio_transformacion(void* buffer){
	int posicion = 0;
	int tamString = 0;
	Solicitud_Master delegacion;

	memcpy(&tamString,							buffer + posicion,						sizeof(int));
	delegacion.archivo = malloc(sizeof(char) * tamString +1);
	memcpy(delegacion.archivo,					buffer + (posicion += sizeof(int)),		sizeof(char)*tamString);
	delegacion.archivo [tamString] = '\0';

	return delegacion;
}

Paquete srlz_datosNodo(t_nodos* datos_nodo){
	int codigo = INFO_NODO;
	char proceso='D';


	int posicion = 0;//int para ir guiando desde donde se copia
	int sizeBuffer = 0;
	int tamString = 0;
	int tamPayload = 0;


	Paquete paquete;
	sizeBuffer =sizeof(char)+
			(sizeof(int)*8) // 2 son para el codigo y el "tam_buffer"; otros 3 para tamano,libre y port; y 2 para tamString
			+ strlen(datos_nodo->nodo) + strlen(datos_nodo->ip);

	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - (sizeof(int)*2) - sizeof(char);

	//memcpy(dir de dónde copiaré, dir de lo que copiaré, tamaño de lo que copiaré)
	memcpy(paquete.buffer                                                   ,&(proceso)                     ,sizeof(char));
	memcpy(paquete.buffer + (posicion=sizeof(proceso))						,&(codigo)						,sizeof(int)); //copio el codigo
	memcpy(paquete.buffer + (posicion += sizeof(codigo))					,&(tamPayload)					,sizeof(tamPayload)); //se copia el tamaño del buffer
	memcpy(paquete.buffer + (posicion += sizeof(tamPayload))		,&(datos_nodo->tamano)			,sizeof(datos_nodo->tamano)); //se copia el campo bytes ocupados
	memcpy(paquete.buffer + (posicion += sizeof(datos_nodo->tamano))	,&(datos_nodo->libre)			,sizeof(datos_nodo->libre)); //se copia el campo libre

	tamString = strlen(datos_nodo->nodo);
	memcpy(paquete.buffer + (posicion += sizeof(datos_nodo->libre))			,&(tamString)					,sizeof(int) ); //guardo el tam del siguiente array
	memcpy(paquete.buffer + (posicion += sizeof(int))						,datos_nodo->nodo			,strlen(datos_nodo->nodo));

	memcpy(paquete.buffer + (posicion += strlen(datos_nodo->nodo))		,&(datos_nodo->port_worker)			,sizeof(int));

	memcpy(paquete.buffer + (posicion += sizeof(int))					,&(datos_nodo->port_datanode)		,sizeof(int));

	tamString = strlen(datos_nodo->ip);
	memcpy(paquete.buffer + (posicion += sizeof(int))					,&(tamString)					,sizeof(int) ); //
	memcpy(paquete.buffer + (posicion += sizeof(int))					,datos_nodo->ip			,strlen(datos_nodo->ip));

	return paquete;	
}

t_nodos dsrlz_datosNodo(void* buffer) //este buffer es el buffer original - cod de op - tam buffer
{
	int posicion = 0; //int para ir guiando desde donde se copia
	int tamString = 0;
	t_nodos solicitud;

	memcpy(&solicitud.tamano 				,buffer+posicion							,sizeof(int));
	memcpy(&solicitud.libre 				,buffer+(posicion+=sizeof(int))				,sizeof(int));
	memcpy(&(tamString)					 	,buffer+(posicion+=sizeof(int))				,sizeof(int));
	solicitud.nodo = malloc(sizeof(char) * tamString+1);
	memcpy(solicitud.nodo			   	    ,buffer+(posicion+=sizeof(int))				,sizeof(char)*tamString);
	solicitud.nodo[tamString]='\0';

	memcpy(&solicitud.port_worker 			,buffer+(posicion+=sizeof(char)*tamString)		,sizeof(int));
	memcpy(&solicitud.port_datanode		    ,buffer+(posicion+=sizeof(int))					,sizeof(int));
	memcpy(&(tamString)					 	,buffer+(posicion+=sizeof(int))					,sizeof(int));
	solicitud.ip = malloc(sizeof(char) * tamString+1);
	memcpy(solicitud.ip			   	    ,buffer+(posicion+=sizeof(int))				,sizeof(char)*tamString);
	solicitud.ip[tamString]='\0';

	return solicitud;
}

//**************************************************************************//

//***********************FUNCIONES WORKER********************************//

char* concatenar(const char *s1, const char *s2)
{
    const size_t len1 = strlen(s1);
    const size_t len2 = strlen(s2);
    char *result = malloc(len1+len2+1);//+1 for the zero-terminator
    //in real code you would check for errors in malloc here
    memcpy(result, s1, len1);
    memcpy(result+len1, s2, len2+1);//+1 to copy the null-terminator
    return result;
}

//***********************FUNCIONES FS ********************************//

int countParametrosConsola(char * string){ // Devuelve la cantidad de parametros que contiene un ingreso por consola
	int cant_elementos = 0, i;

	for(i=0; i<=strlen(string); i=i+1){
		if(string[i] == ' ' ){
			cant_elementos = cant_elementos + 1;
		}
	}

	return cant_elementos;

}

/**************************************************
 *
 * Obtiene el Proximo Index para La Tabla de Directorio
 *
 **************************************************/
int getIndexDirectorioYama(t_list* listaTablaDirectorios, t_log* infoLogger){

	t_directorios* registroTablaDirectorios = NULL;
	int lastIndex = -1;

    void _each_elemento_(t_directorios* registroTablaDirectorios)
	{
		lastIndex = registroTablaDirectorios->index;
	}
    list_iterate(listaTablaDirectorios, (void*)_each_elemento_);

    free(registroTablaDirectorios);

    return (lastIndex + 1);

}
/**************************************************
 *
 * Crear un Directorio en YamaFS
 *
 **************************************************/
bool crearDirectorioYama(char* pathDirectorio, t_list* listaTablaDirectorios, t_log* infoLogger){ //Crea un Directorio en FsYama

	t_directorios* registroTablaDirectorios = NULL;
	char** arregloDirectorios = NULL;
	int lastIndexDirectory = 0, indice, idPadreActual = 0, cantDirectorios;

	// Si se quiere crear solo el root
	if(strcmp(pathDirectorio, "/") == 0 ){

		registroTablaDirectorios = malloc(sizeof(t_directorios));

		// Cargo el Registro de directorio
		registroTablaDirectorios->index = getIndexDirectorioYama(listaTablaDirectorios, infoLogger);

		strcpy( registroTablaDirectorios->nombre ,pathDirectorio);
		registroTablaDirectorios->nombre[strlen(pathDirectorio)] = '\0';
		registroTablaDirectorios->padre = -1;

		// Agrego el Registro en la Lista
		list_add(listaTablaDirectorios,registroTablaDirectorios);
		//free(registroTablaDirectorios);

	}else{
		// Separo el pathDirectorio en todos los subdirectorios posibles
		arregloDirectorios = string_split(pathDirectorio, "/");

		// Verifico si no se superaron los 100 directorios permitidos por YamaFS
		if(list_size(listaTablaDirectorios)+cantidadDirectoriosPath(pathDirectorio) > 101){
			log_info(infoLogger, "[Error] No se crearon los Directorios por superar el limite de 100 permitidos por YamaFS");
			printf("[Error] Se ha alcanzado el limite de 100 permitidos por YamaFS\n");
			return false;
		}

		cantDirectorios = cantidadDirectoriosPath(pathDirectorio);

		// Recorro cada nodo del path
		for (indice = 0; indice  < cantDirectorios; indice=indice+1 ) {

			// Existe Hijo(X) con Padre(Y) ?
			if(indice == 0){
				registroTablaDirectorios = ExisteNodoDirectorioYama(listaTablaDirectorios, arregloDirectorios[indice], "/");
			}else{
				registroTablaDirectorios = ExisteNodoDirectorioYama(listaTablaDirectorios, arregloDirectorios[indice], arregloDirectorios[indice-1]);
			}

			// Si existe el hijo
			if(registroTablaDirectorios != NULL ){

				idPadreActual = registroTablaDirectorios->index;
			}else{

				registroTablaDirectorios = malloc(sizeof(t_directorios));

				// Cargo el Registro de directorio
				registroTablaDirectorios->index = list_size(listaTablaDirectorios);
				strcpy( registroTablaDirectorios->nombre ,arregloDirectorios[indice]);
				registroTablaDirectorios->padre = idPadreActual;

				// Agrego el Registro en la Lista
				list_add(listaTablaDirectorios,registroTablaDirectorios);

				// Guardo la referencia del Padre para el proximo nodo
				idPadreActual = registroTablaDirectorios->index;

				//free(registroTablaDirectorios);
			}
		}

		free(arregloDirectorios);

	}

	//free(registroTablaDirectorios);

	// Persisto los datos
	persistirTablaDirectorio(listaTablaDirectorios, infoLogger);
	
	return true;
}

/**************************************************
 *
 * Formateo YamaFS
 *
 **************************************************/
bool formatearYama(t_list* listaTablaDirectorios, t_log* infoLogger){

	// Libero la Tabla de Directorios
	list_clean(listaTablaDirectorios);

	// Creo el Root
	crearDirectorioYama("/", listaTablaDirectorios, infoLogger);


	// Elimino el archivo nodos.bin
	unlink("metadata/nodos.bin");

	// Vacio la Tabla de Archivos
	if(vaciarTablaArchivos(infoLogger) != -1 ){
		log_info(infoLogger, "Iniciando la Tabla de Archivos" );
	}else{
		log_info(infoLogger, "No se pudo Inicializar la Tabla de Archivos" );
	}

    // Vacio los Bitmaps
	if(vaciarBitmaps(infoLogger) != -1 ){
		log_info(infoLogger, "Iniciando Bitmaps" );
	}else{
		log_info(infoLogger, "No se pudo Inicializar los Bitmaps" );
	}

	return true;
}

/**************************************************
 *
 * Verifico si existe un Nodo con Padre e Hijo
 *
 **************************************************/
t_directorios* ExisteNodoDirectorioYama(t_list* listaTablaDirectorios, char* nodoHijo, char* nodoPadre){

	int lastIndexDirectory = 0, idPadreHijo;
	t_directorios* registroTablaDirectorios = NULL;
	//registroTablaDirectorios = malloc(sizeof(t_directorios));

	t_list* listaPadresAux = list_create();
	bool existePadre = false;

	bool _find_nodo_(t_directorios* padre)
	{
		return (strcmp(padre->nombre,nodoPadre) == 0 );
	}

	void _each_nodo_padre_(t_directorios* padre)
	{
		if(!existePadre){
			if(padre->index == idPadreHijo){
				existePadre = true;
			}else{
				existePadre = false;
			}
		}
	}

	// Closure para encontrar los hijos de un Directorio
	bool _find_directoy_(t_directorios* hijoIterado)
	{
		// Guardo el id del padre para compararlo con los demas padres
		idPadreHijo = hijoIterado->padre;

		// Armo una lista con todos los padres que se llaman nodoPadre
		listaPadresAux = list_filter(listaTablaDirectorios, (void*)_find_nodo_);

		// Verifico si alguno de los padres de la Lista es el padre del hijoIterado
		list_iterate(listaPadresAux, (void*)_each_nodo_padre_);

		//printf("Comparacion: %s ==  %s | Padre: %d == %d\n", directorioaBuscar->nombre, directorioActual, directorioaBuscar->padre, lastIndexDirectory);

		return (strcmp(hijoIterado->nombre,nodoHijo) == 0 && existePadre);
	}

	// Busco el Nodo
	registroTablaDirectorios = list_find(listaTablaDirectorios,(void*)_find_directoy_);

	// Borro la lista
	list_destroy(listaPadresAux);
	return registroTablaDirectorios;
}

/**************************************************
 *
 * Esta vacio un Directorio en FsYama?
 *
 **************************************************/
bool estaVacioDirectorioYama(char* pathDirectorio, t_list* listaTablaDirectorios){

	// TODO
	return true;
}

/**************************************************
 *
 * Eliminar un Directorio en FsYama
 *
 **************************************************/
bool eliminarDirectorioYama(char* directorio, t_list* listaTablaDirectorios, t_log* infoLogger){

	t_directorios* registroTablaDirectorios = NULL;
	t_directorios* registroTablaDirectoriosBorrados = NULL;
	int cantDirectorios, indice, idRegistroEliminar = 0;
	char** arregloDirectorios = NULL;

	// Separo el pathDirectorio en todos los subdirectorios posibles
	arregloDirectorios = string_split(directorio, "/");

	cantDirectorios = cantidadDirectoriosPath(directorio);

	// Recorro cada nodo del directorio
	for (indice = 0; indice  < cantDirectorios; indice=indice+1 ) {

		// Obtengo el Registro de cada Nodo
		if(indice == 0){
			registroTablaDirectorios = ExisteNodoDirectorioYama(listaTablaDirectorios, arregloDirectorios[indice], "/");
		}else{
			registroTablaDirectorios = ExisteNodoDirectorioYama(listaTablaDirectorios, arregloDirectorios[indice], arregloDirectorios[indice-1]);
		}
		
		// Determino el nodo a borrar
		idRegistroEliminar = registroTablaDirectorios->index;

		// Borro el registro
		registroTablaDirectoriosBorrados = list_remove(listaTablaDirectorios,idRegistroEliminar);
	}

	// Persisto los datos
	persistirTablaDirectorio(listaTablaDirectorios, infoLogger);
	
	return true;
}

/**************************************************
 *
 * Eliminar un Archivo en FsYama
 *
 **************************************************/
bool eliminarArchivoYama(char* pathArchivo, t_list* listaTablaDirectorios, t_list* listaTablaNodos, t_log* infoLogger){

	int cantDirectorios, indice;
	char** arregloDirectorios = NULL;
	char* nombreArchivoOrigen = NULL;
	char* nombreArchivoAux = string_new();
	char* cmdString = string_new();
	t_list* listaDatosBloques = list_create();

	// Separo el pathArchivo en todos los subdirectorios posibles
	arregloDirectorios = string_split(pathArchivo, "/");
	cantDirectorios = cantidadDirectoriosPath(pathArchivo);

	// Obtengo el nombre del archivo
	nombreArchivoOrigen = malloc(strlen(arregloDirectorios[cantDirectorios-1])+1);
	strcpy(nombreArchivoOrigen,arregloDirectorios[cantDirectorios-1]);

	// Obtengo el indice del Directorio en YamaFS
	nombreArchivoAux = string_substring(pathArchivo, 0, string_length(pathArchivo) - string_length(nombreArchivoOrigen) - 1);
	indice = obtenerIndiceDirectorioYama(nombreArchivoAux , listaTablaDirectorios);

	// -------------------------------------------------------------------
	// Obtengo los bloques/nodos del archivo para poder liberar el bitmap
	// -------------------------------------------------------------------

		// Obtengo los datos de los bloques de archivo
		listaDatosBloques = obtenerDatosBloquesArchivo(pathArchivo, false, listaTablaDirectorios, listaTablaNodos, infoLogger);

	    void _each_elemento_(t_bloques_archivos* registroBloque)
		{

			printf("\nBloque: %d\n", registroBloque->bloque);
			printf("bytes_ocupados: %d\n", registroBloque->bytes_ocupados);
			printf("copia1 Nodo: %s Bloque: %d IP: %s Port: %d\n", registroBloque->copia1.nodo, registroBloque->copia1.bloque,registroBloque->copia1.ip,registroBloque->copia1.port);
			printf("copia2 Nodo: %s Bloque: %d IP: %s Port: %d\n", registroBloque->copia2.nodo, registroBloque->copia2.bloque,registroBloque->copia2.ip,registroBloque->copia2.port);

			// Marco los Bitmaps como libres - Copia 1
			t_bitmap* bitmap1_aux = cargarBitmap(listaTablaNodos, registroBloque->copia1.nodo);
			marcarBitmapBloqueLibre(listaTablaNodos, bitmap1_aux, registroBloque->copia1.nodo, registroBloque->copia1.bloque, infoLogger);
			free(bitmap1_aux->contenido);

			// Marco los Bitmaps como libres - Copia 2
			t_bitmap* bitmap2_aux = cargarBitmap(listaTablaNodos, registroBloque->copia2.nodo);
			marcarBitmapBloqueLibre(listaTablaNodos, bitmap2_aux, registroBloque->copia2.nodo, registroBloque->copia2.bloque, infoLogger);
			free(bitmap2_aux->contenido);

		}
	    list_iterate(listaDatosBloques, (void*)_each_elemento_);

	    list_destroy(listaDatosBloques);

	// -------------------------------------------------------------------


	string_append(&cmdString,"rm ");
	string_append(&cmdString,"metadata/archivos/");
	string_append(&cmdString,string_itoa(indice));
	string_append(&cmdString,"/");
	string_append(&cmdString,nombreArchivoOrigen);

printf("cmdString: %s\n", cmdString);

	system(cmdString);

	free(nombreArchivoAux);
	free(cmdString);
	free(nombreArchivoOrigen);
	free(arregloDirectorios);
}

/**************************************************
 *
 * Renombrar un Directorio en FsYama
 *
 **************************************************/
void renombrarDirectorioYama(char* directorioOrigen, char* directorioDestino, t_list* listaTablaDirectorios, t_log* infoLogger){

	t_directorios* registroTablaDirectorios = NULL;
	int cantDirectorios, indice;
	char** arregloDirectoriosOrigen = NULL;
	char** arregloDirectoriosDestino = NULL;

	// Separo el pathDirectorio en todos los subdirectorios posibles
	arregloDirectoriosOrigen = string_split(directorioOrigen, "/");
	arregloDirectoriosDestino = string_split(directorioDestino, "/");

	cantDirectorios = cantidadDirectoriosPath(directorioOrigen);

	// Recorro cada nodo del directorioOrigen
	for (indice = 0; indice  < cantDirectorios; indice=indice+1 ) {

		// Obtengo el Registro de cada Nodo
		if(indice == 0){
			registroTablaDirectorios = ExisteNodoDirectorioYama(listaTablaDirectorios, arregloDirectoriosOrigen[indice], "/");
		}else{
			registroTablaDirectorios = ExisteNodoDirectorioYama(listaTablaDirectorios, arregloDirectoriosOrigen[indice], arregloDirectoriosOrigen[indice-1]);
		}
		
		//Genero el Registro para reemplazar
		t_directorios* registroTablaDirectoriosReemplazo;
		registroTablaDirectoriosReemplazo = malloc(sizeof(t_directorios));

		// Cargo el Registro de directorio
		strcpy( registroTablaDirectoriosReemplazo->nombre ,arregloDirectoriosDestino[indice]);
		registroTablaDirectoriosReemplazo->padre = registroTablaDirectorios->padre;
		registroTablaDirectoriosReemplazo->index = registroTablaDirectorios->index;

		// Reemplazo el Registro
		list_replace(listaTablaDirectorios, registroTablaDirectoriosReemplazo->index, registroTablaDirectoriosReemplazo);

		free(registroTablaDirectoriosReemplazo);
	}

	// Persisto los datos
	persistirTablaDirectorio(listaTablaDirectorios, infoLogger);


	free(arregloDirectoriosOrigen);
	free(arregloDirectoriosDestino);
}

/**************************************************
 *
 * Renombrar un Archivo en FsYama
 *
 **************************************************/
void renombrarArchivoYama(char* pathArchivo, char* nombreArchivoDestino, t_list* listaTablaDirectorios, t_log* infoLogger){

	int cantDirectorios, indice;
	char** arregloDirectorios = NULL;
	char* nombreArchivoOrigen = NULL;
	char* nombreArchivoAux = string_new();

	// Separo el pathArchivo en todos los subdirectorios posibles
	arregloDirectorios = string_split(pathArchivo, "/");
	cantDirectorios = cantidadDirectoriosPath(pathArchivo);

	// Obtengo el nombre del archivo
	nombreArchivoOrigen = malloc(strlen(arregloDirectorios[cantDirectorios-1])+1);
	strcpy(nombreArchivoOrigen,arregloDirectorios[cantDirectorios-1]);

	// Obtengo el indice del Directorio en YamaFS
	nombreArchivoAux = string_substring(pathArchivo, 0, string_length(pathArchivo) - string_length(nombreArchivoOrigen) - 1);
	indice = obtenerIndiceDirectorioYama(nombreArchivoAux , listaTablaDirectorios);

	char *cmdString = string_new();
	string_append(&cmdString,"mv ");
	string_append(&cmdString,"metadata/archivos/");
	string_append(&cmdString,string_itoa(indice));
	string_append(&cmdString,"/");
	string_append(&cmdString,nombreArchivoOrigen);
	string_append(&cmdString," ");
	string_append(&cmdString,"metadata/archivos/");
	string_append(&cmdString,string_itoa(indice));
	string_append(&cmdString,"/");
	string_append(&cmdString,nombreArchivoDestino);
	system(cmdString);

	free(nombreArchivoAux);
	free(cmdString);
	free(nombreArchivoOrigen);
	free(arregloDirectorios);
}

t_list* listarDirectorioYama(char* directorio, t_list* listaTablaDirectorios){//Listar los archivos de un Directorio

	t_list* listaLs;

	bool _find_directoy_(t_directorios* directorioaBuscar)
	{
		//return (directorioaBuscar->padre == currentDirectory);
	}

	listaLs = list_filter(listaTablaDirectorios,(void*)_find_directoy_);

	return listaLs;
}

void showContenidolista(t_list* listaTablaDirectorios){ //Listar los archivos del Filesystem completo

	int indice = 0;

    void _each_elemento_(t_directorios* registroTablaDirectorios)
	{
		indice = indice + 1;

		// Muestro el encabezaado
		if(indice == 1) {
			printf("TABLA DE DIRECTORIOS\n");
			printf("Index \t Nombre \t Padre\n");
			printf("------\t -------\t -------\n");
		}

		printf("%d \t %s \t %d \n", registroTablaDirectorios->index,registroTablaDirectorios->nombre,registroTablaDirectorios->padre);
	}
    list_iterate(listaTablaDirectorios, (void*)_each_elemento_);
}

// Muestro todos los archivos de un Directorio de YamaFS
void mostrarArchivosDirectorioFS(char* pathDirectorio, t_list* listaTablaDirectorios){

	int indice;
	DIR *dp;
	struct dirent *ep;     
	struct stat st = {0};
	char* nombreArchivoMetadataAux = NULL;

	// Obtengo el indice del Directorio en YamaFS
	indice = obtenerIndiceDirectorioYama( pathDirectorio, listaTablaDirectorios);

	// Si el directorio existe

	nombreArchivoMetadataAux = string_from_format("metadata/archivos/%d/",indice);
	if (stat(nombreArchivoMetadataAux, &st) != -1) {

		dp = opendir (nombreArchivoMetadataAux);

		if (dp != NULL){

			printf("Listado de Archivos en %s\n", pathDirectorio);	
			while (ep = readdir (dp))

				if(strcmp(ep->d_name,".") != 0 && strcmp(ep->d_name,"..") != 0){
					printf("%s\n", ep->d_name);	
				}

				(void) closedir (dp);
		}
	}
	free(nombreArchivoMetadataAux);
}

// Obtener el indice de la Tabla de Directorios del directorio destino
int obtenerIndiceDirectorioYama( char* pathDirectorio, t_list* listaTablaDirectorios){

	t_directorios* registroTablaDirectorios = NULL;
	//registroTablaDirectorios = malloc(sizeof(t_directorios));
	int cantDirectorios, indice, lastIndex = 0;
	char** arregloDirectorios = NULL;

	// Separo el pathDirectorio en todos los subdirectorios posibles
	arregloDirectorios = string_split(pathDirectorio, "/");

	cantDirectorios = cantidadDirectoriosPath(pathDirectorio);

	// Recorro cada nodo del path
	for (indice = 0; indice  < cantDirectorios; indice=indice+1 ) {

		// Existe Hijo(X) con Padre(Y) ?
		if(indice == 0){
			registroTablaDirectorios = ExisteNodoDirectorioYama(listaTablaDirectorios, arregloDirectorios[indice], "/");
		}else{
			registroTablaDirectorios = ExisteNodoDirectorioYama(listaTablaDirectorios, arregloDirectorios[indice], arregloDirectorios[indice-1]);
		}

		// Guardo el Index del ultimo nodo
		lastIndex = registroTablaDirectorios->index;
	}

	free(arregloDirectorios);
	//free(registroTablaDirectorios);

	return lastIndex;		
}

Paquete srlz_delegacionAlmacenarArchivo(t_delegacion_almacenar_archivo* delegacion){
	int codigoOperacion = ENVIO_ARCHIVO_REDUCCION_GLOBAL;
	char proceso	=	'W';
	int sizeBuffer	=	0;
	int tamString	=	0;
	int tamPayload	=	0;
	int posicion	=	0;

	Paquete paquete;
	sizeBuffer	=	sizeof(char)
				+	(sizeof(int)*3)
				+	strlen(delegacion->archivo);

	paquete.tam_buffer = sizeBuffer;
	paquete.buffer = malloc( sizeBuffer );
	tamPayload = sizeBuffer - (sizeof(int)*2) - sizeof(char);

	memcpy(paquete.buffer                                                   ,&(proceso)                     ,sizeof(char));
	memcpy(paquete.buffer + (posicion=sizeof(char))							,&(codigoOperacion)				,sizeof(int));
	memcpy(paquete.buffer + (posicion += sizeof(int))						,&(tamPayload)					,sizeof(int));

	tamString = strlen(delegacion->archivo);
	memcpy(paquete.buffer + (posicion+=sizeof(tamPayload))					,&(tamString)					,sizeof(tamString));
	memcpy(paquete.buffer + (posicion+=sizeof(tamString))					,delegacion->archivo					,strlen(delegacion->archivo));

	return paquete;
}

t_delegacion_almacenar_archivo dsrlz_delegacionAlmacenarArchivo(void* buffer){

	int posicion = 0; //int para ir guiando desde donde se copia
	int tamString = 0;
	t_delegacion_almacenar_archivo archivo;

	//salteo los primeros 2 ints que contienen el cod_operacion y el tamano de PayLoad
	memcpy(&tamString 		,buffer+posicion					,sizeof(int));
	archivo.archivo=malloc(sizeof(char)*tamString +1);
	memcpy(archivo.archivo,buffer+(posicion+=sizeof(int))						,sizeof(char)* tamString);
	archivo.archivo [tamString] = '\0';
	return archivo;
}

/**************************************************
 *
 * Copio un Archivo de Master a Worker
 *
 **************************************************/
bool copiarArchivo_a_Worker(char* archivoFrom, int codOperacion, int socketWorker, t_log* infoLogger){

	struct stat st = {0};

	// Obtengo los datos del archivo Origen
	stat(archivoFrom, &st);

	int tam_archivo = st.st_size;


	log_info(infoLogger, "Iniciando envio del Archivo %s al Worker.", archivoFrom);
	printf("\nIniciando envio del Archivo %s al Worker.\n", archivoFrom);

	// Serializo el archivo a enviar
	Paquete paquete =srlz_bloque_archivo('M', codOperacion, archivoFrom, 0, tam_archivo, 0);
	int bytes_enviados = send(socketWorker,paquete.buffer,paquete.tam_buffer,0);
	free(paquete.buffer);

	if(bytes_enviados != -1){
		log_info(infoLogger, "Archivo enviado con éxito al Worker");
		printf("Archivo enviado con éxito al Worker\n");

		return true;
	}else{
		return false;
	}
}

/**************************************************
 *
 * Copio un Archivo a YamaFS
 *
 **************************************************/
bool copiarArchivoYama(char* archivoFrom, char* pathDirectorio, char* tipoArchivo, t_list* listaTablaDirectorios, t_queue* colaNodos, t_list* listaTablaNodos, t_list* listaNodosConectados, t_queue* colaNodosConectados, t_log* infoLogger){ 

	int cantDirectorios, indice, lastIndex = 0, bloque;
	char** arregloDirectoriosFrom = NULL;
	char* nombreArchivoFrom;
	char* pathDestinoCompleto = string_new();
	struct stat st = {0};
	t_list* listaAuxBloquesMarcadosOcupados = list_create();
	nodo_bloque* registroNodoBloque = NULL;


	// Obtener el indice de la Tabla de Directorios del directorio destino
 	lastIndex = obtenerIndiceDirectorioYama( pathDirectorio, listaTablaDirectorios);

	// Separo el pathDirectorio en todos los subdirectorios posibles
	arregloDirectoriosFrom = string_split(archivoFrom, "/");
	cantDirectorios = cantidadDirectoriosPath(archivoFrom);

	// Obtengo el nombre del archivo origen
	nombreArchivoFrom = malloc(strlen(arregloDirectoriosFrom[cantDirectorios-1])+1);
	strcpy(nombreArchivoFrom,arregloDirectoriosFrom[cantDirectorios-1]);

	// **************************************************
	// Si el directorio destino no existe, lo creo
	// **************************************************

		string_append(&pathDestinoCompleto, "metadata/archivos/");
		string_append(&pathDestinoCompleto, string_itoa(lastIndex));

		if (stat(pathDestinoCompleto, &st) == -1) {
			if(mkdir(pathDestinoCompleto, 0777) == -1){ // Si hubo error
				return false;
			}
		}

	// **************************************************
	// Se crea el archivo en la Tabla de Archivos
	// **************************************************
	string_append(&pathDestinoCompleto, "/");
	string_append(&pathDestinoCompleto, nombreArchivoFrom);		
	
	// Obtengo los datos del archivo Origen
	stat(archivoFrom, &st);

	char * string_aux = string_new();
	int tam_archivo = st.st_size;

	string_append(&string_aux, "TAMANIO=");
	string_append(&string_aux, string_itoa(tam_archivo));
	string_append(&string_aux, "\n");

	string_append(&string_aux, "TIPO=TEXTO");
	string_append(&string_aux, "\n");

	// Determino la cantidad de bloques que tiene el archivo
	int total_bloques = (tam_archivo / TAM_BLOQUE );

	// Si el tamano no es entero, lo redondeo para arriba
	if(tam_archivo % TAM_BLOQUE) { total_bloques = total_bloques+1; }

	int totalBytesBloque = 0;
	t_bloques_archivos* lstBloquesArchivos = NULL;
	//lstBloquesArchivos = malloc(sizeof(t_bloques_archivos));

	t_nodos* registroNodos1 = NULL;
	t_nodos* registroNodos2 = NULL;

	int bytesDesde = 0;
	bool hayBloquesContiguos = true;
	char* stringAux = string_new();

	// Guardo la informacion de cada bloque
	for (bloque = 0; bloque  < total_bloques; bloque=bloque+1 ) {

		// ------------------------------------------------------------------
	    // Extraigo 2 nodos de la Cola de Conectados para utilizarlos en la asignacion
	    // ------------------------------------------------------------------

			if(queue_size(colaNodosConectados) >= 2){
				registroNodos1 = queue_pop(colaNodosConectados);
				registroNodos2 = queue_pop(colaNodosConectados);
			}else{
				printf("No existen 2 Nodos conectados para iniciar el proceso. [Sistema Inestable]\n");
				return false;
			}


			printf("Nodos seleccionados para el Bloque %d: %s (copia1) y %s (copia2)\n", bloque, registroNodos1->nodo, registroNodos2->nodo);

			// Obtengo el primero bloques libre del Nodo
			int primerBloqueLibreNodo1 = obtenerPrimerBloquesLibresNodo(listaTablaNodos, registroNodos1->nodo, total_bloques);

			// Si no se pudo obtener el bloque libre
			if(primerBloqueLibreNodo1 == -1){
				printf("No se encontraron bloques contiguos libres del Nodo %s\n", registroNodos1->nodo);

				// Agregal al final de la Cola los nodo extraidos
				queue_push(colaNodosConectados, registroNodos1);
				queue_push(colaNodosConectados, registroNodos2);

				hayBloquesContiguos = false;
				//return false;
			}

			int primerBloqueLibreNodo2 = obtenerPrimerBloquesLibresNodo(listaTablaNodos, registroNodos2->nodo, total_bloques);

			// Si no se pudo obtener el bloque libre
			if(primerBloqueLibreNodo2 == -1){
				printf("No se encontraron bloques contiguos libres del Nodo %s\n", registroNodos2->nodo);

				// Agregal al final de la Cola los nodo extraidos
				queue_push(colaNodosConectados, registroNodos1);
				queue_push(colaNodosConectados, registroNodos2);
				
				hayBloquesContiguos = false;
				//return false;
			}

	    // ------------------------------------------------------------------


		if(hayBloquesContiguos){

			stringAux = string_from_format("BLOQUE%sBYTES=", string_itoa(bloque));
			string_append(&string_aux, stringAux);

			// Si el archivo es binario, el bloque es de 1 MiB
			if(strcmp(tipoArchivo, "b") == 0){
				totalBytesBloque = TAM_BLOQUE;
			}else{ // Si es archivo de texto, el bloque puede ser menor a 1 MiB

				// Si es el ultimo bloque, calculo el total de bytes por la diferencia
				if(bloque == (total_bloques-1)){
					totalBytesBloque = tam_archivo - bytesDesde;
				}else{

					totalBytesBloque = tamanoBloqueArchivoTexto(archivoFrom, bytesDesde);
				}
			}

			string_append(&string_aux, string_itoa( totalBytesBloque));
			string_append(&string_aux, "\n");

			// Asigno los Nodos destino del bloque
			lstBloquesArchivos = asignarBloquesNodos(false, listaTablaNodos, bloque, totalBytesBloque, registroNodos1->nodo, registroNodos2->nodo,primerBloqueLibreNodo1, primerBloqueLibreNodo2);

			// Determino a que Nodos se copian los bloques

			stringAux = string_from_format("BLOQUE%sCOPIA0=", string_itoa(bloque));
			string_append(&string_aux, stringAux);
			stringAux = string_from_format("[%s,%d]\n", lstBloquesArchivos->copia1.nodo, lstBloquesArchivos->copia1.bloque);
			string_append(&string_aux, stringAux);

			string_append(&string_aux, string_from_format("BLOQUE%sCOPIA1=", string_itoa(bloque)));
			string_append(&string_aux, string_from_format("[%s,%d]\n", lstBloquesArchivos->copia2.nodo, lstBloquesArchivos->copia2.bloque));


			// Envio el Bloque al Nodo para que lo persista [Copia 1]
			if(enviarBloqueNodo(listaNodosConectados, bloque, lstBloquesArchivos->copia1.nodo, lstBloquesArchivos->copia1.bloque, archivoFrom, bytesDesde, bytesDesde + totalBytesBloque - 1, infoLogger) == -1){ // Si hubo error

				printf("El Nodo %s se encuentra desconectado.\n", lstBloquesArchivos->copia1.nodo);
				return false;
			}

			// Envio el Bloque al Nodo para que lo persista [Copia 2]
			if(enviarBloqueNodo(listaNodosConectados, bloque, lstBloquesArchivos->copia2.nodo, lstBloquesArchivos->copia2.bloque, archivoFrom, bytesDesde, bytesDesde + totalBytesBloque - 1, infoLogger) == -1){ // Si hubo error

				printf("El Nodo %s se encuentra desconectado.\n", lstBloquesArchivos->copia2.nodo);
				return false;
			}

			bytesDesde = bytesDesde + totalBytesBloque;

			// ------------------------------------------------------------------
		    // Genero lista de los bloques a marcar como ocupados si toda la operacion termino correctamente
		    // ------------------------------------------------------------------

				registroNodoBloque = malloc(sizeof(nodo_bloque));
				registroNodoBloque->nodo = malloc(strlen(registroNodos1->nodo) + 1);

				//registroNodoBloque->nodo = malloc(strlen(registroNodos1->nodo) + 1);
				strcpy(registroNodoBloque->nodo, registroNodos1->nodo);
				registroNodoBloque->nodo[strlen(registroNodos1->nodo)] = '\0';
				registroNodoBloque->bloque = primerBloqueLibreNodo1;

				// Agrego el Registro del Nodo 1 en la Lista
				list_add(listaAuxBloquesMarcadosOcupados, registroNodoBloque);

			// -----------------------------------------------------------------

				registroNodoBloque = malloc(sizeof(nodo_bloque));
				registroNodoBloque->nodo = malloc(strlen(registroNodos2->nodo) + 1);

				strcpy(registroNodoBloque->nodo, registroNodos2->nodo);
				registroNodoBloque->nodo[strlen(registroNodos2->nodo)] = '\0';
				registroNodoBloque->bloque = primerBloqueLibreNodo2;

				// Agrego el Registro del Nodo 2 en la Lista
				list_add(listaAuxBloquesMarcadosOcupados, registroNodoBloque);

			// ------------------------------------------------------------------
		    // Marco los Bitmaps como ocupados y roto la cota de extraidos
		    // ------------------------------------------------------------------

				// Marco los Bitmaps como ocupados
				t_bitmap* bitmap1 = cargarBitmap(listaTablaNodos, registroNodos1->nodo);
				marcarBitmapBloqueOcupado(listaTablaNodos, bitmap1, registroNodos1->nodo, primerBloqueLibreNodo1, 1, infoLogger);
				free(bitmap1->contenido);

				t_bitmap* bitmap2 = cargarBitmap(listaTablaNodos, registroNodos2->nodo);
				marcarBitmapBloqueOcupado(listaTablaNodos, bitmap2, registroNodos2->nodo, primerBloqueLibreNodo2, 1, infoLogger);
				free(bitmap2->contenido);

			    // Agregal al final de la Cola los 2 nodos extraidos
				queue_push(colaNodosConectados, registroNodos1);
				queue_push(colaNodosConectados, registroNodos2);

		    // ------------------------------------------------------------------	    

			free(lstBloquesArchivos->copia1.nodo);
			free(lstBloquesArchivos->copia2.nodo);
			free(lstBloquesArchivos);
		}else{
			break;
		}
	}

	if(hayBloquesContiguos){

		FILE *archivo_yamafs = fopen(pathDestinoCompleto, "w");

		// Guardo los datos en el archivo
		fwrite(string_aux, strlen(string_aux), 1, archivo_yamafs);

		// Cierro el FD
	    fclose(archivo_yamafs);
	}else{

		// Hago el Rollback de los bloques ocupados
	    void _each_elemento2_(nodo_bloque* registroNodoBloqueAux)
		{
			// Marco los Bitmaps como libres
			t_bitmap* bitmap_aux = cargarBitmap(listaTablaNodos, registroNodoBloqueAux->nodo);
			marcarBitmapBloqueLibre(listaTablaNodos, bitmap_aux, registroNodoBloqueAux->nodo, registroNodoBloqueAux->bloque, infoLogger);
			free(bitmap_aux->contenido);
		}
	    list_iterate(listaAuxBloquesMarcadosOcupados, (void*)_each_elemento2_);
	}

	free(arregloDirectoriosFrom);
	free(nombreArchivoFrom);
	free(pathDestinoCompleto);
	free(string_aux);
	free(registroNodoBloque->nodo);
	free(registroNodoBloque);
	list_destroy(listaAuxBloquesMarcadosOcupados);
	free(stringAux);

    return hayBloquesContiguos;
}

// Envio un Bloque a un Nodo para que lo persista
int  enviarBloqueNodo(t_list* listaNodosConectados, int bloque, char* nombreNodo, int bloqueDestino, char* archivoFrom, int bytesDesde, int bytesHasta, t_log* infoLogger){

	log_info(infoLogger, "Iniciando envio del Bloque %s al Nodo %s en el Bloque %d desde el Bytes %d Hasta %d. Total de Bytes: %d", string_itoa(bloque), nombreNodo, bloqueDestino, bytesDesde,  bytesHasta, (bytesHasta-bytesDesde+1));

	// Busco cual es el socket del Nodo
	int socketNodo = obtenerSocketNodo(listaNodosConectados, nombreNodo);

	// Serializo el archivo a enviar
	Paquete paquete =srlz_bloque_archivo('F', COPIAR_BLOQUES, archivoFrom, bytesDesde, bytesHasta, bloqueDestino);
	int bytes_enviados = send(socketNodo,paquete.buffer,paquete.tam_buffer,0);

	if(bytes_enviados != -1){
		log_info(infoLogger, "Bloque enviado con éxito al Nodo %s", nombreNodo);
	}

	free(paquete.buffer);

	return bytes_enviados;
}

// Recibo un Bloque de un Nodo
char* recibirBloqueNodo(t_list* listaNodosConectados, char* nombreNodo, int bloque, int tamanoBloque, t_log* infoLogger){

	log_info(infoLogger, "Pedido del Bloque %d al Nodo %s. Tamaño del Bloque: %d", bloque, nombreNodo, tamanoBloque);

	// Busco cual es el socket del Nodo
	int socketNodo = obtenerSocketNodo(listaNodosConectados, nombreNodo);

	// Serializo la solicitud de pedido de bloque a un Nodo y se la envio
	Paquete paquete =srlz_solicitud_pedido_bloque('F', PEDIDO_BLOQUES, bloque, tamanoBloque);
	send(socketNodo,paquete.buffer,paquete.tam_buffer,0);
	free(paquete.buffer);

	// Recibo el bloque solicitado
	Encabezado encabezado=recibir_header(&socketNodo);

	paquete=recibir_payload(&socketNodo,&encabezado.tam_payload);
	t_bloque_contenido contenido_bloque=dsrlz_bloque_contenido(paquete.buffer);

	log_info(infoLogger, "Bloque recibido del Nodo %s", nombreNodo);

	free(paquete.buffer);

	return contenido_bloque.contenido_archivo;
}

// Cargo el Bitmap de un Nodo
t_bitmap* cargarBitmap(t_list* listaTablaNodos, char* nombreNodo){

	char* nombreArchivoBitmap = string_new();
	nombreArchivoBitmap = string_from_format("metadata/bitmaps/%s.dat", nombreNodo);

	FILE* bitmap_f = fopen(nombreArchivoBitmap, "r");

	int tamanoBitmap = 0;
	t_list* listaux;

	// Obtengo el tamano del Nodo
	void find_nodo(t_nodos* registroTablaNodos){

		if(strcmp(registroTablaNodos->nodo,nombreNodo)==0 ){
			tamanoBitmap = registroTablaNodos->tamano;
		}
	}
	listaux = list_filter(listaTablaNodos,(void*)find_nodo);


	t_bitmap* bitmap;
	bitmap = malloc(sizeof(t_bitmap));
	bitmap->tamano = tamanoBitmap;

	int total_bytes = fread(bitmap->contenido, sizeof(char) , tamanoBitmap, bitmap_f);

	// Levanto en memoria el Bitmap del Nodo
   	if( total_bytes != tamanoBitmap){
   		printf("Fallo en lectura de Bitmap del Nodo %s\n", nombreNodo);
   	}

	// Cierro el FD
	fclose(bitmap_f);
	list_destroy(listaux);
	free(nombreArchivoBitmap);

	return bitmap;
}

void marcarBitmapBloqueOcupado(t_list* listaTablaNodos, t_bitmap* bitmap, char* nombreNodo, int bloque_inicio, int total_bloques, t_log* infoLogger){

	int i;
	for (i = bloque_inicio; i < (bloque_inicio + total_bloques); i++) { 
		bitmap->contenido[i]='1';
	}

	// Persisto el Bitmap
	persistirBitmap(listaTablaNodos, bitmap, nombreNodo, infoLogger);
}

void marcarBitmapBloqueLibre(t_list* listaTablaNodos, t_bitmap* bitmap, char* nombreNodo, int bloque, t_log* infoLogger){

	bitmap->contenido[bloque]='0';

	// Persisto el Bitmap
	persistirBitmap(listaTablaNodos, bitmap, nombreNodo, infoLogger);

}

/**************************************************
 *
 * Determino el tamano en bytes de un bloque de una archivo de Texto
 *
 **************************************************/
int tamanoBloqueArchivoTexto(char* archivoFrom, int bytesDesde){

	// Defino el archivo binario - directorios.dat
    FILE *archivo = fopen(archivoFrom, "r");
	//char buf[1024]; 
	int contadorBytes = 0, ultimoEnter = 0, anteUltimoEnter = 0;

	struct stat s;
	stat(archivoFrom, &s);
	int tamanoTotalArchivo = s.st_size; 

    int caracter;

	// Si se pudo posicionar dentro del archivo
	if(fseek( archivo, bytesDesde, SEEK_SET ) == 0){


	    while ((caracter = fgetc(archivo)) != EOF) {

			// Cuento los bytes del bloque actual
			contadorBytes++;

			if(caracter == '\n'){

//printf("Linea:%s\n", buf);
				anteUltimoEnter = ultimoEnter;
				ultimoEnter = contadorBytes;

				if(contadorBytes > TAM_BLOQUE){
					contadorBytes = anteUltimoEnter;			
					break;
				}
			}
	    }

//printf("bytesDesde: %d contadorBytes: %d\n",bytesDesde, contadorBytes );	    




	}

	// Cierro el FD
	fclose(archivo);

	return contadorBytes;
}

/**************************************************
 *
 * Determino si un Directorio existe
 *
 **************************************************/
bool existeDirectorioYama(char* pathDirectorio, t_list* listaTablaDirectorios){

	t_directorios* registroTablaDirectorios = NULL;
	//registroTablaDirectorios = malloc(sizeof(t_directorios));
	int cantDirectorios, indice;
	char** arregloDirectorios = NULL;

	// Separo el pathDirectorio en todos los subdirectorios posibles
	arregloDirectorios = string_split(pathDirectorio, "/");

	cantDirectorios = cantidadDirectoriosPath(pathDirectorio);

	// Recorro cada nodo del path
	for (indice = 0; indice  < cantDirectorios; indice=indice+1 ) {
 
		// Existe Hijo(X) con Padre(Y) ?
		if(indice == 0){
			registroTablaDirectorios = ExisteNodoDirectorioYama(listaTablaDirectorios, arregloDirectorios[indice], "/");
		}else{
			registroTablaDirectorios = ExisteNodoDirectorioYama(listaTablaDirectorios, arregloDirectorios[indice], arregloDirectorios[indice-1]);
		}

		// Si no existe un hijo, ya no existe todos el directorio
		if(registroTablaDirectorios == NULL ){
			//free(registroTablaDirectorios);
			return false;
		}

		//free(registroTablaDirectorios);
	}

	free(arregloDirectorios);
	//free(registroTablaDirectorios);

	return true;
}

// Obtengo el MD5 de un archivo en YamaFS
char* obtenerMD5ArchivoYama(char *pathArchivo, t_list* listaTablaDirectorios, t_list* listaNodosConectados, t_log* infoLogger){

	int cantDirectorios, indice;
	char** arregloDirectorios = NULL;
	char* nombreArchivo = NULL;

	// Separo el pathArchivo en todos los subdirectorios posibles
	arregloDirectorios = string_split(pathArchivo, "/");
	cantDirectorios = cantidadDirectoriosPath(pathArchivo);

	// Obtengo el nombre del archivo
	nombreArchivo = malloc(strlen(arregloDirectorios[cantDirectorios-1])+1);
	strcpy(nombreArchivo,arregloDirectorios[cantDirectorios-1]);

	// Obtengo el indice del Directorio en YamaFS
	indice = obtenerIndiceDirectorioYama( string_substring(pathArchivo, 0, string_length(pathArchivo) - string_length(nombreArchivo) - 1), listaTablaDirectorios);

	// Obtengo los nodos que tienen sus bloques
	t_config* archivoFS = config_create(string_from_format("metadata/archivos/%d/%s", indice, nombreArchivo));	
	
	// Determino la cantidad de bloques que tiene el archivo
	int total_bloques = (config_get_int_value(archivoFS,"TAMANIO") / TAM_BLOQUE );

	// Si el tamano no es entero, lo redondeo para arriba
	if(config_get_int_value(archivoFS,"TAMANIO") % TAM_BLOQUE) { total_bloques = total_bloques+1; }

	int bloque = 0, tamanoBloque = 0, nro_bloque;
	char* contenidoBloque;
	FILE* fr;
	char *cmdMD5 = string_new();
	string_append(&cmdMD5,"cat ");

	// Guardo la informacion de cada bloque
	for (bloque = 0; bloque  < total_bloques; bloque=bloque+1 ) {

		// Obtengo la informacion de la Copia0
		char** arregloCopia0;
		arregloCopia0 = string_get_string_as_array(config_get_string_value(archivoFS,string_from_format("BLOQUE%sCOPIA0", string_itoa(bloque))));

		tamanoBloque = config_get_int_value(archivoFS,string_from_format("BLOQUE%sBYTES", string_itoa(bloque) ));
		nro_bloque = atoi(arregloCopia0[1]);

		// Solicito el Bloque al Nodo de la Copia0
		//contenidoBloque = malloc(tamanoBloque);
		contenidoBloque = recibirBloqueNodo(listaNodosConectados, arregloCopia0[0], nro_bloque, tamanoBloque, infoLogger);

		// Defino el archivo temporal para guardar el bloque (solo para debug)
		char* nombreArchivoTmp;
		nombreArchivoTmp = string_from_format("files/%s_bloque_%d.tmp", arregloCopia0[0], nro_bloque);

		// Voy generando el commnad para calcular el MD5
		string_append_with_format(&cmdMD5,"%s ", nombreArchivoTmp);


		fr = fopen(nombreArchivoTmp, "w");
		if(fr == NULL){
			printf("No se pudo crear el archivo temporal con el contenido del bloque.\n");
		}else{
			fwrite(contenidoBloque, tamanoBloque, sizeof(char), fr);
		}
		fclose(fr); 

		free(nombreArchivoTmp);
		free(arregloCopia0);
		free(contenidoBloque);	
	}

	// ***************** Obtengo el MD5 con MD5SUM
		string_append_with_format(&cmdMD5," > files/bloque_%s.tmp ", nombreArchivo);
		system(cmdMD5);


//printf("cmdMD5: %s\n", cmdMD5);
		cmdMD5 = string_from_format("md5sum files/bloque_%s.tmp > files/md5_%s.tmp", nombreArchivo, nombreArchivo);
		system(cmdMD5);

		char* md5 = string_new(); 
		md5 = string_from_format("00000000000000000000000000000000");

		fr = fopen(string_from_format("files/md5_%s.tmp", nombreArchivo), "r");	
		int fs_block_sz = fread(md5, sizeof(char), string_length(md5), fr);
		md5[string_length(md5)] = '\0';
		fclose(fr);

	// ******************************************************************

    // Libero
	config_destroy(archivoFS);
	free(cmdMD5);
	free(nombreArchivo);

	return md5;
}



// Muestro informacion sobre un archivo en YamaFS
void mostrarInfoArchivoYama(char *pathArchivo, t_list* listaTablaDirectorios){

	int cantDirectorios, indice;
	char** arregloDirectorios = NULL;
	char* nombreArchivo = NULL;

	// Separo el pathArchivo en todos los subdirectorios posibles
	arregloDirectorios = string_split(pathArchivo, "/");
	cantDirectorios = cantidadDirectoriosPath(pathArchivo);

	// Obtengo el nombre del archivo
	nombreArchivo = malloc(strlen(arregloDirectorios[cantDirectorios-1])+1);
	strcpy(nombreArchivo,arregloDirectorios[cantDirectorios-1]);


	indice = obtenerIndiceDirectorioYama( string_substring(pathArchivo, 0, string_length(pathArchivo) - string_length(nombreArchivo) - 1), listaTablaDirectorios);

	// Leo el detalle de archivo configuracion
	t_config* archivoFS = config_create(string_from_format("metadata/archivos/%d/%s", indice, nombreArchivo));	

	printf("TAMANIO: %s\n", config_get_string_value(archivoFS,"TAMANIO"));
	printf("TIPO: %s\n", config_get_string_value(archivoFS,"TIPO"));
	
	// Determino la cantidad de bloques que tiene el archivo
	int total_bloques = (config_get_int_value(archivoFS,"TAMANIO") / TAM_BLOQUE );

	// Si el tamano no es entero, lo redondeo para arriba
	if(config_get_int_value(archivoFS,"TAMANIO") % TAM_BLOQUE) { total_bloques = total_bloques+1; }


	int bloque = 0;

	// Guardo la informacion de cada bloque
	for (bloque = 0; bloque  < total_bloques; bloque=bloque+1 ) {

		printf("BLOQUE%sBYTES: %s\n", string_itoa(bloque), config_get_string_value(archivoFS,string_from_format("BLOQUE%sBYTES", string_itoa(bloque) )));
		printf("BLOQUE%sCOPIA0: %s\n", string_itoa(bloque), config_get_string_value(archivoFS,string_from_format("BLOQUE%sCOPIA0", string_itoa(bloque))));
		printf("BLOQUE%sCOPIA1: %s\n", string_itoa(bloque), config_get_string_value(archivoFS,string_from_format("BLOQUE%sCOPIA1", string_itoa(bloque))));
	}

    // Libero
	config_destroy(archivoFS);    
}


/**************************************************
 *
 * Muestro el contenido de un Archivo por pantalla
 *
 **************************************************/
void mostrarArchivoYama(char *pathArchivo, t_list* listaTablaDirectorios, t_list* listaTablaNodos, t_list* listaNodosConectados, t_log* infoLogger){

	t_list* listaDatosBloques = list_create();
	char* contenidoBloque;

	// Obtengo los datos de los bloques de archivo
	listaDatosBloques = obtenerDatosBloquesArchivo(pathArchivo, false, listaTablaDirectorios, listaTablaNodos, infoLogger);

    void _each_elemento_(t_bloques_archivos* registroBloque)
	{

		printf("\nBloque: %d\n", registroBloque->bloque);
		printf("bytes_ocupados: %d\n", registroBloque->bytes_ocupados);
		printf("copia1 Nodo: %s Bloque: %d IP: %s Port: %d\n", registroBloque->copia1.nodo, registroBloque->copia1.bloque,registroBloque->copia1.ip,registroBloque->copia1.port);
		printf("copia2 Nodo: %s Bloque: %d IP: %s Port: %d\n", registroBloque->copia2.nodo, registroBloque->copia2.bloque,registroBloque->copia2.ip,registroBloque->copia2.port);


		contenidoBloque = recibirBloqueNodo(listaNodosConectados, registroBloque->copia1.nodo, registroBloque->copia1.bloque, registroBloque->bytes_ocupados, infoLogger);

		printf("%s\n", contenidoBloque);
		//log_info(infoLogger, "Contenido del Bloque %d\n%s", registroBloque->copia1.bloque, contenidoBloque);
		free(contenidoBloque);

	}
    list_iterate(listaDatosBloques, (void*)_each_elemento_);

    list_destroy(listaDatosBloques);


}

/**************************************************
 *
 * Dado un path completo, obtengo el nombre del archivo
 *
 **************************************************/
char* obtenerArchivoPath(char* pathArchivo){

	int cantDirectorios;
	char** arregloDirectorios = NULL;
	char* nombreArchivo = string_new();

	// Separo el pathArchivo en todos los subdirectorios posibles
	arregloDirectorios = string_split(pathArchivo, "/");
	cantDirectorios = cantidadDirectoriosPath(pathArchivo);

	// Obtengo el nombre del archivo
	nombreArchivo = malloc(strlen(arregloDirectorios[cantDirectorios-1])+1);
	strcpy(nombreArchivo,arregloDirectorios[cantDirectorios-1]);

	return nombreArchivo;
}

/**************************************************
 *
 * Determino si existe un Archivo en YamaFS y devuelvo el indice del directorio que lo contiene
 *
 **************************************************/
int existeArchivoYama(char *pathArchivo, t_list* listaTablaDirectorios){

	int cantDirectorios, indice;
	char** arregloDirectorios = NULL;
	char* nombreArchivo = NULL;
	char* nombreArchivoMetadataAux = NULL;

	// Separo el pathArchivo en todos los subdirectorios posibles
	arregloDirectorios = string_split(pathArchivo, "/");
	cantDirectorios = cantidadDirectoriosPath(pathArchivo);

	// Obtengo el nombre del archivo
	nombreArchivo = malloc(strlen(arregloDirectorios[cantDirectorios-1])+1);
	strcpy(nombreArchivo,arregloDirectorios[cantDirectorios-1]);


	if(existeDirectorioYama(string_substring(pathArchivo, 0, string_length(pathArchivo) - string_length(nombreArchivo) - 1), listaTablaDirectorios)){

		indice = obtenerIndiceDirectorioYama( string_substring(pathArchivo, 0, string_length(pathArchivo) - string_length(nombreArchivo) - 1), listaTablaDirectorios);

		nombreArchivoMetadataAux = string_from_format("metadata/archivos/%d/%s", indice, nombreArchivo);
		if(existeArchivo(nombreArchivoMetadataAux) ){
			free(nombreArchivoMetadataAux);
			free(nombreArchivo);
			return indice;
		}

	}

	free(nombreArchivoMetadataAux);
	free(nombreArchivo);
	return -100;
}

void persistirTablaDirectorio(t_list* listaTablaDirectorios, t_log* infoLogger){//Persistir la Tabla de Directorio

	int tamlista = list_size(listaTablaDirectorios);
	bool errorEscritura = false;

	// Defino el archivo binario - directorios.dat
    FILE *directorios_dat = fopen("metadata/directorios.dat", "wb");

    if(directorios_dat == NULL) {
        log_info(infoLogger, "[Error] Falló la apertura del archivo metadata/directorios.dat" );
    	return;
    }

    void _each_elemento_(t_directorios* registroTablaDirectorios)
	{
		// Serializo el registro para poder guardarlo
		Paquete paquete = srlz_tablaDirectorio(registroTablaDirectorios);

		// Persisto los datos
    	if(fwrite(paquete.buffer, paquete.tam_buffer, 1, directorios_dat) != 1){
    	    log_info(infoLogger, "[Error] Falló la escritura sobre el archivo metadata/directorios.dat" );

    	    if(!errorEscritura)
    	    	errorEscritura = true;
    	}
    	free(paquete.buffer);
	}
    list_iterate(listaTablaDirectorios, (void*)_each_elemento_);

	if(!errorEscritura){
	    log_info(infoLogger, "Archivo metadata/directorios.dat actualizado. Total elementos: %d",tamlista );
	}

	// Cierro el FD
    fclose(directorios_dat);

}

void cargarTablaDirectorio(t_list* listaTablaDirectorios, t_log* infoLogger){//Cargar la Tabla de Directorio desde directorios.dat

	char * nombreDirectorio;
	int index = 0, padre = 0, tamDirectorio = 0;
	t_directorios* registroTablaDirectorios = NULL;

	// Defino el archivo binario - directorios.dat
    FILE *directorios_dat = fopen("metadata/directorios.dat", "rb");

    if(!directorios_dat){
    	log_info(infoLogger, "[Error] No se pudo abrir el archivo metadata/directorios.dat" );
    	return;
    }

    log_info(infoLogger, "Reestableciendo el FS de metadata/directorios.dat" );

	while(1) {
    	if(feof(directorios_dat)) break;

    	if(fread(&index, sizeof(int) ,1, directorios_dat) == 1){
        	if(fread(&padre, sizeof(int) ,1, directorios_dat) == 1){
            	if(fread(&tamDirectorio, sizeof(int) ,1, directorios_dat) == 1){

                    // Leo el nombre del directorio (dinamico)
                    nombreDirectorio = malloc(sizeof(char) * tamDirectorio + 1);

                    if(fread(nombreDirectorio, sizeof(char) * tamDirectorio ,1, directorios_dat) == 1){
                		nombreDirectorio[tamDirectorio]='\0';
                		
                		registroTablaDirectorios = malloc(sizeof(t_directorios));

                		// Cargo el Registro de directorio
                		strcpy( registroTablaDirectorios->nombre ,nombreDirectorio);
                		registroTablaDirectorios->nombre[strlen(nombreDirectorio)] = '\0';
                		registroTablaDirectorios->padre = padre;
                		registroTablaDirectorios->index = index;

                		// Agrego el Registro en la Lista
                		list_add(listaTablaDirectorios,registroTablaDirectorios);

                		//free(registroTablaDirectorios);
                    }

                    free(nombreDirectorio);
            	}
        	}
    	}

    	if(feof(directorios_dat)) break;
    }

    log_info(infoLogger, "FS Reestablecido desde metadata/directorios.dat" );

	// Cierro el FD
	fclose(directorios_dat);
    //free(registroTablaDirectorios);
}

bool existeArchivo(char *filename){//Funcion para determinar si un archivo local existe

    FILE *archivo = fopen(filename, "rb");

    if(!archivo){
    	return false;
    }else{
        fclose(archivo);
    	return true;
    }
}

int cantidadDirectoriosPath(char* pathDirectorio){//Devuelve la cantidad de directorios que hay en un path
	char** arregloDirectorio1 = string_split(pathDirectorio, "/");
	int contadorDirectorios = 0;


	void _each_directory_(char* directorio)
	{
		contadorDirectorios = contadorDirectorios + 1;
	}

	string_iterate_lines(arregloDirectorio1, (void*)_each_directory_);

	free(arregloDirectorio1);

	return contadorDirectorios;
}

void transformacion(t_solicitud_reduccion_local* solicitud){

}



t_solicitud_transformacion algoritmo(t_list* lista,t_bloques_archivos* bloque,int job){//(t_bloques_archivos*  bloque, t_list* lista, t_solicitud_transformacion*  solicitud){
	int cant1=0;
	int cant2=0;
	t_solicitud_transformacion solicitud;
   	bool find_nodo1_(t_estados* estado)
        		{
        			return (strcmp(estado->nodo ,bloque->copia1.nodo)==0 && estado->estado==1);
        			//return (strcmp(estado->nodo ,nodo1)==0 );
        		}
   	bool find_nodo2_(t_estados* estado )
        		{
        			return (strcmp(estado->nodo,bloque->copia2.nodo)==0 && estado->estado==1);
        			//return (strcmp(estado->nodo ,nodo2)==0 );
        		}
    cant1=list_count_satisfying(lista,(void*)find_nodo1_);
    cant2=list_count_satisfying(lista,(void*)find_nodo2_);

        	if (cant1<cant2){
        		solicitud.bloque = bloque->copia1.bloque;
        		solicitud.bytes_ocupados=bloque->bytes_ocupados;
        		solicitud.nodo=malloc(strlen(bloque->copia1.nodo)+1);
        		strcpy(solicitud.nodo,bloque->copia1.nodo);
        		solicitud.nodo[strlen(bloque->copia1.nodo)]='\0';
        		solicitud.worker_ip=malloc(strlen(bloque->copia1.ip)+1);
        		strcpy(solicitud.worker_ip,bloque->copia1.ip);
        		solicitud.worker_ip[strlen(bloque->copia1.ip)]='\0';
        		//solicitud.archivo_tmp=malloc(strlen("archivotransformacion")+1);
        		//strcpy(solicitud.archivo_tmp,"archivotransformacion");
        		crearNOmbretemporalTransformacion(job,&solicitud);
        		solicitud.worker_port=bloque->copia1.port;

        		//return nodo1;
        	}else if(cant1>cant2){
        		solicitud.bloque = bloque->copia2.bloque;
           		solicitud.bytes_ocupados=bloque->bytes_ocupados;
           		solicitud.nodo=malloc(strlen(bloque->copia2.nodo)+1);
          		strcpy(solicitud.nodo,bloque->copia2.nodo);
          		solicitud.nodo[strlen(bloque->copia2.nodo)]='\0';
          		solicitud.worker_ip=malloc(strlen(bloque->copia2.ip)+1);
           		strcpy(solicitud.worker_ip,bloque->copia2.ip);
           		solicitud.worker_ip[strlen(bloque->copia2.ip)]='\0';
           		//solicitud.archivo_tmp=malloc(strlen("archivotransformacion"));
           		//strcpy(solicitud.archivo_tmp,"archivotransformacion");
           		crearNOmbretemporalTransformacion(job,&solicitud);
           		solicitud.worker_port=bloque->copia2.port;

           		//return nodo2;
        	}else{
        		solicitud= MenosTrabajoRealizado( bloque, lista,job);
        	}

	return solicitud;

}
void mover(worker_algoritmo** puntero){
	*puntero=(*puntero)->next;
}

void algoritmo2(t_list* lista,worker_algoritmo *punteroclock,t_bloques_archivos* bloque){
	nodo_bloque bloquenodo;
	if(strcmp(bloque->copia1.nodo,punteroclock->nodo)==0){
		bloquenodo=bloque->copia1;
		punteroclock->prioridad=punteroclock->prioridad-1;
		punteroclock->canttrabajo++;
		//*punteroclock=*punteroclock->next;
		printf("%s\n",punteroclock->nodo);
	}

}

nodo_bloque algoritmoclock(t_bloques_archivos* bloque,worker_algoritmo* lista,worker_algoritmo** punteroclock,int base){
	worker_algoritmo* puntero;
	worker_algoritmo* punterocabeza;
	/*t_list* lista;
	t_link_element* elemento;

	elemento=lista->head;*/
	nodo_bloque bloquenodo;
	punterocabeza=malloc(sizeof(worker_algoritmo));
	puntero=malloc(sizeof(worker_algoritmo));
	punterocabeza=lista;

	if(strcmp(bloque->copia1.nodo,(*punteroclock)->nodo)==0){
		bloquenodo=bloque->copia1;
		(*punteroclock)->prioridad=(*punteroclock)->prioridad-1;
		(*punteroclock)->canttrabajo++;
		//*punteroclock=*punteroclock->next;
		//printf("%s\n",punteroclock->nodo);

		if((*punteroclock)->next==NULL){
			*punteroclock=punterocabeza;
		}else{
			//puntero=punteroclock;
			*punteroclock=(*punteroclock)->next;
		}
		if((*punteroclock)->prioridad==0){
			(*punteroclock)->prioridad=base;
		}

	}else if(strcmp(bloque->copia2.nodo,(*punteroclock)->nodo)==0){

		bloquenodo=bloque->copia2;
		(*punteroclock)->prioridad=(*punteroclock)->prioridad-1;
		(*punteroclock)->canttrabajo++;

		if((*punteroclock)->next==NULL){
			*punteroclock=punterocabeza;
		}else{
			*punteroclock=(*punteroclock)->next;
				}
		if((*punteroclock)->prioridad==0){
			(*punteroclock)->prioridad=base;
		}
	}else{

		puntero=(*punteroclock)->next;

		int encontrado=1;

		while(encontrado==1){
			if (puntero==NULL){
				puntero=punterocabeza;
			}

			if(puntero->prioridad==0){
				puntero->prioridad=base;
			}


			if(strcmp(puntero->nodo,bloque->copia1.nodo)==0 && puntero->prioridad>0){

				bloquenodo=bloque->copia1;
				puntero->prioridad--;
				puntero->canttrabajo++;
				encontrado=0;


			}else if(strcmp(puntero->nodo,bloque->copia2.nodo)==0 && puntero->prioridad>0){

				bloquenodo=bloque->copia2;
				puntero->prioridad--;
				puntero->canttrabajo++;
				encontrado=0;

			}

			puntero=puntero->next;


		}
	}
return bloquenodo;

}
void agregar(worker_algoritmo* worker,char* nodo){



}

t_solicitud_transformacion crearsolicitudTransformacion(t_bloques_archivos* bloque,nodo_bloque* bloquenodo,int job){
	t_solicitud_transformacion solicitud;
	solicitud.bloque = bloquenodo->bloque;
	solicitud.bytes_ocupados=bloque->bytes_ocupados;
	solicitud.nodo=malloc(strlen(bloquenodo->nodo)+1);
	strcpy(solicitud.nodo,bloquenodo->nodo);
	solicitud.nodo[strlen(bloquenodo->nodo)]='\0';
	solicitud.worker_ip=malloc(strlen(bloquenodo->ip)+1);
	strcpy(solicitud.worker_ip,bloquenodo->ip);
	solicitud.worker_ip[strlen(bloquenodo->ip)]='\0';
	crearNOmbretemporalTransformacion(job,&solicitud);
	solicitud.worker_port=bloquenodo->port;
	return solicitud;
}

int  funciondisponibilidadWclock(int base,int cargamaxima,int cargaactual){
	int disponibilidad=0;
	return disponibilidad=base + cargamaxima - cargaactual;
}
int buscarelmaximocarga(worker_algoritmo* lista ){
	worker_algoritmo* punteroauxiliar;
	punteroauxiliar=malloc(sizeof(worker_algoritmo));
	int max=lista->canttrabajo;
	punteroauxiliar=lista;
	while(punteroauxiliar!=NULL){
		if(lista->canttrabajo>max){
			max=lista->canttrabajo;
		}
		punteroauxiliar=punteroauxiliar->next;
	}
	return max;
}
worker_algoritmo* aplicarfunciondisponibilidadWclock(worker_algoritmo* lista ,int base){
	worker_algoritmo* punteroauxiliar;
	punteroauxiliar=malloc(sizeof(worker_algoritmo));
	worker_algoritmo* punteroclock;
	punteroclock=malloc(sizeof(worker_algoritmo));
	punteroauxiliar=lista;
	int disponibilidadmax=0;
	int maximocarga=buscarelmaximocarga(lista);
		while(punteroauxiliar!=NULL){
			punteroauxiliar->prioridad=funciondisponibilidadWclock(base,maximocarga,punteroauxiliar->canttrabajo);
			if(punteroauxiliar->prioridad>disponibilidadmax){
				disponibilidadmax=punteroauxiliar->prioridad;
				punteroclock=punteroauxiliar;
			}
			punteroauxiliar=punteroauxiliar->next;
		}
		//free(punteroauxiliar);
		return punteroclock;

}
worker_algoritmo* aplicarfunciondisponibilidadClock(worker_algoritmo* lista ,int base){
	worker_algoritmo* punteroauxiliar;
	worker_algoritmo* punteroclock;
	punteroclock=malloc(sizeof(worker_algoritmo));
	punteroauxiliar=malloc(sizeof(worker_algoritmo));

	punteroauxiliar=lista;

	int disponibilidadmax=0;
	int maximocarga=0;
		while(punteroauxiliar!=NULL){

			punteroauxiliar->prioridad=funciondisponibilidadWclock(base,0,0);

			if(punteroauxiliar->prioridad>disponibilidadmax){
				disponibilidadmax=punteroauxiliar->prioridad;
				punteroclock=punteroauxiliar;

			}
			punteroauxiliar=punteroauxiliar->next;
		}
	//free(punteroauxiliar);
	return punteroclock;
}

void algoritmoWclock(worker_algoritmo* lista ,worker_algoritmo* punteroclock,int base,t_bloques_archivos* bloque){
	aplicarfunciondisponibilidadWclock(lista,base);

}
worker_algoritmo* aplicardisponibilidad(char* nombrealgoritmo,worker_algoritmo* lista ,int base){
	if(strcmp(nombrealgoritmo,"CLOCK")==0){
		return aplicarfunciondisponibilidadClock( lista , base);
	}else{
		return aplicarfunciondisponibilidadWclock(lista ,base);
	}
}

void agregardatosnodo(worker_algoritmo** lista ,t_nodos* nodoagregar,t_list* listanodos){
	worker_algoritmo* nuevo;
	nuevo=malloc(sizeof(worker_algoritmo));
	worker_algoritmo* puntero;
	nuevo=malloc(sizeof(worker_algoritmo));
	puntero=*lista;
	nuevo->nodo=malloc(strlen(nodoagregar->nodo)+1);
	strcpy(nuevo->nodo,nodoagregar->nodo);
	nuevo->nodo[strlen(nodoagregar->nodo)]='\0';
	nuevo->prioridad=0;
	nuevo->canttrabajo=0;
	nuevo->next=NULL;
	if (*lista==NULL){
		*lista=nuevo;
	}else{
		while(puntero->next){
			puntero=puntero->next;

		}
		puntero->next=nuevo;
	}

	// Habria que hacer free de nuevo

	dato_nodo* nododelalista;
	nododelalista=malloc(sizeof(dato_nodo));
	nododelalista->port=nodoagregar->port_worker;
	nododelalista->ip=malloc(strlen(nodoagregar->ip)+1);
	strcpy(nododelalista->ip,nodoagregar->ip);
	nododelalista->ip[strlen(nodoagregar->ip)]='\0';

	nododelalista->nodo=malloc(strlen(nodoagregar->nodo)+1);
	strcpy(nododelalista->nodo,nodoagregar->nodo);
	nododelalista->nodo[strlen(nodoagregar->nodo)]='\0';
	nododelalista->cantidadtrabajo=0;
	list_add(listanodos,nododelalista);

}




void crearNOmbretemporalTransformacion(int job,t_solicitud_transformacion* solicitud){
	char* nombre=string_new();
	string_append(&nombre,"/tmp/job");
	string_append_with_format(&nombre,"%i-",job);
	string_append(&nombre,solicitud->nodo);
	string_append(&nombre,"-temp");
	string_append_with_format(&nombre,"%i",solicitud->bloque);
	solicitud->archivo_tmp=malloc(strlen(nombre)+1);
	strcpy(solicitud->archivo_tmp,nombre);
	solicitud->archivo_tmp[strlen(nombre)]='\0';
	free(nombre);
}
void crearNombreTemporalReduccionLocal(int id_master,t_solicitud_reduccion_local* solicitud,int job){
	char* nombre=string_new();
	string_append(&nombre,"/tmp/Master");
	string_append_with_format(&nombre,"%i",id_master);
	string_append(&nombre,"-job");
	string_append_with_format(&nombre,"%i",job);
	string_append(&nombre,"-RL");
	string_append_with_format(&nombre,"%s",solicitud->nodo);
	solicitud->archivo_tmp_reduccion_local=malloc(strlen(nombre)+1);
	strcpy(solicitud->archivo_tmp_reduccion_local,nombre);
	solicitud->archivo_tmp_reduccion_local[strlen(nombre)]='\0';

}






t_solicitud_transformacion MenosTrabajoRealizado(t_bloques_archivos* bloque,t_list* lista,int job){
	int cant1=0;
	int cant2=0;
	t_solicitud_transformacion solicitud;
	bool find_1(t_estados* estado){
		return (strcmp(estado->nodo,bloque->copia1.nodo)==0 && estado->estado==3);
	}
	bool find_2(t_estados* estado){
		return (strcmp(estado->nodo,bloque->copia2.nodo)==0 && estado->estado==3);
	}
	cant1=list_count_satisfying(lista,(void*)find_1);
	cant2=list_count_satisfying(lista,(void*)find_2);
	if (cant1<cant2){
		solicitud.bloque = bloque->copia1.bloque;
		solicitud.bytes_ocupados=bloque->bytes_ocupados;
		solicitud.nodo=malloc(strlen(bloque->copia1.nodo)+1);
   		strcpy(solicitud.nodo,bloque->copia1.nodo);
   		solicitud.nodo[strlen(bloque->copia1.nodo)]='\0';
   		solicitud.worker_ip=malloc(strlen(bloque->copia1.ip)+1);
		strcpy(solicitud.worker_ip,bloque->copia1.ip);
		solicitud.worker_ip[strlen(bloque->copia1.ip)]='\0';
   		//solicitud.archivo_tmp=malloc(strlen("archivotransformacion")+1);
		//strcpy(solicitud.archivo_tmp,"archivotransformacion");
		crearNOmbretemporalTransformacion(job,&solicitud);
		solicitud.worker_port=bloque->copia1.port;

		//return nodo1;
	}else if (cant1>cant2){
		solicitud.bloque = bloque->copia2.bloque;
		solicitud.bytes_ocupados=bloque->bytes_ocupados;
		solicitud.nodo=malloc(strlen(bloque->copia2.nodo)+1);
   		strcpy(solicitud.nodo,bloque->copia2.nodo);
   		solicitud.nodo[strlen(bloque->copia2.nodo)]='\0';
 		solicitud.worker_ip=malloc(strlen(bloque->copia2.ip)+1);
 		strcpy(solicitud.worker_ip,bloque->copia2.ip);
 		solicitud.worker_ip[strlen(bloque->copia2.ip)]='\0';
 		//solicitud.archivo_tmp=malloc(strlen("archivotransformacion"));
		//strcpy(solicitud.archivo_tmp,"archivotransformacion");
 		crearNOmbretemporalTransformacion(job,&solicitud);
 		solicitud.worker_port=bloque->copia2.port;


		//return nodo2;
	}else{

		solicitud.bloque = bloque->copia1.bloque;
		solicitud.bytes_ocupados=bloque->bytes_ocupados;
		solicitud.nodo=malloc(strlen(bloque->copia1.nodo)+1);
		strcpy(solicitud.nodo,bloque->copia1.nodo);
		solicitud.nodo[strlen(bloque->copia1.nodo)]='\0';
		solicitud.worker_ip=malloc(strlen(bloque->copia1.ip)+1);
		strcpy(solicitud.worker_ip,bloque->copia1.ip);
		solicitud.worker_ip[strlen(bloque->copia1.ip)]='\0';
		//solicitud.archivo_tmp=malloc(strlen("archivotransformacion"));
		//strcpy(solicitud.archivo_tmp,"archivotransformacion");
		crearNOmbretemporalTransformacion(job,&solicitud);
		solicitud.worker_port=bloque->copia1.port;
		//return nodo1;

	}
	return solicitud;
}
datomaster* buscarIdMaster(int mastersocket,t_list* listamasters){
	datomaster* master=NULL;
	//master=malloc(sizeof(datomaster));
	master=malloc(sizeof(int)*2);
	int id=0;

	bool find_master(datomaster* datomaster_aux){

				return (datomaster_aux->socketmaster   == mastersocket );
			}

	master=list_find(listamasters,(void*)find_master);


	return master;
}

void agregarEstadoALista(t_list* listaEstado,int job,t_solicitud_transformacion* solicitud,int master,char* etapa){
	t_estados* estado=NULL;
	estado=malloc(sizeof(t_estados));
	estado->nodo=malloc(strlen(solicitud->nodo)+1);
	strcpy(estado->nodo,solicitud->nodo);
	estado->nodo[strlen(solicitud->nodo)]='\0';
   	estado->archivo_tmp=malloc(strlen(solicitud->archivo_tmp)+1);
   	strcpy(estado->archivo_tmp,solicitud->archivo_tmp);
   	estado->archivo_tmp[strlen(solicitud->archivo_tmp)]='\0';
   	estado->bloque=solicitud->bloque;
   	estado->estado=1;
   	estado->id_job=job;
   	estado->master=master;
   	estado->etapa=malloc(strlen (etapa)+1);
   	strcpy(estado->etapa,etapa);
   	estado->etapa[strlen(etapa)]='\0';
   	list_add(listaEstado,estado);
   	//printf("%d \t %d \t %s \t %d \t %s \t %s \t %d\n", estado->id_job, estado->master, estado->nodo, estado->bloque, estado->etapa, estado->archivo_tmp, estado->estado );

   	showContenidolistaEstados(listaEstado);
   //free(estado->archivo_tmp);
	//free(estado->etapa);
	//free(estado->nodo);
	//free(estado);
}
void actualizarListaMaster(char* archivofinal,int master_id,t_list* listamaster){
	void actualizar(datomaster* masterlista){
				if(masterlista->master==master_id){

						masterlista->nombrearchivoRGfinal=malloc(strlen(archivofinal)+1);
						strcpy(masterlista->nombrearchivoRGfinal,archivofinal);
						masterlista->nombrearchivoRGfinal[strlen(archivofinal)]='\0';

				}
			}
	list_iterate(listamaster,(void*)actualizar);

}

//funciones para la replanificacion

void agregardatosparalaReplanificacion(t_solicitud_transformacion solicitud_t,t_bloques_archivos* bloque,t_list* listaReplanificacion){
	copia_replanificacion* copia;
	copia=malloc(sizeof(copia_replanificacion));
	if (strcmp(bloque->copia1.nodo,solicitud_t.nodo)==0){
		copia->nombrearchivo=malloc(strlen(solicitud_t.archivo_tmp)+1);
		strcpy(copia->nombrearchivo,solicitud_t.archivo_tmp);
		copia->nombrearchivo[strlen(solicitud_t.archivo_tmp)]='\0';
		copia->copia=bloque->copia2;
		copia->bytes_ocupados=solicitud_t.bytes_ocupados;

	}else{
		copia->nombrearchivo=malloc(strlen(solicitud_t.archivo_tmp)+1);
		strcpy(copia->nombrearchivo,solicitud_t.archivo_tmp);
		copia->nombrearchivo[strlen(solicitud_t.archivo_tmp)]='\0';
		copia->copia=bloque->copia1;
		copia->bytes_ocupados=solicitud_t.bytes_ocupados;

	}
	list_add(listaReplanificacion,copia);

}



t_bloques_archivos replanificacion(t_list* listarp,notificacionArchivo* notificacion,worker_algoritmo** punteroclock,worker_algoritmo* lista,int base,int job){

	t_bloques_archivos bloquearchivo;
	copia_replanificacion* copiabuscada;
	copiabuscada=malloc(sizeof(copia_replanificacion));
	nodo_bloque bloquenodo;
	t_solicitud_transformacion solicitud_transformacion;

	bool find(copia_replanificacion* copia1){
		return (strcmp(copia1->nombrearchivo,notificacion->archivo )==0 );
	}
	copiabuscada=list_find(listarp,(void*)find);
	bloquearchivo.bloque=copiabuscada->copia.bloque;
	bloquearchivo.bytes_ocupados=copiabuscada->bytes_ocupados;
	bloquearchivo.copia1=copiabuscada->copia;
	bloquearchivo.copia2=copiabuscada->copia;
	/*bloquenodo=algoritmoclock(&bloquearchivo,lista,punteroclock,base);
	printf("buscando error4\n");
	solicitud_transformacion=crearsolicitudTransformacion(&bloquearchivo,&bloquenodo,job);
	printf("buscando error5\n");
	return solicitud_transformacion;*/
	return bloquearchivo;
}

bool existecopia(t_list* lista,notificacionArchivo* notificacion){

	bool find(copia_replanificacion* copia){
		return(strcmp(copia->nombrearchivo,notificacion->archivo)==0);
	}
	int cant=list_count_satisfying(lista,(void*)find);
	return cant>0;
}





//

t_list* buscarlosArchivosNOdo(char* nodo,t_list* lista){
	t_list* listaaux;
	listaaux=list_create();
	bool find_nodo(t_estados* estado){
			return (strcmp(estado->nodo,nodo)==0 );
		}
	listaaux=list_filter(lista,(void*)find_nodo);
	return listaaux;

}
void actualizarListaEstado(notificacionArchivo* notificacion,t_list* listaEstado){

	void actualizar(t_estados* estado){
		if(strcmp(estado->archivo_tmp,notificacion->archivo)==0){
			if(!notificacion->fallo){
				estado->estado=3;
			}else{
				estado->estado=2;
			}
		}
	}
	list_iterate(listaEstado,(void*)actualizar);
	printf("----------------actualizar ------------\n");
	showContenidolistaEstados(listaEstado);

}
void agregarmasterAlaLista(t_list* listamaster,int master_id,int nuevo_fd){
	datomaster* master=NULL;

	//master =malloc(sizeof(datomaster));
	master=malloc(sizeof(datomaster));
	master->socketmaster=nuevo_fd;
	master->master=master_id;
	master->job=0;
	master->nombrearchivoRGfinal=NULL;

	list_add(listamaster,master);

	//free(master);
}
void agregarjobListaMaster(int job,t_list* listamaster,int socket){
	void actualizar(datomaster* master){
		if(master->socketmaster==socket){

				master->job=job;

		}
	}
	list_iterate(listamaster,(void*)actualizar);
}

t_list* nodoArchivoListo(notificacionArchivo* notificacion,t_list* listaestado){
	t_estados* auxestado;
	auxestado=malloc(sizeof(t_estados));
	//int cant=0,cant1=0;
	//t_list* listaarchivos;
	//listaarchivos=list_create();
	bool findestado(t_estados* estado){
		return (strcmp(estado->archivo_tmp,notificacion->archivo )==0 && strcmp(estado->etapa,"transformacion")==0 && estado->estado==3);
	}
	auxestado=list_find(listaestado,(void*)findestado);
	bool find(t_estados* estado){
		return(strcmp (estado->nodo,auxestado->nodo)==0 && strcmp(estado->etapa,"transformacion")==0 && estado->estado==3 && estado->id_job==auxestado->id_job);
	}

	return list_filter(listaestado,(void*)find);

}
void agregardatosNodo(t_solicitud_transformacion* solicitud,t_list* listanodo){
	dato_nodo* dato;
	dato=NULL;
	dato =malloc(sizeof(dato_nodo));
	//dato->port=0;

	int cant=0;



	if (list_is_empty(listanodo)){
		dato->port=solicitud->worker_port;
		dato->nodo=malloc(strlen(solicitud->nodo)+1);
		strcpy(dato->nodo,solicitud->nodo);
		dato->nodo[strlen(solicitud->nodo)]='\0';
		dato->ip=malloc(strlen(solicitud->worker_ip)+1);
		strcpy(dato->ip,solicitud->worker_ip);
		dato->ip[strlen(solicitud->worker_ip)]='\0';
		dato->cantidadtrabajo=1;
		list_add(listanodo,dato);

		//free(dato->ip);
		//free(dato->nodo);
	//	free(dato);


	}else{
		bool findnodo(dato_nodo* datoNodo){


			return(strcmp(datoNodo->nodo,solicitud->nodo)==0);
		}
		cant=list_count_satisfying(listanodo,(void*)findnodo);
		if(cant==0){
			dato->port=solicitud->worker_port;
			dato->nodo=malloc(strlen(solicitud->nodo)+1);
			strcpy(dato->nodo,solicitud->nodo);
			dato->nodo[strlen(solicitud->nodo)]='\0';
			dato->ip=malloc(strlen(solicitud->worker_ip)+1);
			strcpy(dato->ip,solicitud->worker_ip);
			dato->ip[strlen(solicitud->worker_ip)]='\0';
			dato->cantidadtrabajo=1;
			list_add(listanodo,dato);
			//free(dato->ip);
			//free(dato->nodo);
			//free(dato);


		}else{
			void actualizar(dato_nodo* datoNodo){
					if(strcmp(datoNodo->nodo,solicitud->nodo)==0){
						datoNodo->cantidadtrabajo++;
					}
				}
			list_iterate(listanodo,(void*)actualizar);
		}
	}
	//free(dato->ip);
	//free(dato->nodo);
	//free(dato);


}
bool listoparaReduccion(t_list* listaestado,notificacionArchivo* notificacion){
	t_estados* aux;
	aux=malloc(sizeof(t_estados));
	//aux=NULL;
	int cant=0,cant1=0;
	//t_list* listaarchivos;
	//listaarchivos=list_create();
	bool findNodo(t_estados* estado){
		return (strcmp(estado->archivo_tmp,notificacion->archivo )==0 && strcmp(estado->etapa,"transformacion")==0);
	}
	aux=list_find(listaestado,(void*)findNodo);
	bool findnodos(t_estados* estado){
		return(strcmp (estado->nodo,aux->nodo)==0 && estado->id_job==aux->id_job && strcmp(estado->etapa,"transformacion")==0);
	}
	cant=list_count_satisfying(listaestado,(void*)findnodos);
	bool findnodosTerminados(t_estados* estado){
		return (strcmp(estado->nodo,aux->nodo)==0 && estado->estado==3 && estado->id_job==aux->id_job && strcmp(estado->etapa,"transformacion")==0);

	}
	cant1=list_count_satisfying(listaestado,(void*)findnodosTerminados);
	//free(aux);
	return cant==cant1;
}


/*void agregarArchivos(t_list* listaArchivos,lista_temporales_transformacion* local){
	int i;
	t_estados* estadoArchivo;
	estadoArchivo = malloc(sizeof(t_estados));
	lista_temporales_transformacion* temporales;
	temporales = local;

	for( i=0;i<list_size(listaArchivos);i++){
		estadoArchivo=list_get(listaArchivos,i);

		temporales->archivo_temporal_transformacion=malloc(strlen(estadoArchivo->archivo_tmp)+1);
		strcpy(temporales->archivo_temporal_transformacion,estadoArchivo->archivo_tmp);
		temporales->archivo_temporal_transformacion[strlen(estadoArchivo->archivo_tmp)]='\0';

		temporales = temporales->sig;
		temporales = malloc(sizeof(lista_temporales_transformacion));
		temporales->sig = NULL;

		//local->archivo_tmp_transformacion->archivo_temporal_transformacion=malloc(strlen(estadoArchivo->archivo_tmp)+1);
		//strcpy(local->archivo_tmp_transformacion->archivo_temporal_transformacion,estadoArchivo->archivo_tmp);
		//local->archivo_tmp_transformacion->archivo_temporal_transformacion[strlen(estadoArchivo->archivo_tmp)]='\0';
	}
}*/


void agregarArchivos(t_list* listaArchivos,t_solicitud_reduccion_local* local){
	int i;
	t_estados* estadoArchivo;
	estadoArchivo=malloc(sizeof(t_estados));
	//lista_temporales_transformacion* temporales;
	//temporales=malloc(sizeof(lista_temporales_transformacion));
	//temporales=NULL;

	for( i=0;i<list_size(listaArchivos);i++){
		lista_temporales_transformacion* temporales;
			temporales=malloc(sizeof(lista_temporales_transformacion));
		estadoArchivo=list_get(listaArchivos,i);

		temporales->archivo_temporal_transformacion=malloc(strlen(estadoArchivo->archivo_tmp)+1);
		strcpy(temporales->archivo_temporal_transformacion,estadoArchivo->archivo_tmp);
		temporales->archivo_temporal_transformacion[strlen(estadoArchivo->archivo_tmp)]='\0';
		temporales->sig=NULL;
		/*if(local->archivo_tmp_transformacion==NULL){
			local->archivo_tmp_transformacion=temporales;
		}else{*/

		temporales->sig=local->archivo_tmp_transformacion;
		local->archivo_tmp_transformacion=temporales;
		//}

		//local->archivo_tmp_transformacion->archivo_temporal_transformacion=malloc(strlen(estadoArchivo->archivo_tmp)+1);
		//strcpy(local->archivo_tmp_transformacion->archivo_temporal_transformacion,estadoArchivo->archivo_tmp);
		//local->archivo_tmp_transformacion->archivo_temporal_transformacion[strlen(estadoArchivo->archivo_tmp)]='\0';
	}
}




t_solicitud_reduccion_local crearSolicitudlocal(t_list* listauxiliararchivos,t_list* listanodos ){
	t_solicitud_reduccion_local solicitudLocal;
	t_estados* estado;
	dato_nodo* nodo;
	nodo=NULL;
	nodo=malloc(sizeof(dato_nodo));
	estado=NULL;

	estado =malloc(sizeof(t_estados));
	estado=list_get(listauxiliararchivos,0);
	nodo=obtenernodo(listanodos,estado);
	solicitudLocal.nodo=malloc(strlen(nodo->nodo)+1);
	strcpy(solicitudLocal.nodo,nodo->nodo);
	solicitudLocal.nodo[strlen(nodo->nodo)]='\0';
	solicitudLocal.worker_ip=malloc(strlen(nodo->ip)+1);
	strcpy(solicitudLocal.worker_ip,nodo->ip);
	solicitudLocal.worker_ip[strlen(nodo->ip)]='\0';
	solicitudLocal.worker_port=nodo->port;
	solicitudLocal.archivo_tmp_transformacion = malloc(sizeof(lista_temporales_transformacion));
	solicitudLocal.archivo_tmp_transformacion=NULL;

	crearNombreTemporalReduccionLocal(estado->master,&solicitudLocal,estado->id_job);
	//agregarArchivos(listauxiliararchivos,solicitudLocal.archivo_tmp_transformacion);
	agregarArchivos(listauxiliararchivos,&solicitudLocal);

	return solicitudLocal;
}

void agregarEstadoRLALista(t_list* listaEstados,t_solicitud_reduccion_local* solicitud_l,char* etapa,datomaster* master){
		t_estados* estado=NULL;
		estado=malloc(sizeof(t_estados));

		estado->nodo=malloc(strlen(solicitud_l->nodo)+1);
		strcpy(estado->nodo,solicitud_l->nodo);
		estado->nodo[strlen(solicitud_l->nodo)]='\0';
	   	estado->archivo_tmp=malloc(strlen(solicitud_l->archivo_tmp_reduccion_local)+1);
	   	strcpy(estado->archivo_tmp,solicitud_l->archivo_tmp_reduccion_local);
	   	estado->archivo_tmp[strlen(solicitud_l->archivo_tmp_reduccion_local)]='\0';
	   	estado->bloque=0;
	   	estado->estado=1;
	   	estado->id_job=master->job;
	   	estado->master=master->master;
	   	estado->etapa=malloc(strlen (etapa)+1);
	   	strcpy(estado->etapa,etapa);
	   	estado->etapa[strlen(etapa)]='\0';
	   	list_add(listaEstados,estado);
	   	showContenidolistaEstados(listaEstados);

}


void agregarEstadoRGALista(t_list* listaEstados,t_solicitud_reduccion_global* solicitud_g,char* etapa,datomaster* master,char* nodoseleccionado){
		t_estados* estado=NULL;
		estado=malloc(sizeof(t_estados));

		estado->nodo=malloc(strlen(nodoseleccionado)+1);
		strcpy(estado->nodo,nodoseleccionado);
		estado->nodo[strlen(nodoseleccionado)]='\0';
	   	estado->archivo_tmp=malloc(strlen(solicitud_g->archivo_reduccion_global)+1);
	   	strcpy(estado->archivo_tmp,solicitud_g->archivo_reduccion_global);
	   	estado->archivo_tmp[strlen(solicitud_g->archivo_reduccion_global)]='\0';
	   	estado->bloque=0;
	   	estado->estado=1;
	   	estado->id_job=master->job;
	   	estado->master=master->master;
	   	estado->etapa=malloc(strlen (etapa)+1);
	   	strcpy(estado->etapa,etapa);
	   	estado->etapa[strlen(etapa)]='\0';
	   	list_add(listaEstados,estado);
	   	showContenidolistaEstados(listaEstados);

}




bool listoParaRG(t_list* listaestado,notificacionArchivo* notificacion){
	int cant=0,cant1=0;

	t_estados* estadobuscado;

	estadobuscado=malloc(sizeof(t_estados));

	bool findestado(t_estados* estado){
			return (strcmp(estado->etapa,"reduccionLocal")==0 && strcmp(estado->archivo_tmp,notificacion->archivo)==0);
		}

	estadobuscado=list_find(listaestado,(void*)findestado);

	bool find(t_estados* estado){
				return (strcmp(estado->etapa,"reduccionLocal")==0  && estado->id_job==estadobuscado->id_job);

			}
	cant=list_count_satisfying(listaestado,(void*)find);

	bool findTerminados(t_estados* estado){
				return (strcmp(estado->etapa,"reduccionLocal")==0 && estado->estado==3 && estado->id_job==estadobuscado->id_job);

			}
	cant1=list_count_satisfying(listaestado,(void*)findTerminados);
	return cant==cant1;

}
t_list* obtenerArchivosreduccionglobal(t_list* listaestados,notificacionArchivo* notificacion){
	t_estados* auxestado;
	auxestado=malloc(sizeof(t_estados));

	bool findestado(t_estados* estado){
		return (strcmp(estado->archivo_tmp,notificacion->archivo )==0 && strcmp(estado->etapa,"reduccionLocal")==0 && estado->estado==3);
	}
	auxestado=list_find(listaestados,(void*)findestado);
	bool find(t_estados* estado){
		return(strcmp(estado->etapa,"reduccionLocal")==0 && estado->estado==3 && estado->id_job==auxestado->id_job);
	}

	return list_filter(listaestados,(void*)find);

}

void abortarjob(datomaster* masterbuscado,t_list* listamaster,t_list* listaEstados){
	void abortar(t_estados* estado){
		if(estado->id_job==masterbuscado->job){
			estado->estado=2;
		}
	}


	list_iterate(listaEstados,(void*)abortar);
	bool find(datomaster* master1){
		return masterbuscado->master==master1->master && masterbuscado->job==master1->job;
	}
	list_remove_by_condition(listamaster,(void*)find);


}

char* elegirnodoencargado(worker_algoritmo* lista){
	//worker_algoritmo* punteroaux;
	//punteroaux=malloc(sizeof(worker_algoritmo));
	//punteroaux=NULL;
	int minimo;
	//punteroaux=lista;
	minimo=lista->canttrabajo;
	//punteroaux=punteroaux->next;
	char* nombrenodo=string_new();
	nombrenodo=malloc(strlen(lista->nodo)+1);
	strcpy(nombrenodo,lista->nodo);
	nombrenodo[strlen(lista->nodo)]='\0';

	while(lista != NULL){
		if(lista->canttrabajo < minimo){
			nombrenodo=malloc(strlen(lista->nodo)+1);
			minimo=lista->canttrabajo;
			strcpy(nombrenodo,lista->nodo);
			nombrenodo[strlen(lista->nodo)]='\0';
		}
		lista=lista->next;
	}

	//free(punteroaux);
	return nombrenodo;
}
t_solicitud_reduccion_global crearSolicitudGlobal(t_estados* estado,t_list* listanodos,char* nodoseleccionado,datomaster* masterbuscado){
	t_solicitud_reduccion_global solicitudG;
	dato_nodo* nodobuscado;
	nodobuscado =malloc(sizeof(dato_nodo));
	nodobuscado=obtenernodo(listanodos,estado);
	solicitudG.worker_ip=malloc(strlen(nodobuscado->ip)+1);
	strcpy(solicitudG.worker_ip,nodobuscado->ip);
	solicitudG.worker_ip[strlen(nodobuscado->ip)]='\0';
	solicitudG.nodo=malloc(strlen(nodobuscado->nodo)+1);
	strcpy(solicitudG.nodo,nodobuscado->nodo);
	solicitudG.nodo[strlen(nodobuscado->nodo)]='\0';
	solicitudG.worker_port=nodobuscado->port;

	solicitudG.archivo_tmp_reduccion_local=malloc(strlen(estado->archivo_tmp)+1);
	strcpy(solicitudG.archivo_tmp_reduccion_local,estado->archivo_tmp);
	solicitudG.archivo_tmp_reduccion_local[strlen(estado->archivo_tmp)]='\0';

	solicitudG.archivo_reduccion_global=malloc(strlen(masterbuscado->nombrearchivoRGfinal)+1);
	strcpy(solicitudG.archivo_reduccion_global,masterbuscado->nombrearchivoRGfinal);
	solicitudG.archivo_reduccion_global[strlen(masterbuscado->nombrearchivoRGfinal)]='\0';

	if(strcmp(nodoseleccionado,estado->nodo)==0){
		solicitudG.encargado=true;
		//crearnombreRG(&solicitudG,nodoseleccionado,estado->master,estado->id_job);
	}else{
		solicitudG.encargado=false;
		//crearnombreRG(&solicitudG,nodoseleccionado,estado->master,estado->id_job);
		//solicitudG.archivo_reduccion_global=NULL;
	}



	return solicitudG;
}
void crearnombreRG(t_solicitud_reduccion_global* solicitudG ,char* nodo,int id_master,int job){
	char* nombre=string_new();
	string_append(&nombre,"/tmp/Master");
	string_append_with_format(&nombre,"%i",id_master);
	//string_append(&nombre,"-worker");
	//string_append_with_format(&nombre,"%s",nodo);
	string_append(&nombre,"-job");
	string_append_with_format(&nombre,"%i",job);
	string_append(&nombre,"-final");
	solicitudG->archivo_reduccion_global=malloc(strlen(nombre)+1);
	strcpy(solicitudG->archivo_reduccion_global,nombre);
	solicitudG->archivo_reduccion_global[strlen(nombre)]='\0';

}
dato_nodo*  seleccionarnodo(t_list* listanodos){
	int minimo,cant=0,port=0;
	dato_nodo* nodo;

	nodo=malloc(sizeof(dato_nodo));
	nodo=list_get(listanodos,0);
	minimo=nodo->cantidadtrabajo;
	for(cant=0;cant<list_size(listanodos);cant++){
		nodo=list_get(listanodos,cant);
		if(minimo>nodo->cantidadtrabajo){
			minimo=nodo->cantidadtrabajo;
			port=nodo->port;
		}

	}
	bool findnodo(dato_nodo* nodobuscado){
		return (nodobuscado->port==port && nodobuscado->cantidadtrabajo==minimo);
	}
	nodo=list_find(listanodos,(void*)findnodo);
	return nodo;


}



dato_nodo* obtenernodo(t_list* lista,t_estados* estado){
	dato_nodo* nodobuscado;
	nodobuscado=malloc(sizeof(dato_nodo));
	bool findnodo(dato_nodo* dato){

		return (strcmp(dato->nodo,estado->nodo)==0);
	}
	nodobuscado=list_find(lista,(void*)findnodo);

	return nodobuscado;
}

/*t_list*  analisarlistaestadoParaRL(t_list* listaestados){
	t_list* auxtodos;
	list_create(auxtodos);
	int cant1,cant2;
	bool findtodosRl(t_estados* estado){
			return (strcmp(estado->etapa,"reduccionLocal")==0);
		}
	cant1=list_count_satisfying()(listaestados,(void*)findtodosRl);
	bool findterminados(t_estados* estado){
				return (strcmp(estado->etapa,"reduccionLocal")==0 && estado==3);

	cant2=list_count_satisfying(listaestados,(void*)findterminados);
	if(cant1=cant2)){
		auxtodos=
		return auxtodos;
	}

	return auxtodos;
	}
}*/




void cargarTablaNodos(t_list* listaTablaNodos, t_queue* colaNodos, t_log* infoLogger){

	char** arregloNodos;
	int tamanoTotalNodo, tamanoLibreNodo;
	char * string_aux;	
	t_nodos* registroTablaNodos;

	// Leo el nodos.bin
	t_config* nodos_bin = config_create("metadata/nodos.bin");

	// Si existe el archivo
	if(config_has_property(nodos_bin, "NODOS")){

	    log_info(infoLogger, "Reestableciendo el FS de metadata/nodos.bin" );

		// Obtengo un Array de todos los Nodos
		arregloNodos =  string_get_string_as_array(config_get_string_value(nodos_bin,"NODOS"));


	    void _each_elemento_(char* nombreNodo)
		{
			// Genero las key para consultar el archivo nodos.bin
			string_aux = string_new();
			string_append(&string_aux, nombreNodo);
			string_append(&string_aux, "TOTAL");
			tamanoTotalNodo = config_get_int_value(nodos_bin,string_aux);
			free(string_aux);

			string_aux = string_new();
			string_append(&string_aux, nombreNodo);
			string_append(&string_aux, "LIBRE");
			tamanoLibreNodo = config_get_int_value(nodos_bin,string_aux);

			// Cargo el Registro de nodos
			registroTablaNodos = malloc( (2*sizeof(int)) + strlen(nombreNodo)+1);

			registroTablaNodos->tamano = tamanoTotalNodo;
			registroTablaNodos->libre = tamanoLibreNodo;
			
			registroTablaNodos->nodo = malloc(strlen(nombreNodo)+1);
			strcpy( registroTablaNodos->nodo ,nombreNodo);
			registroTablaNodos->nodo[strlen(nombreNodo)] = '\0';

			// Agrego el Registro en la Lista
			list_add(listaTablaNodos,registroTablaNodos);

			// Cargo la Cola de Nodos para gestionar los bloques
			queue_push(colaNodos, registroTablaNodos);

			//free(registroTablaNodos);
			free(string_aux);
		}
		string_iterate_lines(arregloNodos, (void*)_each_elemento_);
	}

    log_info(infoLogger, "FS Reestablecido desde metadata/nodos.bin" );

    // Libero
	config_destroy(nodos_bin);    

	free(arregloNodos);
	
}

// Agrego un nuevo nodo a la Lista de Nodos
void addDataNode(t_list* listaNodosConectados, int socket, t_list* listaTablaNodos, t_queue* colaNodos, t_queue* colaNodosConectados, t_nodos* registroTablaNodos, t_log* infoLogger){

	t_nodo_socket* registroNodoSocket = NULL;
	registroNodoSocket = malloc(sizeof(t_nodo_socket));

	// Cargo el Registro del Nodo y su Socket
	registroNodoSocket->socket = socket;
	registroNodoSocket->nodo = malloc(strlen(registroTablaNodos->nodo)+1);
	strcpy( registroNodoSocket->nodo ,registroTablaNodos->nodo);
	registroNodoSocket->nodo[strlen(registroTablaNodos->nodo)] = '\0';


	if(!existeDataNode(listaTablaNodos, registroTablaNodos->nodo ,infoLogger)){

		// Agrego el Registro en la Lista
		list_add(listaTablaNodos,registroTablaNodos);

		// Cargo la Cola de Nodos para gestionar los bloques
		queue_push(colaNodos, registroTablaNodos);


		// Persistir datos
		persistirTablaNodo(listaTablaNodos, infoLogger);
	}else{

		// Actualizo en listaTablaNodos la IP y Port del Nodo conectado
		actualizarListaTablaNodos(listaTablaNodos, registroTablaNodos);

	}

	// Agrego el Registro en la Lista NodoSockets y la Cola colaNodosConectados
	cargarListaNodosConectados(listaNodosConectados, listaTablaNodos, colaNodosConectados, registroNodoSocket);

}

// Cuando se conecta un Nodo, actualizo la IP y Port en listaTablaNodos
void actualizarListaTablaNodos(t_list* listaTablaNodos, t_nodos* registroTablaNodos){

	// Elimino el Registro anterior
	bool _find_nodo_(t_nodos* registroTablaNodosAux)
	{
		return (strcmp(registroTablaNodosAux->nodo,registroTablaNodos->nodo) == 0);
	}
	list_remove_by_condition(listaTablaNodos, (void*)_find_nodo_);

	// Cargo el Registro Actualizado
	list_add(listaTablaNodos, registroTablaNodos);
}

bool existeDataNode(t_list* listaTablaNodos, char* nombreDataNode, t_log* infoLogger){

	int existeNodo = false;	

    void _each_elemento_(t_nodos* registroTablaNodos)
	{

		if(strcmp(registroTablaNodos->nodo,nombreDataNode) == 0 ){
			existeNodo = true;
		}
	}
    list_iterate(listaTablaNodos, (void*)_each_elemento_);

	return existeNodo;
}

//Persistir la Tabla de Nodos
void persistirTablaNodo(t_list* listaTablaNodos, t_log* infoLogger){

	char * string_aux = string_new();
	int contadorNodos = 0, acumuladorTotal = 0, acumuladorLibre = 0;


	char * string_aux2 = string_new();
	string_append(&string_aux2, "[");

	// Si no existe el archivo, lo creo
	if (!existeArchivo("metadata/nodos.bin")){
		FILE *archivo_nodos_bin = fopen("metadata/nodos.bin", "w");
	    fclose(archivo_nodos_bin);
	}

	// Leo el nodos.bin
	t_config* nodos_bin = config_create("metadata/nodos.bin");

    void _each_elemento_(t_nodos* registroTablaNodos)
	{
		string_aux = string_new();
		string_append(&string_aux, registroTablaNodos->nodo);
		string_append(&string_aux, "TOTAL");

		if(!config_has_property(nodos_bin, string_aux)){
			config_set_value(nodos_bin, string_aux, string_itoa(registroTablaNodos->tamano));
			config_save(nodos_bin);
		}

		int acumParcialTotal = config_get_int_value(nodos_bin,string_aux);
		acumuladorTotal = acumuladorTotal + acumParcialTotal;

		string_aux = string_new();
		string_append(&string_aux, registroTablaNodos->nodo);
		string_append(&string_aux, "LIBRE");

		if(!config_has_property(nodos_bin, string_aux)){
			config_set_value(nodos_bin, string_aux, string_itoa(registroTablaNodos->libre));
			config_save(nodos_bin);
		}

		int acumParcialLibre = config_get_int_value(nodos_bin,string_aux);
		acumuladorLibre = acumuladorLibre + acumParcialLibre;

		// Actualizo el campo NODOS del archivo nodos.bin
		contadorNodos = contadorNodos + 1;

		if(contadorNodos != 1){
			string_append(&string_aux2, ",");
		}
		string_append(&string_aux2, registroTablaNodos->nodo);

	}
    list_iterate(listaTablaNodos, (void*)_each_elemento_);

    // Termino de generar el String de NODOS
	string_append(&string_aux2, "]");
	config_set_value(nodos_bin, "NODOS", string_aux2);
	config_save(nodos_bin);

	// Actualizo los registros LIBRE y TAMANIO
	config_set_value(nodos_bin, "LIBRE", string_itoa(acumuladorLibre));
	config_set_value(nodos_bin, "TAMANIO", string_itoa(acumuladorTotal));
	config_save(nodos_bin);

    // Libero
	config_destroy(nodos_bin);    

	free(string_aux);
	free(string_aux2);
}

//Listar la Tabla de Nodos
void showContenidolistaNodos(t_list* listaTablaNodos){ 

	int indice = 0;

	if(list_size(listaTablaNodos) > 0){

	    void _each_elemento_(t_nodos* registroTablaNodos)
		{
			indice = indice + 1;

			// Muestro el encabezaado
			if(indice == 1) {
				printf("\nTABLA DE NODOS\n");
				printf("Nodo \t Tamaño \t Libre\n");
				printf("------\t -------\t -------\n");
			}

			printf("%s \t %d \t %d \n", registroTablaNodos->nodo,registroTablaNodos->tamano,obtenerEspacioLibreNodo(listaTablaNodos, registroTablaNodos->nodo));

		}
	    list_iterate(listaTablaNodos, (void*)_each_elemento_);
	}

}

//Listar la Tabla de Estados
void showContenidolistaEstados(t_list* listaEstados){ 

	int indice = 0;

	if(list_size(listaEstados) > 0){

	    void _each_elemento_(t_estados* registroTablaEstados)
		{
			indice = indice + 1;

			// Muestro el encabezaado
			if(indice == 1) {
				printf("TABLA DE ESTADOS\n");
				printf("Id_job \t Master \t Nodo\t Bloque\t Etapa\t Archivo TMP\t Estado \n");
				printf("------\t -------\t -------\n");
			}

			printf("%d \t %d \t %s \t %d \t %s \t %s \t %d\n", registroTablaEstados->id_job, registroTablaEstados->master, registroTablaEstados->nodo, registroTablaEstados->bloque, registroTablaEstados->etapa, registroTablaEstados->archivo_tmp, registroTablaEstados->estado );
		}
	    list_iterate(listaEstados, (void*)_each_elemento_);
	}
}

//Listar los Nodos Conectados
void showContenidolistaNodosConectados(t_list* listaNodosConectados, t_queue* colaNodosConectados){ 

	int indice = 0;
	t_nodos* registroNodos = NULL;

	if(list_size(listaNodosConectados) > 0){

	    void _each_elemento_(t_nodo_socket* registroTablaNodos)
		{
			indice = indice + 1;

			// Muestro el encabezaado
			if(indice == 1) {
				printf("\nLISTA NODOS CONECTADOS\n");
				printf("Nodo \t Socket\n");
				printf("------\t -------\n");
			}

			printf("%s \t %d\n", registroTablaNodos->nodo, registroTablaNodos->socket);
		
		}
	    list_iterate(listaNodosConectados, (void*)_each_elemento_);



		if(queue_size(colaNodosConectados) > 0){

			for (indice = 0; indice  < queue_size(colaNodosConectados); indice=indice+1 ) {

				// Muestro el encabezaado
				if(indice == 0) {
					printf("\nCOLA NODOS CONECTADOS\n");
					printf("Nodo\n");
					printf("------\n");
				}

				// Obtengo un elemento
				registroNodos = queue_pop(colaNodosConectados);

				printf("%s\n", registroNodos->nodo);

				// Lo vuelvo a agregar a la cola
				queue_push(colaNodosConectados, registroNodos);
			}
		}
	}
}

// Generacion de datos del Nodo para enviar a FS
void obtenerDatosNodo(t_nodos* datos_nodo){

	// Leo el archivo de config
	t_config* cfg = config_create("config/config.cfg");

	// Obtengo los datos del Nodo
	char* nombreNodo = config_get_string_value(cfg,"NODO_NOMBRE");
	char* pathDataBin = config_get_string_value(cfg,"RUTA_DATABIN");
	int tamanoDataBin = config_get_int_value(cfg,"TAMANO_DATABIN");
	int port_datanode = config_get_int_value(cfg,"NODO_PUERTO");
	int port_worker = config_get_int_value(cfg,"WORKER_PUERTO");
	char* ip = config_get_string_value(cfg,"NODO_IP");

	datos_nodo->nodo = malloc (strlen(nombreNodo)+1);
    strcpy( datos_nodo->nodo ,nombreNodo);
    datos_nodo->nodo[strlen(nombreNodo)] = '\0';

	datos_nodo->ip = malloc (strlen(ip)+1);
    strcpy( datos_nodo->ip ,ip);
    datos_nodo->ip[strlen(ip)] = '\0';

    datos_nodo->tamano = tamanoDataBin;
    datos_nodo->libre = datos_nodo->tamano;
    datos_nodo->port_worker = port_worker;
    datos_nodo->port_datanode = port_datanode;

    // Libero
	config_destroy(cfg);
}

// Creo el DataBin en el DataNode
void crearDataBin(char* pathDataBin){

	// Leo el archivo de config
	t_config* cfg = config_create("config/config.cfg");
	int tamanoDataBin = config_get_int_value(cfg,"TAMANO_DATABIN");

	FILE * archivo_databin = fopen(pathDataBin, "wb");
	ftruncate(fileno(archivo_databin), tamanoDataBin*1024*1024);

	// Cierro el FD
	fclose(archivo_databin);

    // Libero
	config_destroy(cfg);
}


// Persistir un bloque en DataBin
void persistirBloque(int bloque, char* contenido, int tamanoContenido, t_log* infoLogger){

	// Leo el archivo de config
	t_config* cfg = config_create("config/config.cfg");

	// Obtengo los datos del Nodo
	char* pathDataBin = config_get_string_value(cfg,"RUTA_DATABIN");

	FILE * archivo_databin = fopen(pathDataBin, "r+b");

	// Me posiciono al comienzo del bloque a persistir
	fseek( archivo_databin, bloque*TAM_BLOQUE, SEEK_SET );

	// Persisto el bloque
	int write_sz = fwrite(contenido, tamanoContenido, sizeof(char), archivo_databin);

	// Cierro el FD
	fclose(archivo_databin);

    // Libero
	config_destroy(cfg);
}


// Persistir un Archivo completo
void persistirArchivo(char* nombreArchivo, char* contenido, int tamanoContenido, t_log* infoLogger){

	FILE * archivo_completo = fopen(nombreArchivo, "w+");

	// Me posiciono al comienzo del bloque a persistir

	// Persisto el bloque
	int write_sz = fwrite(contenido, tamanoContenido, sizeof(char), archivo_completo);

	// Cierro el FD
	fclose(archivo_completo);
}

// Obtengo un bloque de DataBin
t_bloque_contenido* obtenerBloque(int bloque, int tamanoContenido, t_log* infoLogger){

	t_bloque_contenido* solicitud = NULL;
	solicitud = malloc(sizeof(t_bloque_contenido));
	solicitud->contenido_archivo = malloc(tamanoContenido);
	solicitud->bloque = bloque;

	// Leo el archivo de config
	t_config* cfg = config_create("config/config.cfg");

	// Obtengo los datos del Nodo
	char* pathDataBin = config_get_string_value(cfg,"RUTA_DATABIN");
	solicitud->nombre = malloc(strlen(pathDataBin)+1);
	strcpy(solicitud->nombre, pathDataBin);

	FILE * archivo_databin = fopen(pathDataBin, "rb");

	// Me posiciono al comienzo del bloque a persistir
	fseek( archivo_databin, bloque*TAM_BLOQUE, SEEK_SET );

	// Leo el bloque
	int write_sz = fread(solicitud->contenido_archivo, tamanoContenido, sizeof(char), archivo_databin);

	// Cierro el FD
	fclose(archivo_databin);

    // Libero
	config_destroy(cfg);

	return solicitud;
}

// Creo el Bitmap de cada Nodo
void crearBitmapNodo(t_list* listaTablaNodos, t_nodos* datos_nodo, t_log* infoLogger){

	//t_bitarray* bitmap;
	//char* buffer;
	t_bitmap* bitmap;
	bitmap = malloc(sizeof(t_bitmap));
	bitmap->tamano = datos_nodo->tamano;

	// Defino el tamaño del Bitmap
	//buffer = malloc( datos_nodo->tamano / 8);
	//bitmap = bitarray_create_with_mode(buffer, datos_nodo->tamano / 8, MSB_FIRST);
	//buffer = malloc( datos_nodo->tamano);


	// Inicializo el BitMap del Nodo
	int i;
	for (i = 0; i < datos_nodo->tamano; i++) { 
		bitmap->contenido[i]='0';
	}

	bitmap->contenido[datos_nodo->tamano]='\0';

	// Persisto el Bitmap
	persistirBitmap(listaTablaNodos, bitmap, datos_nodo->nodo, infoLogger);

	free(bitmap->contenido);
}

// Persisto el Bitmap de un Nodo
void persistirBitmap(t_list* listaTablaNodos, t_bitmap* bitmap, char* nombreNodo, t_log* infoLogger){

	t_list* registroTablaNodos = NULL;
	int tamanoBitmap = 0;
	t_list* listaux;
	char* nombreArchivoBitmap = string_new();
	nombreArchivoBitmap = string_from_format("metadata/bitmaps/%s.dat", nombreNodo);

	FILE* bitmap_f = fopen(nombreArchivoBitmap, "w+");

	// Averiguo el tamaño del bitmap del Nodo
	void find_nodo(t_nodos* registroTablaNodos){

		if(strcmp(registroTablaNodos->nodo,nombreNodo)==0 ){
			tamanoBitmap = registroTablaNodos->tamano;
		}
	}
	listaux = list_filter(listaTablaNodos,(void*)find_nodo);

	fwrite(bitmap->contenido, bitmap->tamano, 1, bitmap_f);

	log_info(infoLogger,"Actualizando Bitmap del Nodo %s. Tamano Bitmap: %d", nombreNodo, tamanoBitmap);

	// Cierro el FD
	fclose(bitmap_f);
	list_destroy(listaux);
	free(nombreArchivoBitmap);
}

// Consulto el bitmap del nodo y devuelvo los primeros n bloques libres
int obtenerPrimerBloquesLibresNodo(t_list* listaTablaNodos, char* nombreNodo, int total_bloques){
	
	int primerBloque = -1, bloquesLibresContiguos = 0, i;
	t_bitmap* bitmap;
	char* nombreArchivoBitmap = string_new();
	nombreArchivoBitmap = string_from_format("metadata/bitmaps/%s.dat", nombreNodo);

	if (!existeArchivo(nombreArchivoBitmap)){
		return -1;
	}
	bitmap = cargarBitmap(listaTablaNodos, nombreNodo);


	// Busco el primer bloque libre dentro del nodo
	for (i = 0; i < bitmap->tamano; i++) { 

		// Si esta libre
		if(bitmap->contenido[i] == '0'){
			primerBloque = i;
			break;
		}
	}


/*

	// Si la cantidad de bloques es inferior al Tamano del Bitmap
	if(total_bloques <= bitmap->tamano){

		// Busco el primer bloque libre dentro del nodo
		for (i = 0; i < bitmap->tamano; i++) { 

			bloquesLibresContiguos++;

			// Si esta libre
			if(bitmap->contenido[i] == '0'){
				if(bloquesLibresContiguos == 1){
					primerBloque = i;
				}
			}else{
				bloquesLibresContiguos = 0;
				primerBloque = -1;
			}


			if(total_bloques == bloquesLibresContiguos){
				break;
			}
		}

		// Valido que si se llego al final del bitmap, se lograron los bloques necesarios
		if(primerBloque != -1 && total_bloques != bloquesLibresContiguos){
			primerBloque = -1;
		}

	}
*/

	free(bitmap->contenido);
	free(nombreArchivoBitmap);
	
	return primerBloque;
}

// Asigno los bloques de los archivos a 2 nodos equitativamente
t_bloques_archivos* asignarBloquesNodos(bool es_worker, t_list* listaTablaNodos, int nro_bloque, int totalBytesBloque, char* nombreNodo1, char* nombreNodo2, int primerBloqueLibreNodo1, int primerBloqueLibreNodo2){

	// Obtengo la IP y el Port de los Nodos
/*	
	char* ipNodo1 = string_new();
	ipNodo1 = obtenerIPNodo(listaTablaNodos, nombreNodo1);
	int portNodo1 = obtenerPortNodo(listaTablaNodos, nombreNodo1, es_worker);

	char* ipNodo2 = string_new();
	ipNodo2 = obtenerIPNodo(listaTablaNodos, nombreNodo2);
	int portNodo2 = obtenerPortNodo(listaTablaNodos, nombreNodo2, es_worker);
*/

	t_bloques_archivos* lstBloquesArchivos = NULL;
	//lstBloquesArchivos = malloc(sizeof(int)*8 + strlen(nombreNodo1) + 1 + strlen(nombreNodo2) + 1 +  strlen(ipNodo1) +1 +  strlen(ipNodo2) + 1);
	lstBloquesArchivos = malloc(sizeof(t_bloques_archivos));

	// Genero el Registro
	lstBloquesArchivos->bloque = nro_bloque;
	lstBloquesArchivos->bytes_ocupados = totalBytesBloque;

	lstBloquesArchivos->copia1.bloque = primerBloqueLibreNodo1;
	lstBloquesArchivos->copia1.nodo = malloc(strlen(nombreNodo1)+1);
	strcpy( lstBloquesArchivos->copia1.nodo ,nombreNodo1);
	lstBloquesArchivos->copia1.nodo[strlen(nombreNodo1)] = '\0';


	lstBloquesArchivos->copia2.bloque = primerBloqueLibreNodo2;
	lstBloquesArchivos->copia2.nodo = malloc(strlen(nombreNodo2)+1);
	strcpy( lstBloquesArchivos->copia2.nodo ,nombreNodo2);
	lstBloquesArchivos->copia2.nodo[strlen(nombreNodo2)] = '\0';	


	//free(ipNodo1);
	//free(ipNodo2);

	return lstBloquesArchivos;
}

// Obtengo la IP de un determinado Nodo conectado
char* obtenerIPNodo(t_list* listaTablaNodos, char* nombreNodo){

	t_nodos* registroNodo = NULL;
	//registroNodo = malloc(sizeof(t_nodos));

	bool _find_nodo_(t_nodos* registroTablaNodos)
	{
		return (strcmp(registroTablaNodos->nodo,nombreNodo) == 0);
	}
	registroNodo = list_find(listaTablaNodos,(void*)_find_nodo_);

	return registroNodo->ip;
}

// Obtengo el Port de un determinado Nodo conectado
int obtenerPortNodo(t_list* listaTablaNodos, char* nombreNodo, bool es_worker){

	t_nodos* registroNodo = NULL;
	//registroNodo = malloc(sizeof(t_nodos));

	bool _find_nodo_(t_nodos* registroTablaNodos)
	{
		return (strcmp(registroTablaNodos->nodo,nombreNodo) == 0);
	}

	if (!list_is_empty(listaTablaNodos)){
		registroNodo = list_find(listaTablaNodos,(void*)_find_nodo_);

		if(es_worker){
			return registroNodo->port_worker;
		}else{
			return registroNodo->port_datanode;
		}
		
	}else{
		return 0;
	}
}

// Obtengo una lista de todos los bloques de un archivo
t_list* obtenerDatosBloquesArchivo(char* pathArchivo, bool es_worker, t_list* listaTablaDirectorios, t_list* listaTablaNodos, t_log* infoLogger){

	t_list* listaBloques = list_create();

	// Si existe el archivo en YamaFS
	if(existeArchivoYama(pathArchivo, listaTablaDirectorios) != -100){

		int cantDirectorios, indice;
		char** arregloDirectorios = NULL;
		char* nombreArchivo = NULL;

		// Separo el pathArchivo en todos los subdirectorios posibles
		arregloDirectorios = string_split(pathArchivo, "/");
		cantDirectorios = cantidadDirectoriosPath(pathArchivo);

		// Obtengo el nombre del archivo
		nombreArchivo = malloc(strlen(arregloDirectorios[cantDirectorios-1])+1);
		strcpy(nombreArchivo,arregloDirectorios[cantDirectorios-1]);


		indice = obtenerIndiceDirectorioYama( string_substring(pathArchivo, 0, string_length(pathArchivo) - string_length(nombreArchivo) - 1), listaTablaDirectorios);

		// Leo el detalle de archivo configuracion
		t_config* archivoFS = config_create(string_from_format("metadata/archivos/%d/%s", indice, nombreArchivo));	

		// Determino la cantidad de bloques que tiene el archivo
		int total_bloques = (config_get_int_value(archivoFS,"TAMANIO") / TAM_BLOQUE );

		// Si el tamano no es entero, lo redondeo para arriba
		if(config_get_int_value(archivoFS,"TAMANIO") % TAM_BLOQUE) { total_bloques = total_bloques+1; }


		int bloque = 0;

		char* nombreAux = string_new();		
		char* ipNodoAux = string_new();

		// Recorro cada bloque
		for (bloque = 0; bloque  < total_bloques; bloque=bloque+1 ) {

			t_bloques_archivos* solicitud = NULL;
			solicitud = malloc(sizeof(t_bloques_archivos));


			// Datos del archivo
		    solicitud->bloque= bloque;
		    solicitud->bytes_ocupados = config_get_int_value(archivoFS,string_from_format("BLOQUE%iBYTES", bloque ));

			// Datos del Nodo de la copia 1
			nombreAux = string_get_string_as_array( config_get_string_value(archivoFS,string_from_format("BLOQUE%iCOPIA0", bloque)) )[0];
			solicitud->copia1.nodo = malloc (strlen(nombreAux)+1);
		    strcpy( solicitud->copia1.nodo , nombreAux);
		    solicitud->copia1.nodo[strlen(nombreAux)] = '\0';

			solicitud->copia1.bloque= atoi(string_get_string_as_array(config_get_string_value(archivoFS,string_from_format("BLOQUE%iCOPIA0", bloque)))[1] );

			// Si el nodo esta conectado, obtengo el PORT e IP
			if(existeDataNode(listaTablaNodos, solicitud->copia1.nodo ,infoLogger)){

				// Obtengo el Port
			    solicitud->copia1.port=obtenerPortNodo(listaTablaNodos, solicitud->copia1.nodo, es_worker);

			    // Obtengo la IP
				ipNodoAux = obtenerIPNodo(listaTablaNodos, solicitud->copia1.nodo);
				solicitud->copia1.ip = malloc (strlen(ipNodoAux)+1);
			    strcpy( solicitud->copia1.ip ,ipNodoAux);
				solicitud->copia1.ip[strlen(ipNodoAux)] = '\0';

			}else{
				solicitud->copia1.port=0;
				solicitud->copia1.ip = NULL;
			}



			// Datos del Nodo de la copia 2


			nombreAux = string_get_string_as_array( config_get_string_value(archivoFS,string_from_format("BLOQUE%iCOPIA1", bloque)) )[0];
			solicitud->copia2.nodo = malloc (strlen(nombreAux)+1);
		    strcpy( solicitud->copia2.nodo , nombreAux);
		    solicitud->copia2.nodo[strlen(nombreAux)] = '\0';

		    solicitud->copia2.bloque= atoi(string_get_string_as_array(config_get_string_value(archivoFS,string_from_format("BLOQUE%iCOPIA1", bloque)))[1] );

			// Si el nodo esta conectado, obtengo el PORT e IP
			if(existeDataNode(listaTablaNodos, solicitud->copia2.nodo ,infoLogger)){

				// Obtengo el Port
			    solicitud->copia2.port=obtenerPortNodo(listaTablaNodos, solicitud->copia2.nodo, es_worker);
			    // Obtengo la IP
				ipNodoAux = obtenerIPNodo(listaTablaNodos, solicitud->copia2.nodo);
				solicitud->copia2.ip = malloc (strlen(ipNodoAux)+1);
			    strcpy( solicitud->copia2.ip ,ipNodoAux);
				solicitud->copia2.ip[strlen(ipNodoAux)] = '\0';
			}else{
				solicitud->copia2.port=0;
				solicitud->copia2.ip = NULL;

			}

			// Actualizo la lista
			list_add(listaBloques,solicitud);

		}

		//free(solicitud->copia1.ip);
		//free(solicitud->copia2.ip);
		//free(solicitud->copia1.nodo);
		//free(solicitud->copia2.nodo);
		

	    // Libero
		config_destroy(archivoFS);    
		//free(nombreAux);
		//free(ipNodoAux);
		free(nombreArchivo);
	}

	return listaBloques;
}

// Cargo la Lista listaNodosConectados
void cargarListaNodosConectados(t_list* listaNodosConectados, t_list* listaTablaNodos, t_queue* colaNodosConectados, t_nodo_socket* registroNodoSocket){

	t_nodo_socket* registroNodoSocketAux = NULL;
	//registroNodoSocketAux = malloc(sizeof(t_nodo_socket));

	bool _find_directoy_(t_nodo_socket* registroNodoSocketAux2)
	{
		return (strcmp(registroNodoSocketAux2->nodo,registroNodoSocket->nodo) == 0);
	}
	registroNodoSocketAux = list_find(listaNodosConectados,(void*)_find_directoy_);

	if(registroNodoSocketAux == NULL){
		// Carga la Lista de Nodos Conectados
		list_add(listaNodosConectados, registroNodoSocket);
	}


	t_nodos* registroNodoAux = NULL;
	//registroNodoAux = malloc(sizeof(t_nodos));

	bool _find_nodo_(t_nodos* registroNodoAux2)
	{
		return (strcmp(registroNodoAux2->nodo,registroNodoSocket->nodo) == 0);
	}
	registroNodoAux = list_find(listaTablaNodos,(void*)_find_nodo_);

	if(registroNodoAux != NULL){
		// Carga la Cola de Nodos Conectados
		queue_push(colaNodosConectados, registroNodoAux);
	}
}

// Obtengo el Socket de un Nodo
int obtenerSocketNodo(t_list* listaNodosConectados, char* nombreNodo){

	t_nodo_socket* registroNodoSocket = NULL;
	//registroNodoSocket = malloc(sizeof(t_nodo_socket));

	bool _find_directoy_(t_nodo_socket* registroNodoSocketAux)
	{
		return (strcmp(registroNodoSocketAux->nodo,nombreNodo) == 0);
	}
	registroNodoSocket = list_find(listaNodosConectados,(void*)_find_directoy_);

	if(registroNodoSocket != NULL){
		return registroNodoSocket->socket;
	}else{
		return 0;
	}
}


void removeN(char* cadena,int n){	// Elimina los primeros n char

	char* aux = malloc(strlen(cadena)+1);

	strcpy(aux,cadena);
	strcpy(cadena,aux+n);

	free(aux);
}

// Vacio la Tabla de Archivos
bool vaciarTablaArchivos(t_log* infoLogger){

	int resultado = -1; // Para el caso de error

	resultado = system("rm -f -R metadata/archivos");
	resultado = system("mkdir metadata/archivos");

	return resultado;
}

// Vacio los Bitmaps de los Nodos
bool vaciarBitmaps(t_log* infoLogger){

	int resultado = -1; // Para el caso de error

	resultado = system("rm -f -R metadata/bitmaps");
	resultado = system("mkdir metadata/bitmaps");

	return resultado;
}

// Obtengo el espacio libre de un nodo
int obtenerEspacioLibreNodo(t_list* listaTablaNodos, char* nombreNodo){
	
	int totalBloquesLibres = 0, i;
	t_bitmap* bitmap = NULL;
	char* nombreArchivoBitmap = string_new();
	//bitmap = malloc(sizeof(t_bitmap));

	nombreArchivoBitmap = string_from_format("metadata/bitmaps/%s.dat", nombreNodo);
	if (!existeArchivo(nombreArchivoBitmap)){
		return -1;
	}
	bitmap = cargarBitmap(listaTablaNodos, nombreNodo);

	// Cuento todos los bloques libres
	for (i = 0; i < bitmap->tamano; i++) { 

		// Si esta libre
		if(bitmap->contenido[i] == '0'){
			totalBloquesLibres++;
		}
	}

	free(bitmap->contenido);
	free(nombreArchivoBitmap);
	

	return totalBloquesLibres;
}
