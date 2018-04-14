COMPILER=gcc

all:
	$(COMPILER) -c -g sockets.c
	$(COMPILER) -c -g funciones.c
	$(COMPILER) -o master master.c sockets.o funciones.o  -lcommons -lpthread
	$(COMPILER) -o worker worker.c sockets.o funciones.o  -lcommons -lpthread
	$(COMPILER) -o datanode datanode.c sockets.o funciones.o  -lcommons
	$(COMPILER) -o fs fs.c sockets.o funciones.o  -lcommons -lreadline
	$(COMPILER) -o yama yama.c sockets.o funciones.o  -lcommons

clean:
	rm -rf bin

sockets:
	$(COMPILER) -c -g sockets.c
	
funciones:
	$(COMPILER) -c -g funciones.c
		
master:
	$(COMPILER) -o master master.c sockets.o funciones.o  -lcommons

worker:
	$(COMPILER) -o worker worker.c sockets.o funciones.o  -lcommons

datanode:
	$(COMPILER) -o datanode datanode.c sockets.o funciones.o  -lcommons

fs:
	$(COMPILER) -o fs fs.c sockets.o funciones.o  -lcommons -lreadline
	
yama:
	$(COMPILER) -o yama yama.c sockets.o funciones.o  -lcommons
	

	