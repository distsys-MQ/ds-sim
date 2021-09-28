# for the xml library, use 'sudo apt-get install libxml2-dev'
CC=gcc
CFLAGS=-g -Wall -O3 -fgnu89-inline

DS_SERVER_SOURCES =	ds_server.c log_normal.c

all: ds-server ds-client

ds-server: $(DS_SERVER_SOURCES)
	$(CC) $(CFLAGS) `xml2-config --cflags` $^ -o $@ `xml2-config --libs` -lm

ds-client: ds_client.c
	$(CC) $(CFLAGS) `xml2-config --cflags` $^ -o $@ `xml2-config --libs`

clean:
	rm ds-server ds-client
 
