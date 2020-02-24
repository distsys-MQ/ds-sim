CC=gcc
CFLAGS=-g -Wall -O3 -fgnu89-inline

all: ds-server ds-client

ds-server: ds_server.c log_normal.c
	$(CC) $(CFLAGS) `xml2-config --cflags` $^ -o $@ `xml2-config --libs` -lm

ds-client: ds_client.c
	$(CC) $(CFLAGS) `xml2-config --cflags` $^ -o $@ `xml2-config --libs`

clean:
	rm ds-server ds-client
 
