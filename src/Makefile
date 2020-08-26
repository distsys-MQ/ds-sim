CC=gcc
CFLAGS=-g -Wall -O3 -fgnu89-inline

all: ds-server

ds-server: ds_server.c log_normal.c
	$(CC) $(CFLAGS) `xml2-config --cflags` $^ -o $@ `xml2-config --libs` -lm

clean:
	rm ds-server
 
