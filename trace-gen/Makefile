CC=gcc
CFLAGS=-g -pedantic -Wall -Wsign-compare -Wwrite-strings -Wtype-limits

all: trace-gen

trace-gen: trace_gen.c log_normal.c
	$(CC) $(CFLAGS) `xml2-config --cflags` $^ -o $@ `xml2-config --libs` -lm

clean:
	rm trace-gen
