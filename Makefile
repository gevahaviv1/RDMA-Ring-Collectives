CC = gcc
CFLAGS = -Wall -O2 -g -Iinclude -Isrc
LDFLAGS = -lpthread -libverbs

SRC = src/pg.c src/pg_net.c src/RDMA_api.c src/test_connect.c
OBJ = $(SRC:.c=.o)

TARGET = test_connect

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CC) $(OBJ) -o $@ $(LDFLAGS)

clean:
	rm -f $(OBJ) $(TARGET)

