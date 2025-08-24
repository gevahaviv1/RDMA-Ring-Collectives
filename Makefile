CC = gcc
CFLAGS = -Wall -Wextra -O2 -g
LIBS = -libverbs -lpthread
INCLUDES = -Iinclude

SRCS = src/pg.c src/pg_net.c src/RDMA_api.c
OBJS = $(SRCS:.c=.o)

all: libpg.a

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

libpg.a: $(OBJS)
	ar rcs $@ $^

clean:
	rm -f $(OBJS) libpg.a

.PHONY: all clean
