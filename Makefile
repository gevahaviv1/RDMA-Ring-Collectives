CC = gcc
CFLAGS = -Wall -Wextra -O2 -g
LIBS = -libverbs -lpthread

SRCS = bootstrap.c chunk_planner.c pg.c read_engine.c reduce.c ring.c rtscts.c serverlist.c
OBJS = $(SRCS:.c=.o)

all: libpg.a

libpg.a: $(OBJS)
	ar rcs $@ $^

clean:
	rm -f $(OBJS) libpg.a

format:
	clang-format -i *.c *.h

.PHONY: all clean format
