CC      ?= gcc
LIBDIR  := ../coseq
CFLAGS  ?= -Wall -O0 -g -std=c11
TARGET  =  sample

all: $(TARGET)

# lib をインストールせずその場で実行できるよう rpath を埋める
$(TARGET): sample.c $(LIBDIR)/libcoseq.so
	$(CC) $(CFLAGS) -I$(LIBDIR) sample.c -o $@ \
		-L$(LIBDIR) -lcoseq -Wl,-rpath,$(abspath $(LIBDIR)) -lpthread

$(LIBDIR)/libcoseq.so:
	$(MAKE) -C $(LIBDIR)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: all run clean
