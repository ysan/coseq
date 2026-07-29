CXX     ?= g++
CLIB    := ../coseq
PPLIB   := ../coseqpp
CXXFLAGS ?= -Wall -O0 -g -std=c++11
TARGET  =  sample

all: $(TARGET)

$(TARGET): sample.cpp $(CLIB)/libcoseq.so $(PPLIB)/libcoseqpp.so
	$(CXX) $(CXXFLAGS) -I$(CLIB) -I$(PPLIB) sample.cpp -o $@ \
		-L$(CLIB) -lcoseq -L$(PPLIB) -lcoseqpp \
		-Wl,-rpath,$(abspath $(CLIB)) -Wl,-rpath,$(abspath $(PPLIB)) -lpthread

$(CLIB)/libcoseq.so:
	$(MAKE) -C $(CLIB)

$(PPLIB)/libcoseqpp.so:
	$(MAKE) -C $(PPLIB)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: all run clean
