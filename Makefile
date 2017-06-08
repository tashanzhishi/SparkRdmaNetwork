CC := g++
LIBS := -pthread -lboost_system -lboost_thread -libverbs
CXXFLAGS := -Wall -std=c++11

SOURCE_DIR := ./src
OBJECT_DIR := ./obj

SOURCES := $(wildcard $(SOURCE_DIR)/*.cpp)
OBJS := $(patsubst %.cpp, $(OBJECT_DIR)/%.o $(notdir $(SOURCES)))

OUTPUT := ./lib/libSparkRdma.so

all : $(OUTPUT)

$(OUTPUT) : $(OBJS)
	$(CC) $(CXXFLAGS) -fPIC -shared -o $@ $(OBJS) $(LIBS)

./obj/%.o : ./src/%.cpp
	$(CC) $(CXXFLAGS) -fPIC -c $< -o $@ $(LIBS)

install:
	cp $(OUTPUT) /usr/lib/
	cp $(OUTPUT) /usr/lib/libSparkRdma.so.0
	cp $(OUTPUT) /usr/lib/libSparkRdma.so.0.0.0

uninstall:
	rm -f /usr/lib/libSparkRdma.so /usr/lib/libSparkRdma.so.0 /usr/lib/libSparkRdma.so.0.0

.PHONY : clean

clean:
	rm -f $(OBJS) $(OUTPUT)