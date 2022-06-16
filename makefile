GCC = g++ -std=c++17 -lnuma
MPICC = mpicxx -std=c++17 -g -lnuma   
CXXFLAGS := -O0 -Wall -Wfatal-errors -pthread
MPICCFLAGS = -O0 -Wall -Wfatal-errors -pthread 
LIBRA = lib/libspdlog.a

LDFLAGS  := -L${HOME}/.local/lib -lgeos
MAKE_DIR = $(PWD)
OBJ_DIR  := $(MAKE_DIR)/bin
APP_DIR  := $(MAKE_DIR)
TEST_DIR := $(MAKE_DIR)/tests
TARGET   := prog
INCLUDE  := -I/usr/local/include -isystem ${HOME}/.local/include -Iinclude
SRC      :=                      \
   $(wildcard src/*.cpp)         \
   $(wildcard tests/*.cpp)       \

OBJECTS := $(SRC:%.cpp=$(OBJ_DIR)/%.o)

all: build $(APP_DIR)/$(TARGET)

$(OBJ_DIR)/%.o: %.cpp
	@mkdir -p $(@D)
	$(MPICC) $(MPICCFLAGS) $(INCLUDE) -o $@ -c $<

$(APP_DIR)/$(TARGET): $(OBJECTS)
	@mkdir -p $(@D)
	$(MPICC) $(MPICCFLAGS) $(INCLUDE) -o $(APP_DIR)/$(TARGET) $(OBJECTS) $(LDFLAGS) 

.PHONY: all build clean debug release

build:
	@mkdir -p $(OBJ_DIR)

debug: MPICCFLAGS += -DDEBUG -g
debug: all

release: MPICCFLAGS += -O2
release: all

clean:
	-@rm -rvf $(OBJ_DIR)/*
	-@rm -rvf $(APP_DIR)/$(TARGET)
