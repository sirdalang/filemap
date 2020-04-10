
# makefile for filemap_test

# NOTE: 使用assert进行测试，因此不要禁用assert宏

TARGET=filemap_test

OBJDIR=obj

CC=g++

SRC=$(wildcard *.cpp)
OBJ=$(patsubst %.cpp,$(OBJDIR)/%.o,$(SRC))

LIBDIR+=-L../

LIB+=-lfilemap

HEADERDIR+=-I../

CFLAG=-Wall -std=c++11 -g

RM=rm -rf

all:$(TARGET)

$(TARGET):$(OBJ)
	$(CC) -o $@ $^  $(CFLAG) $(LIBDIR) $(LIB) $(HEADERDIR)

$(OBJDIR)/%.o:%.cpp
	@if [ ! -d $(OBJDIR) ]; then mkdir -p $(OBJDIR); fi;
	$(CC) -c $< -o $@ $(CFLAG)

.PHONY:
	clean all

clean:
	$(RM) $(OBJ)
	$(RM) $(TARGET)