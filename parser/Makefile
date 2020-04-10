
# makefile for filemap

TARGET=filemap_parser

OBJDIR=obj

CC=gcc

SRC=$(wildcard *.c)
OBJ=$(patsubst %.c,$(OBJDIR)/%.o,$(SRC))

LIBDIR+=-L../
LIB+=-lfilemap
HEADERDIR+=-I../

CFLAG=-Wall -g 

RM=rm -rf

all:$(TARGET)

$(TARGET):$(OBJ)
	$(CC) -o $@ $^ $(LIBDIR) $(LIB) $(HEADERDIR)

$(OBJDIR)/%.o:%.c
	@if [ ! -d $(OBJDIR) ]; then mkdir -p $(OBJDIR); fi;
	$(CC) -c $< -o $@ $(CFLAG)

.PHONY:
	clean all

clean:
	$(RM) $(OBJ)
	$(RM) $(TARGET)