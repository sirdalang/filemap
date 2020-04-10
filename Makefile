
# makefile for filemap

TARGET=libfilemap.a

OBJDIR=obj

CC=gcc

SRC=$(wildcard *.c)
OBJ=$(patsubst %.c,$(OBJDIR)/%.o,$(SRC))

CFLAG=-Wall -g

RM=rm -rf

all:$(TARGET)

$(TARGET):$(OBJ)
	ar crD $@ $^

$(OBJDIR)/%.o:%.c
	@if [ ! -d $(OBJDIR) ]; then mkdir -p $(OBJDIR); fi;
	$(CC) -c $< -o $@ $(CFLAG)

.PHONY:
	clean all

clean:
	$(RM) $(OBJ)
	$(RM) $(TARGET)