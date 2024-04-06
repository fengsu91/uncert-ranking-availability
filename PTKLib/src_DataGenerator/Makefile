# Makefile for datagenerator.cpp, exectutable: datagenerator.exe

# Ignore all error return codes
.IGNORE:

# Include .c & .cpp to the Suffix list
.SUFFIXES: .c .cpp  .o .obj

# Define the CPP compiler & options
CPP = cl
CFLAGS = /FE

# Define all files in project
SOURCE = datagenerator.cpp
OBJECTS = datagenerator.obj

# Recompile all object files in the current directory that hve changed

datagenerator : $(OBJECTS) # Recompile all source files
 
  $(CPP) datagenerator $(OBJECTS)

datagenerator.obj : datagenerator.cpp

output: $(SOURCE) $(HEADERS) # Print files

#print $< #output altered files.

#@echo Compilation completed.