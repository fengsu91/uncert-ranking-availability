# contrib/range_window/Makefile

MODULE_big = range_window
OBJS = \
	$(WIN32RES) \
	range_window.o

EXTENSION = range_window

DATA = range_window--1.0.sql

PGFILEDESC = "range_window - models dealing with uncertainty"

# PG_CPPFLAGS = -I$(libpq_srcdir)
# SHLIB_LINK_INTERNAL = $(libpq)

# DOCS = $(addsuffix .example, $(MODULES))

# this is needed for the regression tests;
# comment out if you want a quieter refint package for other uses
# PG_CPPFLAGS = -DREFINT_VERBOSE

REGRESS = range_window

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/uncert
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
