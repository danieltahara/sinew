# Copyright Hadapt, Inc. 2013
# All rights reserved.

OBJS = serde.o document.o schema.o accesors.o json.o utils.o
MODULE_big = document_type
EXTENSION = document_type
DATA = document_type--1.0.0.sql

jsmndir = lib/jsmn
LIBS += $(jsmndir)/libjsmn.a
SHLIB_LINK := $(LIBS)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/document
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
