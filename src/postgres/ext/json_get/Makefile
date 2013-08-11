# Copyright Hadapt, Inc. 2013
# All rights reserved.

OBJS = json_get.o
MODULE_big = json_get
EXTENSION = json_get
DATA = json_get--1.0.0.sql

jsmndir = lib/jsmn
LIBS += $(jsmndir)/libjsmn.a
SHLIB_LINK := $(LIBS)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/json_get
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
