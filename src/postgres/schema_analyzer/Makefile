# Copyright Hadapt, Inc. 2013
# All rights reserved.

OBJS = schema_analyzer.o
MODULE_big = schema_analyzer
EXTENSION = schema_analyzer
DATA = schema_analyzer--1.0.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/schema_analyzer
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
