# Copyright Hadapt, Inc. 2013
# All rights reserved.

OBJS = document.o
MODULE_big = document
EXTENSION = document
DATA = document--1.0.0.sql

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
