# Copyright Hadapt, Inc. 2013
# All rights reserved.

OBJS = bw_colupgrade.o
MODULE_big = bw_colupgrade
EXTENSION = bw_colupgrade
DATA = bw_colupgrade--1.0.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/bw_colupgrade
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
