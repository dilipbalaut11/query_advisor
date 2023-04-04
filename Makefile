EXTENSION    = query_advisor
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test
#MODULES      = $(patsubst %.c,%,$(wildcard *.c))
PG_CONFIG    ?= pg_config

MODULE_big = query_advisor
OBJS = pg_qualstats.o \
       query_advisor.o \
       hypopg.o \
       hypopg_index.o \
       import/hypopg_import.o \
       import/hypopg_import_index.o
all:



DATA = $(wildcard *--*.sql)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
