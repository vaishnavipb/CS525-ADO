CC=gcc
CFLAGS=-I.
DEPS = dberror.h storage_mgr.h buffer_mgr.h buffer_mgr_stat.h test_helper.h expr.h rm_serializer.o record_mgr.h btree_mgr.h

OBJ = dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o expr.o record_mgr.o btree_mgr.o

all: test_assign4_1 test_expr

test_assign4_1: test_assign4_1.o $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS)

test_expr: test_expr.o $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS)

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

.PHONY : clean
clean :
	$(RM) *.o test_assign4_1 -r
	$(RM) *.o test_expr -r
