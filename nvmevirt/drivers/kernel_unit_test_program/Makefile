CFLAGS= -g -Wall -D_GNU_SOURCE -m64 -fPIC -I src
LDFLAGS= -lm -lpthread
MYLIB=libkvnvme.a
MYLIB_SOURCE=src/kv_nvme.c
MYLIB_OBJ=$(BUILD_DIR)/$(notdir $(MYLIB_SOURCE:.c=.o))
	TEST_PROG_SRCS=$(filter-out $(MYLIB_SOURCE), $(wildcard src/*.c))
	TEST_PROGS=$(patsubst src/%.c,$(BUILD_DIR)/%,$(TEST_PROG_SRCS))

AR=ar
CC=gcc
BUILD_DIR=build

all: $(BUILD_DIR) $(BUILD_DIR)/$(MYLIB) $(TEST_PROGS)

$(BUILD_DIR):
	    mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/$(MYLIB): $(MYLIB_OBJ)
	    $(AR) -cvq $@ $(MYLIB_OBJ)

$(BUILD_DIR)/%.o: src/%.c
	    $(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR)/%: src/%.c $(BUILD_DIR)/$(MYLIB)
	    $(CC) $(CFLAGS) $< -o $@ $(MYLIB_OBJ) $(LDFLAGS)

clean:
	    rm -rf $(BUILD_DIR)
