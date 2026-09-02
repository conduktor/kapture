OUTPUT ?= build
SOURCE := src
LIBBPF_SOURCE := vendor/libbpf
CLANG ?= clang
CC ?= cc
BPFTOOL ?= bpftool
PKG_CONFIG ?= pkg-config

UNAME_M := $(shell uname -m)
ifeq ($(UNAME_M),x86_64)
BPF_ARCH := x86
else ifeq ($(UNAME_M),aarch64)
BPF_ARCH := arm64
else ifeq ($(UNAME_M),arm64)
BPF_ARCH := arm64
else
$(error Unsupported architecture $(UNAME_M); supported: x86_64, aarch64)
endif

CFLAGS := -O2 -g -Wall -Wextra -Werror
LIBBPF_OUTPUT := $(abspath $(OUTPUT)/libbpf)
LIBBPF_ARCHIVE := $(LIBBPF_OUTPUT)/libbpf.a
LIBBPF_CFLAGS := -I$(LIBBPF_OUTPUT)/include
LIBBPF_STATIC_LIBS = $(LIBBPF_ARCHIVE) $(shell $(PKG_CONFIG) --static --libs libelf zlib)

.PHONY: all clean check rootful-smoke

all: $(OUTPUT)/kapture-ebpf-tap

$(OUTPUT):
	mkdir -p $(OUTPUT)

$(LIBBPF_ARCHIVE): | $(OUTPUT)
	$(MAKE) -C $(LIBBPF_SOURCE)/src BUILD_STATIC_ONLY=1 \
		OBJDIR=$(LIBBPF_OUTPUT) INCLUDEDIR=$(LIBBPF_OUTPUT)/include \
		all install_headers

$(OUTPUT)/vmlinux.h: | $(OUTPUT)
	@test -r /sys/kernel/btf/vmlinux || (echo "kernel BTF unavailable" >&2; exit 1)
	$(BPFTOOL) btf dump file /sys/kernel/btf/vmlinux format c > $@

$(OUTPUT)/kapture_tls.bpf.o: $(SOURCE)/kapture_tls.bpf.c $(SOURCE)/kapture_tls.h $(OUTPUT)/vmlinux.h $(LIBBPF_ARCHIVE)
	$(CLANG) -target bpf -D__TARGET_ARCH_$(BPF_ARCH) -O2 -g \
		-I$(OUTPUT) -I$(SOURCE) $(LIBBPF_CFLAGS) -c $< -o $@

$(OUTPUT)/kapture_tls.skel.h: $(OUTPUT)/kapture_tls.bpf.o
	$(BPFTOOL) gen skeleton $< > $@

$(OUTPUT)/kapture-ebpf-tap: $(SOURCE)/kapture_tls.c $(SOURCE)/kapture_tls.h $(OUTPUT)/kapture_tls.skel.h $(LIBBPF_ARCHIVE)
	$(CC) $(CFLAGS) -I$(OUTPUT) -I$(SOURCE) $(LIBBPF_CFLAGS) $< \
		-Wl,-Bstatic $(LIBBPF_STATIC_LIBS) -Wl,-Bdynamic -o $@

check: $(OUTPUT)/kapture-ebpf-tap
	@test -n "$(PID)" || (echo "usage: make check PID=<pid> SSL=/path/to/libssl.so" >&2; exit 1)
	@test -n "$(SSL)" || (echo "usage: make check PID=<pid> SSL=/path/to/libssl.so" >&2; exit 1)
	$(OUTPUT)/kapture-ebpf-tap --check --pid $(PID) --library $(SSL)

rootful-smoke: $(OUTPUT)/kapture-ebpf-tap
	python3 tests/rootful_smoke.py --loader $(OUTPUT)/kapture-ebpf-tap

clean:
	rm -f $(OUTPUT)/kapture-ebpf-tap $(OUTPUT)/kapture_tls.bpf.o \
		$(OUTPUT)/kapture_tls.skel.h $(OUTPUT)/vmlinux.h \
		$(LIBBPF_OUTPUT)/libbpf.a $(LIBBPF_OUTPUT)/libbpf.pc \
		$(LIBBPF_OUTPUT)/staticobjs/*.o
