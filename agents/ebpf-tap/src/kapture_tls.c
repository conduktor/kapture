#define _GNU_SOURCE

#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <linux/bpf.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "kapture_tls.h"
#include "kapture_tls.skel.h"

#define FRAME_HEADER_SIZE 25
#define DIRECTION_HEALTH 2
#define SEQUENCE_SLOTS 65536
#define LINK_CAPACITY 16

static volatile sig_atomic_t stopping;

struct sequence_slot {
    uint64_t key;
    uint64_t expected;
    bool used;
};

struct application {
    int socket_fd;
    struct sequence_slot sequences[SEQUENCE_SLOTS];
    uint64_t last_stats[KAPTURE_STAT_COUNT];
};

struct options {
    int pid;
    const char *library;
    const char *socket_path;
    bool check_only;
};

static void on_signal(int signal_number)
{
    (void)signal_number;
    stopping = 1;
}

static uint64_t monotonic_nanos(void)
{
    struct timespec value = {};
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0)
        return 0;
    return (uint64_t)value.tv_sec * 1000000000ULL + (uint64_t)value.tv_nsec;
}

static void put_le32(unsigned char *output, uint32_t value)
{
    for (size_t i = 0; i < 4; ++i)
        output[i] = (unsigned char)(value >> (i * 8));
}

static void put_le64(unsigned char *output, uint64_t value)
{
    for (size_t i = 0; i < 8; ++i)
        output[i] = (unsigned char)(value >> (i * 8));
}

static int write_all(int fd, const void *buffer, size_t length)
{
    const unsigned char *cursor = buffer;
    while (length > 0) {
        ssize_t written = send(fd, cursor, length, MSG_NOSIGNAL);
        if (written < 0) {
            if (errno == EINTR)
                continue;
            return -errno;
        }
        cursor += (size_t)written;
        length -= (size_t)written;
    }
    return 0;
}

static int send_frame(struct application *app, const struct kapture_event *event)
{
    unsigned char header[FRAME_HEADER_SIZE] = {};
    header[0] = event->direction;
    put_le64(header + 1, event->observed_nanos);
    put_le64(header + 9, monotonic_nanos());
    put_le32(header + 17, event->connection_id);
    put_le32(header + 21, event->length);
    int error = write_all(app->socket_fd, header, sizeof(header));
    if (error == 0)
        error = write_all(app->socket_fd, event->data, event->length);
    return error;
}

static int send_health(struct application *app, uint64_t drops)
{
    unsigned char header[FRAME_HEADER_SIZE] = {};
    unsigned char payload[8] = {};
    uint64_t now = monotonic_nanos();
    header[0] = DIRECTION_HEALTH;
    put_le64(header + 1, now);
    put_le64(header + 9, now);
    put_le32(header + 21, sizeof(payload));
    put_le64(payload, drops);
    int error = write_all(app->socket_fd, header, sizeof(header));
    if (error == 0)
        error = write_all(app->socket_fd, payload, sizeof(payload));
    return error;
}

static uint64_t stream_key(const struct kapture_event *event)
{
    uint64_t key = ((uint64_t)event->tgid << 32) | event->connection_id;
    return key ^ ((uint64_t)event->direction << 63);
}

static uint64_t sequence_gap(struct application *app, const struct kapture_event *event)
{
    uint64_t key = stream_key(event);
    size_t index = (size_t)((key * 11400714819323198485ULL) >> 48);
    for (size_t probe = 0; probe < SEQUENCE_SLOTS; ++probe) {
        struct sequence_slot *slot = &app->sequences[(index + probe) % SEQUENCE_SLOTS];
        if (!slot->used) {
            slot->used = true;
            slot->key = key;
            slot->expected = event->sequence + 1;
            return event->sequence;
        }
        if (slot->key == key) {
            uint64_t gap = event->sequence == slot->expected
                               ? 0
                               : (event->sequence > slot->expected
                                      ? event->sequence - slot->expected
                                      : 1);
            slot->expected = event->sequence + 1;
            return gap;
        }
    }
    return 1;
}

static int handle_event(void *context, void *data, size_t size)
{
    struct application *app = context;
    const struct kapture_event *event = data;
    if (size < sizeof(*event) || event->length > KAPTURE_CHUNK_MAX ||
        sizeof(*event) + event->length > size) {
        fprintf(stderr, "kapture-ebpf-tap: malformed ring-buffer event; invalidating stream\n");
        send_health(app, 1);
        stopping = 1;
        return -EINVAL;
    }

    uint64_t gap = sequence_gap(app, event);
    if (gap != 0) {
        fprintf(stderr,
                "kapture-ebpf-tap: sequence gap (%llu chunk(s)); invalidating UDS session\n",
                (unsigned long long)gap);
        send_health(app, gap);
        stopping = 1;
        return -EPIPE;
    }

    int error = send_frame(app, event);
    if (error != 0) {
        fprintf(stderr, "kapture-ebpf-tap: Kapture socket write failed: %s\n", strerror(-error));
        stopping = 1;
    }
    return error;
}

static int connect_socket(const char *path)
{
    if (strlen(path) >= sizeof(((struct sockaddr_un *)0)->sun_path)) {
        fprintf(stderr, "kapture-ebpf-tap: socket path is too long: %s\n", path);
        return -ENAMETOOLONG;
    }
    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0)
        return -errno;
    struct sockaddr_un address = {.sun_family = AF_UNIX};
    strcpy(address.sun_path, path);
    if (connect(fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
        int error = -errno;
        close(fd);
        return error;
    }
    return fd;
}

static struct bpf_link *attach_symbol(struct bpf_program *program,
                                      int pid,
                                      const char *library,
                                      const char *symbol,
                                      bool return_probe)
{
    LIBBPF_OPTS(bpf_uprobe_opts, options,
                .retprobe = return_probe,
                .func_name = symbol);
    return bpf_program__attach_uprobe_opts(program, pid, library, 0, &options);
}

static int attach_pair(struct bpf_program *entry_program,
                       struct bpf_program *exit_program,
                       int pid,
                       const char *library,
                       const char *symbol,
                       bool required,
                       struct bpf_link **links,
                       size_t *count)
{
    struct bpf_link *entry = attach_symbol(entry_program, pid, library, symbol, false);
    struct bpf_link *exit = attach_symbol(exit_program, pid, library, symbol, true);
    long entry_error = libbpf_get_error(entry);
    long exit_error = libbpf_get_error(exit);
    if (entry_error == 0 && exit_error == 0) {
        links[(*count)++] = entry;
        links[(*count)++] = exit;
        return 0;
    }
    if (entry_error == 0)
        bpf_link__destroy(entry);
    if (exit_error == 0)
        bpf_link__destroy(exit);
    if (required) {
        fprintf(stderr, "kapture-ebpf-tap: required OpenSSL symbol %s unavailable: %s\n",
                symbol, strerror((int)-(entry_error != 0 ? entry_error : exit_error)));
        return (int)(entry_error != 0 ? entry_error : exit_error);
    }
    fprintf(stderr, "kapture-ebpf-tap: optional OpenSSL symbol %s unavailable; continuing\n",
            symbol);
    return 0;
}

static int attach_tls_probes(struct kapture_tls_bpf *skeleton,
                             const struct options *options,
                             struct bpf_link **links,
                             size_t *count)
{
    int error = attach_pair(skeleton->progs.ssl_read_enter, skeleton->progs.ssl_read_exit,
                            options->pid, options->library, "SSL_read", true, links, count);
    if (error != 0)
        return error;
    error = attach_pair(skeleton->progs.ssl_write_enter, skeleton->progs.ssl_write_exit,
                        options->pid, options->library, "SSL_write", true, links, count);
    if (error != 0)
        return error;
    error = attach_pair(skeleton->progs.ssl_read_ex_enter, skeleton->progs.ssl_read_ex_exit,
                        options->pid, options->library, "SSL_read_ex", false, links, count);
    if (error != 0)
        return error;
    return attach_pair(skeleton->progs.ssl_write_ex_enter, skeleton->progs.ssl_write_ex_exit,
                       options->pid, options->library, "SSL_write_ex", false, links, count);
}

static void attach_optional_system_probes(struct kapture_tls_bpf *skeleton,
                                          struct bpf_link **links,
                                          size_t *count)
{
    struct bpf_program *program = NULL;
    bpf_object__for_each_program(program, skeleton->obj) {
        const char *section = bpf_program__section_name(program);
        if (strncmp(section, "tracepoint/", 11) != 0 && strncmp(section, "kprobe/", 7) != 0)
            continue;
        struct bpf_link *link = bpf_program__attach(program);
        long error = libbpf_get_error(link);
        if (error == 0 && *count < LINK_CAPACITY)
            links[(*count)++] = link;
        else if (error != 0)
            fprintf(stderr, "kapture-ebpf-tap: optional diagnostic %s unavailable\n", section);
    }
}

static int read_stats(struct kapture_tls_bpf *skeleton, uint64_t output[KAPTURE_STAT_COUNT])
{
    int cpu_count = libbpf_num_possible_cpus();
    if (cpu_count <= 0)
        return -EINVAL;
    uint64_t *per_cpu = calloc((size_t)cpu_count, sizeof(*per_cpu));
    if (!per_cpu)
        return -ENOMEM;
    int map_fd = bpf_map__fd(skeleton->maps.stats);
    for (uint32_t key = 0; key < KAPTURE_STAT_COUNT; ++key) {
        if (bpf_map_lookup_elem(map_fd, &key, per_cpu) != 0) {
            free(per_cpu);
            return -errno;
        }
        output[key] = 0;
        for (int cpu = 0; cpu < cpu_count; ++cpu)
            output[key] += per_cpu[cpu];
    }
    free(per_cpu);
    return 0;
}

static void print_runtime_stats(struct kapture_tls_bpf *skeleton)
{
    struct bpf_program *program = NULL;
    bpf_object__for_each_program(program, skeleton->obj) {
        struct bpf_prog_info info = {};
        uint32_t length = sizeof(info);
        if (bpf_obj_get_info_by_fd(bpf_program__fd(program), &info, &length) == 0) {
            fprintf(stderr, "kapture-ebpf-tap: program=%s runs=%llu runtime_ns=%llu\n",
                    bpf_program__name(program),
                    (unsigned long long)info.run_cnt,
                    (unsigned long long)info.run_time_ns);
        }
    }
}

static void print_capture_stats(struct kapture_tls_bpf *skeleton)
{
    uint64_t stats[KAPTURE_STAT_COUNT] = {};
    if (read_stats(skeleton, stats) != 0)
        return;
    fprintf(stderr,
            "kapture-ebpf-tap: events=%llu ring_drops=%llu read_faults=%llu "
            "oversize_calls=%llu connects=%llu connect_errors=%llu retransmits=%llu "
            "offcpu_ns=%llu\n",
            (unsigned long long)stats[KAPTURE_STAT_EVENTS],
            (unsigned long long)stats[KAPTURE_STAT_RING_DROPS],
            (unsigned long long)stats[KAPTURE_STAT_READ_FAULTS],
            (unsigned long long)stats[KAPTURE_STAT_OVERSIZE_CALLS],
            (unsigned long long)stats[KAPTURE_STAT_CONNECTS],
            (unsigned long long)stats[KAPTURE_STAT_CONNECT_ERRORS],
            (unsigned long long)stats[KAPTURE_STAT_RETRANSMITS],
            (unsigned long long)stats[KAPTURE_STAT_OFFCPU_NS]);
}

static void usage(FILE *stream, const char *program)
{
    fprintf(stream,
            "Usage: %s --pid PID --library /path/to/libssl.so --socket /tmp/kapture-tap.sock [--check]\n",
            program);
}

static int parse_options(int argc, char **argv, struct options *options)
{
    for (int index = 1; index < argc; ++index) {
        if (strcmp(argv[index], "--check") == 0) {
            options->check_only = true;
        } else if (index + 1 < argc && strcmp(argv[index], "--pid") == 0) {
            options->pid = atoi(argv[++index]);
        } else if (index + 1 < argc && strcmp(argv[index], "--library") == 0) {
            options->library = argv[++index];
        } else if (index + 1 < argc && strcmp(argv[index], "--socket") == 0) {
            options->socket_path = argv[++index];
        } else {
            usage(stderr, argv[0]);
            return -EINVAL;
        }
    }
    if (options->pid <= 0 || !options->library || (!options->check_only && !options->socket_path)) {
        usage(stderr, argv[0]);
        return -EINVAL;
    }
    return 0;
}

int main(int argc, char **argv)
{
    struct options options = {};
    int error = parse_options(argc, argv, &options);
    if (error != 0)
        return EXIT_FAILURE;
    if (access("/sys/kernel/btf/vmlinux", R_OK) != 0) {
        fprintf(stderr, "kapture-ebpf-tap: kernel BTF unavailable at /sys/kernel/btf/vmlinux\n");
        return EXIT_FAILURE;
    }
    if (kill(options.pid, 0) != 0 || access(options.library, R_OK) != 0) {
        fprintf(stderr, "kapture-ebpf-tap: PID or OpenSSL library is not accessible\n");
        return EXIT_FAILURE;
    }

    libbpf_set_strict_mode(LIBBPF_STRICT_ALL);
    struct kapture_tls_bpf *skeleton = kapture_tls_bpf__open();
    if (!skeleton) {
        fprintf(stderr, "kapture-ebpf-tap: cannot open BPF object\n");
        return EXIT_FAILURE;
    }
    error = kapture_tls_bpf__load(skeleton);
    if (error != 0) {
        fprintf(stderr,
                "kapture-ebpf-tap: BPF load failed (%s); check CAP_BPF/CAP_PERFMON or run as root\n",
                strerror(-error));
        kapture_tls_bpf__destroy(skeleton);
        return EXIT_FAILURE;
    }
    int stats_fd = bpf_enable_stats(BPF_STATS_RUN_TIME);
    if (stats_fd < 0) {
        fprintf(stderr,
                "kapture-ebpf-tap: BPF runtime statistics unavailable: %s\n",
                strerror(-stats_fd));
    }
    uint32_t zero = 0;
    uint32_t target = (uint32_t)options.pid;
    if (bpf_map_update_elem(bpf_map__fd(skeleton->maps.target_tgid), &zero, &target, BPF_ANY) != 0) {
        fprintf(stderr, "kapture-ebpf-tap: cannot configure target PID: %s\n", strerror(errno));
        if (stats_fd >= 0)
            close(stats_fd);
        kapture_tls_bpf__destroy(skeleton);
        return EXIT_FAILURE;
    }

    struct bpf_link *links[LINK_CAPACITY] = {};
    size_t link_count = 0;
    error = attach_tls_probes(skeleton, &options, links, &link_count);
    if (error != 0)
        goto cleanup;
    attach_optional_system_probes(skeleton, links, &link_count);

    if (options.check_only) {
        printf("supported: PID %d, %s, kernel BTF and required SSL symbols are usable\n",
               options.pid, options.library);
        error = 0;
        goto cleanup;
    }

    struct application app = {.socket_fd = connect_socket(options.socket_path)};
    if (app.socket_fd < 0) {
        error = app.socket_fd;
        fprintf(stderr, "kapture-ebpf-tap: cannot connect to %s: %s\n",
                options.socket_path, strerror(-error));
        goto cleanup;
    }
    send_health(&app, 0);

    struct ring_buffer *ring = ring_buffer__new(bpf_map__fd(skeleton->maps.events),
                                                handle_event, &app, NULL);
    if (!ring) {
        error = -errno;
        close(app.socket_fd);
        goto cleanup;
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    while (!stopping) {
        error = ring_buffer__poll(ring, 250);
        if (error == -EINTR) {
            error = 0;
            break;
        }
        if (error < 0)
            break;

        uint64_t current[KAPTURE_STAT_COUNT] = {};
        if (read_stats(skeleton, current) == 0) {
            uint64_t corrupting_loss =
                current[KAPTURE_STAT_RING_DROPS] - app.last_stats[KAPTURE_STAT_RING_DROPS] +
                current[KAPTURE_STAT_READ_FAULTS] - app.last_stats[KAPTURE_STAT_READ_FAULTS] +
                current[KAPTURE_STAT_OVERSIZE_CALLS] -
                    app.last_stats[KAPTURE_STAT_OVERSIZE_CALLS];
            memcpy(app.last_stats, current, sizeof(current));
            if (corrupting_loss != 0) {
                fprintf(stderr,
                        "kapture-ebpf-tap: kernel capture lost %llu chunk(s); invalidating UDS session\n",
                        (unsigned long long)corrupting_loss);
                send_health(&app, corrupting_loss);
                error = -ENOBUFS;
                break;
            }
        }
    }
    ring_buffer__free(ring);
    close(app.socket_fd);
    print_capture_stats(skeleton);
    print_runtime_stats(skeleton);

cleanup:
    for (size_t index = 0; index < link_count; ++index)
        bpf_link__destroy(links[index]);
    kapture_tls_bpf__destroy(skeleton);
    if (stats_fd >= 0)
        close(stats_fd);
    return error == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
