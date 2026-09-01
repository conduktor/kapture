#include "vmlinux.h"
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

#include "kapture_tls.h"

char LICENSE[] SEC("license") = "Dual BSD/GPL";

struct ssl_call {
    __u64 ssl;
    __u64 buffer;
    __u64 requested;
    __u64 result_length;
};

struct stream_key {
    __u32 tgid;
    __u32 connection_id;
    __u8 direction;
    __u8 padding[3];
};

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u32);
} target_tgid SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 16384);
    __type(key, __u64);
    __type(value, struct ssl_call);
} read_calls SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 16384);
    __type(key, __u64);
    __type(value, struct ssl_call);
} write_calls SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 32768);
    __type(key, struct stream_key);
    __type(value, __u64);
} sequences SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, KAPTURE_STAT_COUNT);
    __type(key, __u32);
    __type(value, __u64);
} stats SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 32768);
    __type(key, __u32);
    __type(value, __u64);
} offcpu_starts SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 16 * 1024 * 1024);
} events SEC(".maps");

static __always_inline bool selected_process(__u64 pid_tgid)
{
    __u32 zero = 0;
    __u32 *selected = bpf_map_lookup_elem(&target_tgid, &zero);
    return selected && *selected != 0 && *selected == (__u32)(pid_tgid >> 32);
}

static __always_inline void increment_stat(__u32 index, __u64 amount)
{
    __u64 *value = bpf_map_lookup_elem(&stats, &index);
    if (value)
        *value += amount;
}

static __always_inline __u32 connection_id(__u64 ssl)
{
    return (__u32)(ssl ^ (ssl >> 32));
}

static __always_inline __u64 next_sequence(const struct stream_key *key)
{
    __u64 initial = 0;
    __u64 *sequence = bpf_map_lookup_elem(&sequences, key);
    if (!sequence) {
        bpf_map_update_elem(&sequences, key, &initial, BPF_NOEXIST);
        sequence = bpf_map_lookup_elem(&sequences, key);
    }
    return sequence ? __sync_fetch_and_add(sequence, 1) : 0;
}

/*
 * bpf_ringbuf_reserve() requires a verifier-known constant size on kernels
 * that predate ring-buffer dynptrs. Keep four fixed reservation classes so a
 * small Kafka frame does not consume a full 16 KiB ring record.
 */
#define DEFINE_SUBMIT_CHUNK(name, capacity)                                                   \
    static __noinline int name(const struct stream_key *key,                                  \
                               __u64 buffer,                                                   \
                               __u64 length,                                                   \
                               __u64 sequence,                                                 \
                               __u64 observed_nanos)                                           \
    {                                                                                          \
        if (length == 0 || length > (capacity))                                                \
            return 1;                                                                          \
        struct kapture_event *event =                                                         \
            bpf_ringbuf_reserve(&events, sizeof(struct kapture_event) + (capacity), 0);        \
        if (!event) {                                                                          \
            increment_stat(KAPTURE_STAT_RING_DROPS, 1);                                       \
            return 1;                                                                          \
        }                                                                                      \
        event->tgid = key->tgid;                                                               \
        event->tid = (__u32)bpf_get_current_pid_tgid();                                       \
        event->connection_id = key->connection_id;                                             \
        event->direction = key->direction;                                                     \
        event->reserved[0] = 0;                                                                \
        event->reserved[1] = 0;                                                                \
        event->reserved[2] = 0;                                                                \
        event->sequence = sequence;                                                            \
        event->observed_nanos = observed_nanos;                                                \
        event->length = (__u32)length;                                                         \
        if (bpf_probe_read_user(event->data, length, (void *)buffer) != 0) {                   \
            bpf_ringbuf_discard(event, 0);                                                     \
            increment_stat(KAPTURE_STAT_READ_FAULTS, 1);                                      \
            return 1;                                                                          \
        }                                                                                      \
        bpf_ringbuf_submit(event, 0);                                                          \
        increment_stat(KAPTURE_STAT_EVENTS, 1);                                                \
        return 0;                                                                              \
    }

DEFINE_SUBMIT_CHUNK(submit_256, 256)
DEFINE_SUBMIT_CHUNK(submit_1024, 1024)
DEFINE_SUBMIT_CHUNK(submit_4096, 4096)
DEFINE_SUBMIT_CHUNK(submit_16384, KAPTURE_CHUNK_MAX)

static __always_inline int submit_chunk(const struct stream_key *key,
                                        __u64 buffer,
                                        __u64 length,
                                        __u64 sequence,
                                        __u64 observed_nanos)
{
    if (length <= 256)
        return submit_256(key, buffer, length, sequence, observed_nanos);
    if (length <= 1024)
        return submit_1024(key, buffer, length, sequence, observed_nanos);
    if (length <= 4096)
        return submit_4096(key, buffer, length, sequence, observed_nanos);
    return submit_16384(key, buffer, length, sequence, observed_nanos);
}

struct chunk_context {
    struct stream_key key;
    __u64 buffer;
    __u64 length;
    __u64 observed_nanos;
};

static long emit_chunk(__u32 chunk_index, void *opaque_context)
{
    struct chunk_context *context = opaque_context;
    __u64 offset = (__u64)chunk_index * KAPTURE_CHUNK_MAX;
    if (offset >= context->length)
        return 1;
    __u64 chunk_length = context->length - offset;
    if (chunk_length > KAPTURE_CHUNK_MAX)
        chunk_length = KAPTURE_CHUNK_MAX;
    __u64 sequence = next_sequence(&context->key);
    if (submit_chunk(&context->key,
                     context->buffer + offset,
                     chunk_length,
                     sequence,
                     context->observed_nanos) != 0)
        return 1;
    return offset + chunk_length >= context->length;
}

static __always_inline int remember_call(void *map, struct pt_regs *ctx, bool extended)
{
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    if (!selected_process(pid_tgid))
        return 0;

    struct ssl_call call = {
        .ssl = PT_REGS_PARM1_CORE(ctx),
        .buffer = PT_REGS_PARM2_CORE(ctx),
        .requested = PT_REGS_PARM3_CORE(ctx),
        .result_length = extended ? PT_REGS_PARM4_CORE(ctx) : 0,
    };
    bpf_map_update_elem(map, &pid_tgid, &call, BPF_ANY);
    return 0;
}

static __always_inline int emit_call(void *map, struct pt_regs *ctx, __u8 direction, bool extended)
{
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    struct ssl_call *saved = bpf_map_lookup_elem(map, &pid_tgid);
    if (!saved)
        return 0;

    struct ssl_call call = *saved;
    bpf_map_delete_elem(map, &pid_tgid);

    long result = PT_REGS_RC_CORE(ctx);
    __u64 length = 0;
    if (extended) {
        if (result != 1 || !call.result_length)
            return 0;
        if (bpf_probe_read_user(&length, sizeof(length), (void *)call.result_length) != 0) {
            increment_stat(KAPTURE_STAT_READ_FAULTS, 1);
            return 0;
        }
    } else {
        if (result <= 0)
            return 0;
        length = (__u64)result;
    }

    if (length > call.requested)
        length = call.requested;
    if (length == 0)
        return 0;
    if (length > KAPTURE_CALL_MAX) {
        /* Never forward a truncated byte stream to the Kafka decoder. */
        increment_stat(KAPTURE_STAT_OVERSIZE_CALLS, 1);
        return 0;
    }

    struct chunk_context chunk_context = {
        .key = {
            .tgid = (__u32)(pid_tgid >> 32),
            .connection_id = connection_id(call.ssl),
            .direction = direction,
        },
        .buffer = call.buffer,
        .length = length,
        .observed_nanos = bpf_ktime_get_ns(),
    };
    bpf_loop(KAPTURE_MAX_CHUNKS, emit_chunk, &chunk_context, 0);
    return 0;
}

SEC("uprobe") int ssl_read_enter(struct pt_regs *ctx)
{
    return remember_call(&read_calls, ctx, false);
}

SEC("uretprobe") int ssl_read_exit(struct pt_regs *ctx)
{
    return emit_call(&read_calls, ctx, KAPTURE_READ, false);
}

SEC("uprobe") int ssl_read_ex_enter(struct pt_regs *ctx)
{
    return remember_call(&read_calls, ctx, true);
}

SEC("uretprobe") int ssl_read_ex_exit(struct pt_regs *ctx)
{
    return emit_call(&read_calls, ctx, KAPTURE_READ, true);
}

SEC("uprobe") int ssl_write_enter(struct pt_regs *ctx)
{
    return remember_call(&write_calls, ctx, false);
}

SEC("uretprobe") int ssl_write_exit(struct pt_regs *ctx)
{
    return emit_call(&write_calls, ctx, KAPTURE_WRITE, false);
}

SEC("uprobe") int ssl_write_ex_enter(struct pt_regs *ctx)
{
    return remember_call(&write_calls, ctx, true);
}

SEC("uretprobe") int ssl_write_ex_exit(struct pt_regs *ctx)
{
    return emit_call(&write_calls, ctx, KAPTURE_WRITE, true);
}

SEC("tracepoint/syscalls/sys_enter_connect")
int selected_connect_enter(void *ctx)
{
    if (selected_process(bpf_get_current_pid_tgid()))
        increment_stat(KAPTURE_STAT_CONNECTS, 1);
    return 0;
}

SEC("tracepoint/syscalls/sys_exit_connect")
int selected_connect_exit(struct trace_event_raw_sys_exit *ctx)
{
    if (selected_process(bpf_get_current_pid_tgid()) && BPF_CORE_READ(ctx, ret) < 0)
        increment_stat(KAPTURE_STAT_CONNECT_ERRORS, 1);
    return 0;
}

SEC("kprobe/tcp_retransmit_skb")
int selected_tcp_retransmit(struct pt_regs *ctx)
{
    if (selected_process(bpf_get_current_pid_tgid()))
        increment_stat(KAPTURE_STAT_RETRANSMITS, 1);
    return 0;
}

SEC("tracepoint/sched/sched_switch")
int selected_sched_switch(struct trace_event_raw_sched_switch *ctx)
{
    __u64 now = bpf_ktime_get_ns();
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u32 previous = (__u32)BPF_CORE_READ(ctx, prev_pid);
    __u32 next = (__u32)BPF_CORE_READ(ctx, next_pid);

    if (selected_process(pid_tgid))
        bpf_map_update_elem(&offcpu_starts, &previous, &now, BPF_ANY);

    __u64 *started = bpf_map_lookup_elem(&offcpu_starts, &next);
    if (started) {
        increment_stat(KAPTURE_STAT_OFFCPU_NS, now - *started);
        bpf_map_delete_elem(&offcpu_starts, &next);
    }
    return 0;
}
