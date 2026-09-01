#ifndef KAPTURE_TLS_H
#define KAPTURE_TLS_H

#define KAPTURE_CHUNK_MAX (16 * 1024)
#define KAPTURE_CALL_MAX (1024 * 1024)
#define KAPTURE_MAX_CHUNKS (KAPTURE_CALL_MAX / KAPTURE_CHUNK_MAX)

enum kapture_direction {
    KAPTURE_WRITE = 0,
    KAPTURE_READ = 1,
};

enum kapture_stat {
    KAPTURE_STAT_EVENTS = 0,
    KAPTURE_STAT_RING_DROPS = 1,
    KAPTURE_STAT_READ_FAULTS = 2,
    KAPTURE_STAT_CONNECTS = 3,
    KAPTURE_STAT_CONNECT_ERRORS = 4,
    KAPTURE_STAT_RETRANSMITS = 5,
    KAPTURE_STAT_OFFCPU_NS = 6,
    KAPTURE_STAT_OVERSIZE_CALLS = 7,
    KAPTURE_STAT_COUNT = 8,
};

struct kapture_event {
    unsigned int tgid;
    unsigned int tid;
    unsigned int connection_id;
    unsigned char direction;
    unsigned char reserved[3];
    unsigned long long sequence;
    unsigned long long observed_nanos;
    unsigned int length;
    unsigned char data[];
};

#endif
