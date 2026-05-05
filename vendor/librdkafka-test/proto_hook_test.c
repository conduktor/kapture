// Sanity check for the Kapture proto hook patch on librdkafka.
//
// Spawns a consumer against localhost:19092, subscribes to a seeded topic,
// installs the proto hook, polls for a few messages, and prints every
// SEND/RECV frame. Exits 0 if at least one SEND and one RECV are observed.
//
// Build (after librdkafka.a is built in vendor/librdkafka/build):
//   gcc -I../librdkafka/src \
//       -L../librdkafka/build/src \
//       proto_hook_test.c -o proto_hook_test \
//       -lrdkafka -lpthread -lz -lssl -lcrypto -lzstd -lc++

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <inttypes.h>
#include <unistd.h>

#include "rdkafka.h"

static int send_count = 0;
static int recv_count = 0;
static volatile int run = 1;

static const char *api_name(int api_key) {
        switch (api_key) {
        case 0:
                return "Produce";
        case 1:
                return "Fetch";
        case 2:
                return "ListOffsets";
        case 3:
                return "Metadata";
        case 8:
                return "OffsetCommit";
        case 9:
                return "OffsetFetch";
        case 10:
                return "FindCoordinator";
        case 11:
                return "JoinGroup";
        case 12:
                return "Heartbeat";
        case 13:
                return "LeaveGroup";
        case 14:
                return "SyncGroup";
        case 15:
                return "DescribeGroups";
        case 16:
                return "ListGroups";
        case 17:
                return "SaslHandshake";
        case 18:
                return "ApiVersions";
        case 19:
                return "CreateTopics";
        case 22:
                return "InitProducerId";
        case 32:
                return "DescribeConfigs";
        case 36:
                return "SaslAuthenticate";
        case 60:
                return "DescribeCluster";
        default:
                return "Unknown";
        }
}

static void
proto_hook(rd_kafka_t *rk, int dir, int api_key, int api_version, int32_t corr_id,
           int32_t broker_id, size_t payload_size, double rtt_ms, void *opaque) {
        const char *dir_label = dir == RD_KAFKA_PROTO_DIR_SEND ? "→ SEND" : "← RECV";
        if (dir == RD_KAFKA_PROTO_DIR_SEND) {
                send_count++;
                printf("%s api=%-16s v%d corr_id=0x%08" PRIx32
                       " broker=%" PRId32 " size=%zu\n",
                       dir_label, api_name(api_key), api_version, corr_id,
                       broker_id, payload_size);
        } else {
                recv_count++;
                printf("%s api=%-16s v%d corr_id=0x%08" PRIx32
                       " broker=%" PRId32 " size=%zu rtt=%.1fms\n",
                       dir_label, api_name(api_key), api_version, corr_id,
                       broker_id, payload_size, rtt_ms);
        }
}

static void on_signal(int sig) {
        (void)sig;
        run = 0;
}

int main(int argc, char **argv) {
        signal(SIGINT, on_signal);
        signal(SIGTERM, on_signal);

        const char *brokers = argc > 1 ? argv[1] : "localhost:19092";
        const char *topic   = argc > 2 ? argv[2] : "orders.raw";

        char errstr[512];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_conf_set_proto_hook_cb(conf, proto_hook, NULL);
        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "conf bootstrap.servers: %s\n", errstr);
                return 1;
        }
        char group_id[64];
        snprintf(group_id, sizeof(group_id), "kapture-proto-hook-test-%d",
                 (int)getpid());
        if (rd_kafka_conf_set(conf, "group.id", group_id, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "conf group.id: %s\n", errstr);
                return 1;
        }
        if (rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "conf offset.reset: %s\n", errstr);
                return 1;
        }
        if (rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "conf auto.commit: %s\n", errstr);
                return 1;
        }
        if (rd_kafka_conf_set(conf, "client.id", "kapture-proto-hook-test",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "conf client.id: %s\n", errstr);
                return 1;
        }

        rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr,
                                      sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "rd_kafka_new: %s\n", errstr);
                return 1;
        }
        rd_kafka_poll_set_consumer(rk);

        rd_kafka_topic_partition_list_t *topics =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, topics);
        if (err) {
                fprintf(stderr, "subscribe: %s\n", rd_kafka_err2str(err));
                rd_kafka_topic_partition_list_destroy(topics);
                rd_kafka_destroy(rk);
                return 1;
        }

        printf("connected to %s, subscribed to %s, polling for 5 messages...\n",
               brokers, topic);

        int messages = 0;
        for (int i = 0; i < 200 && run && messages < 5; i++) {
                rd_kafka_message_t *msg = rd_kafka_consumer_poll(rk, 100);
                if (msg) {
                        if (msg->err == 0) {
                                printf("  msg %s/%" PRId32 "@%" PRId64 "\n",
                                       rd_kafka_topic_name(msg->rkt),
                                       msg->partition, msg->offset);
                                messages++;
                        }
                        rd_kafka_message_destroy(msg);
                }
        }

        rd_kafka_topic_partition_list_destroy(topics);
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        printf("\n=== summary ===\n");
        printf("SEND frames observed: %d\n", send_count);
        printf("RECV frames observed: %d\n", recv_count);
        printf("messages consumed:    %d\n", messages);

        if (send_count == 0 || recv_count == 0) {
                fprintf(stderr,
                        "FAIL: hook did not fire (need both SEND and RECV)\n");
                return 2;
        }
        return 0;
}
