# proto_hook_test

Standalone C sanity check for the Kapture proto hook patch on librdkafka.

Boot the dev stack (`pnpm stack:up` + `pnpm seed`) before running.

## Build

```bash
cd vendor/librdkafka
mkdir -p build && cd build
cmake .. -DRDKAFKA_BUILD_STATIC=ON \
         -DRDKAFKA_BUILD_TESTS=OFF \
         -DRDKAFKA_BUILD_EXAMPLES=OFF \
         -DENABLE_LZ4_EXT=OFF \
         -DWITH_CURL=OFF
cmake --build . --target rdkafka -j

cd ../../librdkafka-test
cc -I../librdkafka/src \
   -L../librdkafka/build/src \
   -L/opt/homebrew/opt/openssl@3/lib \
   -L/opt/homebrew/lib \
   proto_hook_test.c -o proto_hook_test \
   -lrdkafka -lpthread -lz -lssl -lcrypto -lzstd -lsasl2 -lc++
```

## Run

```bash
./proto_hook_test localhost:19092 orders.raw
```

The test installs the proto hook, polls for 5 messages, and prints
every SEND / RECV protocol frame with `ApiKey`, `ApiVersion`,
`CorrId`, broker id, payload size, and (for RECV) RTT. Exits 0 if at
least one SEND and one RECV are observed.

Expected output (truncated):

```
→ SEND api=ApiVersions      v3 corr_id=0x00000001 broker=-1 size=33
← RECV api=ApiVersions      v3 corr_id=0x00000001 broker=-1 size=...
→ SEND api=Metadata         v8 corr_id=0x00000002 ...
...
→ SEND api=Fetch            v11 corr_id=0x0000000c broker=0 size=200
← RECV api=Fetch            v11 corr_id=0x0000000c broker=0 size=9005 rtt=1.1ms
  msg orders.raw/6@0
  ...
=== summary ===
SEND frames observed: 25
RECV frames observed: 25
messages consumed:    5
```

CorrId on SEND matches the CorrId on the RECV that completes the
exchange — that's the correlation key Kapture uses to attach
protocol metadata to each delivered message.
