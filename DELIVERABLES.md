# Lab 3 — Kafka for Data Streaming — Deliverables

Topic used: `recitation-c`
Notebook: [`KafkaDemo.ipynb`](./KafkaDemo.ipynb)

---

## Deliverable 1 — SSH tunnel, topics, and offsets

### Establishing the tunnel

The Kafka broker listens on port 9092 on `cs544-f26.cs.uic.edu` and is not
exposed to the public internet. SSH port forwarding gives us a local door to it:

```bash
ssh -o ServerAliveInterval=60 -L 9092:localhost:9092 <NetID>@cs544-f26.cs.uic.edu -NTf
```

| flag | meaning |
| --- | --- |
| `-L 9092:localhost:9092` | forward **local** port 9092 to `localhost:9092` *as resolved on the server* |
| `-N` | do not execute a remote command — forwarding only |
| `-T` | do not allocate a pseudo-terminal |
| `-f` | go to background after authenticating |
| `-o ServerAliveInterval=60` | send a keepalive every 60s so an idle tunnel is not dropped |

Because of `-L`, every client on this machine — the notebook and `kcat` alike —
can just talk to `localhost:9092` and be transparently carried to the broker.

Verify the tunnel is up by asking the broker for its metadata:

```bash
kcat -b localhost:9092 -L
```

Tear it down when finished:

```bash
lsof -ti:9092 | xargs kill -9
```

### Topics

A **topic** is the named, append-only log that Kafka organises messages into —
here `recitation-c`. Producers append to the end of it; consumers read forward
through it. A topic is divided into one or more **partitions**, and the
partition is the unit that actually preserves ordering: records are appended to
the end of a partition and are never modified in place.

Crucially, **reading does not consume**. A message stays in the log for the
configured retention period regardless of how many consumers have read it, which
is what makes replay and multiple independent consumers possible.

### Offsets

Within a partition, every message is assigned a monotonically increasing integer
id — its **offset** (0, 1, 2, …). The offset is the permanent address of that
message in that partition. A consumer's entire position therefore collapses to a
single number per partition: *"I have read up to offset N."*

### How offsets maintain continuity across a disconnect

A consumer that declares a `group_id` periodically **commits** its position back
to the cluster. In our notebook:

```python
group_id=f'{topic}-demo-group',
enable_auto_commit=True,
auto_commit_interval_ms=1000,
```

Kafka records the committed offset per **(consumer group, topic, partition)** in
the internal `__consumer_offsets` topic — stored on the broker, not in the
client process. So the position survives the client.

If the consumer crashes, the SSH tunnel drops, or the notebook kernel is
restarted:

1. Producers keep appending; those messages sit safely in the retained log.
2. On reconnect with the **same `group_id`**, Kafka returns the group's last
   committed offset.
3. The consumer resumes from exactly that point — no messages skipped, and none
   already processed re-read.

`auto_offset_reset` is only consulted when the group has **no** committed offset
(a brand-new group, or one whose offsets aged out):

- `earliest` — begin at the lowest retained offset and replay the whole log
- `latest` — begin at the end, seeing only messages produced from now on

Because commits happen on a timer rather than per message, the delivery
guarantee is **at-least-once**: if the consumer dies after processing a message
but before the next commit lands, that message is redelivered on restart.

**Live demonstration:** run the consumer, interrupt it mid-stream, run the
producer again, then re-run the consumer — it picks up only the messages added
since. Changing the `group_id` suffix resets the group and replays the topic
from the earliest offset.

---

## Deliverable 2 — Producer and consumer modes

Implemented in [`KafkaDemo.ipynb`](./KafkaDemo.ipynb).

### Producer

```python
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

cities = ['Chicago', 'Pittsburgh', 'New York']
```

- `bootstrap_servers=['localhost:9092']` — the tunnelled broker. The client uses
  this only to *bootstrap*: it fetches cluster metadata, then talks to whichever
  broker leads the target partition.
- `value_serializer` — Kafka stores raw bytes, so each value is turned into JSON
  text by `dumps()` and then into UTF-8 bytes by `.encode()`.
- `producer.send(topic=topic, value=data)` appends the record and returns
  immediately; it is asynchronous and batched.
- `producer.flush()` blocks until the buffer is actually delivered — without it
  the cell can finish before the last records leave the process.

Each record looks like `2026-09-25 14:03:11,Chicago,24ºC`.

### Consumer

```python
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id=f'{topic}-demo-group',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,
    consumer_timeout_ms=10000,
)
```

- first positional argument — the topic(s) to subscribe to
- `auto_offset_reset='earliest'` — on a fresh group, replay from the start
- `group_id` — required for offsets to be committed and resumed (Deliverable 1)
- `consumer_timeout_ms=10000` — stop blocking after 10s of silence so the cell
  terminates and `kafka_log.csv` can be inspected; remove it to stream forever

Consumed messages are printed with their partition and offset and appended to
`kafka_log.csv`.

#### One fix to the starter code's logging line

The starter wrote the log with:

```python
message = message.value.decode()
print(loads(message))
os.system(f"echo {message} >> kafka_log.csv")
```

`message` at that point is the raw **JSON text** produced by
`dumps(...)`, not the decoded value. Because `json.dumps()` defaults to
`ensure_ascii=True`, the degree sign is escaped during serialization, so that
line writes the literal six characters `\u00ba` into the CSV:

```
2026-09-25 00:20:56,Chicago,25\u00baC      <- what the starter line produces
2026-09-25 00:20:56,Chicago,25ºC          <- what we actually want
```

The `print(loads(message))` on the line above renders correctly, which is what
makes the bug easy to miss. The notebook decodes first and writes from Python:

```python
value = loads(message)
with open('kafka_log.csv', 'a', encoding='utf-8') as f:
    f.write(value + '\n')
```

This keeps the real `º` character and also avoids handing an unquoted,
space-containing string to the shell.

---

## Deliverable 3 — kcat from the earliest offset

```bash
kcat -b localhost:9092 -t recitation-c -C -o beginning -e
```

| flag | meaning |
| --- | --- |
| `-b localhost:9092` | broker to connect to — the local end of the SSH tunnel |
| `-t recitation-c` | the topic to read |
| `-C` | **consumer** mode (`-P` would be producer mode) |
| `-o beginning` | start from the **earliest** offset in the partition, replaying the entire retained log rather than only new messages |
| `-e` | exit once the last message is reached; omit it to tail the topic and block for new messages |

Note on wording: `kcat` spells the earliest position **`beginning`**, while the
Kafka *client config* property in the notebook spells the same idea `earliest`
(`auto_offset_reset='earliest'`). They are different vocabularies for the same
intent — do not mix them up.

`kcat -o earliest` does **not** error, which makes the mistake easy to miss:
`earliest` is not one of kcat's keywords (`beginning | end | stored | <value> |
-<value> | s@<ms> | e@<ms>`), so it falls through to the numeric parse and
becomes the *absolute offset 0*. On a fresh topic that looks identical to
`beginning`, but it is not the same thing: once retention has trimmed the head
of the log the lowest valid offset is greater than 0, so offset 0 is out of
range.

**This is not hypothetical on our topic.** `recitation-c` currently has a low
watermark of 20 (offsets 0-19 have already aged out), so the two spellings give
opposite results:

```console
$ kcat -b localhost:9092 -t recitation-c -C -o beginning -e -q
"2026-09-25 11:38:58,Pittsburgh,28\u00baC"
... all 10 messages ...

$ kcat -b localhost:9092 -t recitation-c -C -o earliest -e
%4|...|OFFSET|rdkafka#consumer-1| [thrd:main]: recitation-c [0]: offset reset
(at offset 0 ...) to offset END ...: fetch failed due to requested offset not
available on the broker: Broker: Offset out of range
% Reached end of topic recitation-c [0] at offset 30: exiting
                                         <-- zero messages returned
```

`-o earliest` returns **nothing** and silently resets to the end. Use
`beginning`.

Useful companions:

```bash
kcat -b localhost:9092 -L                      # list topics/partitions (metadata)
kcat -b localhost:9092 -t recitation-c -C -o -5  # last 5 messages
```

---

## Verified against the live broker

Run on 2026-09-25 through the SSH tunnel (`lrach@cs544-f26.cs.uic.edu`), topic
`recitation-c`, partition 0:

- **Producer** wrote 10 records, landing at offsets **20–29**.
- **Consumer** read all 10 back, printing partition and offset for each, and
  wrote them to `kafka_log.csv` with the real `º` character and no `º`
  escapes — the logging fix holds end to end.
- **kcat** with `-o beginning` replayed all 10 from the earliest *retained*
  offset.

Note what kcat prints versus what the consumer prints:

```
kcat     -> "2026-09-25 11:38:58,Pittsburgh,28ºC"    (raw stored bytes)
consumer -> 2026-09-25 11:38:58,Pittsburgh,28ºC           (after loads())
```

kcat shows the bytes Kafka actually stores, so the JSON quoting and the
`ensure_ascii` escape are both plainly visible there — the same escape the
starter's `echo` line would have written straight into the CSV.
