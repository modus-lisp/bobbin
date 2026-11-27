# bobbin

The org's one portable concurrency mechanism: **actors** and **channels**, with messages that
are plain data and always **copied**, on every Lisp.

On modus, threads share no state -- a thread's objects live in its own GC region, and the
runtime refuses a store that would let another thread reach one.  bobbin is that model on every
Lisp, so a library has one threading design and not a `#+modus` fork of it.  It runs on modus's
own machinery where it exists and on SBCL threads elsewhere; on SBCL messages are copied too, so a
sharing bug shows there, where it is easy to debug, instead of only on modus.

No dependencies beyond the `sb-thread` surface SBCL and modus share.

```lisp
(asdf:load-system "bobbin")

(defun echo ()
  (loop (let ((m (bobbin:receive)))
          (when (eq m :stop) (return))
          (bobbin:send (first m) (list :echo (second m))))))

(let ((a (bobbin:spawn-monitor 'echo)))
  (bobbin:send a (list (bobbin:self) "hello"))
  (bobbin:receive :timeout 5)                 ; => (:ECHO "hello"), T
  (bobbin:send a :stop)
  (bobbin:receive-if (lambda (m) (eq (first m) :exit))))   ; => (:EXIT a :NORMAL), T
```

## API

**Actors** (`src/actor.lisp`)

| | |
|---|---|
| `(spawn fn &rest args)` | start an actor running `(apply fn args)`; returns its handle.  `fn` is a symbol naming a global function, or -- only when called on the main thread -- a function object (closure).  `args` are plain data, copied. |
| `(spawn-link fn &rest args)`, `(spawn-monitor fn &rest args)` | the same, linked / monitored before the actor can end |
| `(send actor msg &key (timeout 10))` | copy `msg` to `actor`'s mailbox: T; NIL if the actor has ended (dropped); signals `mailbox-full` if it stays full for `timeout` s |
| `(receive &key timeout)` | `(values msg t)`, or `(values nil nil)` after `timeout` seconds (NIL = for ever) |
| `(receive-if predicate &key timeout)` | selective receive: the first message satisfying `predicate`; the others are kept, in order |
| `(self)` | this actor's handle (the main thread is an actor too) |
| `(actor-alive-p actor)` | |
| `(link actor)`, `(unlink actor)` | both ways: whichever ends first, the other is sent `(:exit handle reason)` |
| `(monitor actor)`, `(demonitor actor)` | one way: when `actor` ends this actor is sent `(:exit actor reason)` |
| `(stop)` | stop modus's actor runtime's scheduler threads, if bobbin started them |
| `(backend)`, `(max-actors)` | `:modus-actors` or `:threads`; how many actors may exist at once |

`reason` is `:normal` when the function returned, `(:error condition-type "message")` when it
signalled an error it did not handle, `:noproc` when `link` / `monitor` named an actor that had
already ended.  An exit is a message only: it does not end the linked actor.  A handle is an
integer, so it is plain data: put `(self)` in a message to be answered.  A handle names one actor
for ever -- once it ends its slot may be reused, but messages to the old handle are dropped, never
delivered to the new occupant.

**Channels** (`src/channel.lisp`, formerly cl-transport/chan): `make-channel (&key capacity name)`,
`channel-send (ch v &key timeout)` (T, or NIL on timeout), `channel-receive (ch &key timeout)`
(`(values v t)` / `(values nil nil)`), `channel-try-receive`, `channel-drain`, `channel-p`, type
`channel`.  Make channels once, on the main thread, and hand them out (a global, or an
`:arguments` element of `make-thread`); on modus a channel's ring is never reclaimed.

**What a message may be** (`src/plain.lisp`), the same on every backend: NIL, T, numbers,
characters, strings, symbols, lists (dotted too, not circular), simple vectors,
`(unsigned-byte 8)` vectors, hash tables.  Anything else -- a closure, a structure or CLOS instance,
a condition, a stream, a channel -- signals in the sender before anything is queued.  Values arrive
EQUAL, never EQ.  (modus's own channels and actors carry DEFSTRUCT instances; bobbin refuses them
there too, so code that runs on modus runs on SBCL.)

## Backends, and how one is chosen

Chosen when bobbin is **loaded**, from what the Lisp has -- `fboundp` of `MODUS:ACTORS-START` /
`MODUS:MAKE-CHANNEL` -- not from `#+sbcl` / `#+modus`:

| backend | where | an actor is | a mailbox is |
|---|---|---|---|
| `:modus-actors` | modus with the actor runtime (the hosted x86-64 CLI) | an actor of `MODUS:ACTORS-*`, scheduled M:N over native threads, its own GC region; a blocked `receive` gives the thread back | the runtime's (bytes copied through per-actor staging) |
| `:threads` | modus without it (AArch64 and other CLIs), and SBCL | one native thread | a bobbin channel: a `MODUS:CHANNEL` on modus, a locked queue of copies on SBCL |

`BOBBIN_BACKEND=threads` in the environment at load time forces `:threads` (to exercise that
backend on a modus that has the runtime); `BOBBIN_BACKEND=actors` insists on the runtime.

## What differs between backends

| | SBCL `:threads` | modus `:threads` | modus `:modus-actors` |
|---|---|---|---|
| actors at once | 4095 (bobbin's table; measured 4095 at once) | **15** (modus has 16 thread slots, main's included) -- fewer if the program runs other threads | **48** by default (`*actor-runtime-config*`, at most 60) |
| an actor costs | an OS thread | an OS thread and one of the 15 slots | a slot, a 2 MB stack, a GC region (8 MB semispace pair by default: an actor's live data must fit) |
| mailbox bound | `*mailbox-bytes*` (256 KB, estimated) | 256 KB ring per mailbox, all 16 made at load | 256 KB of staging per actor (modus's) |
| `send` to a full mailbox | waits `timeout` s, then `mailbox-full` | same | modus waits 10 s, whatever `timeout` says, then `mailbox-full` |
| a blocked `receive` | blocks its thread | blocks its thread | frees the scheduler thread |
| start-up | nothing | 16 mailboxes made at load | the runtime starts at the first `spawn` / `receive` on the main thread (carves 48 x 8 MB regions out of region 0); `stop` ends it, once per process |
| ping-pong, round trips/s (one run each, see `test/`) | 33-58 k | 16-21 k | 12-16 k |

`*actor-runtime-config*` is `(:threads 4 :slots 48 :region-bytes #x800000)`; set it on the main
thread before the first spawn.  Measured on the x86-64 CLI: 60 x 8 MB starts, 60 x 16 MB and 24 x
32 MB do not (region 0 has no room).  An actor that needs more live data than its region holds
(a Tor consensus is several MB) belongs on a thread of the program's own, talking to actors over
a channel.

Rules that are the same everywhere because modus needs them: from inside an actor spawn a
**symbol**, not a closure; spawn arguments and messages are copied; a closure spawned from main
still sees main's objects through its captured variables (on SBCL those are shared, on modus an
actor can read but not store into them) -- pass what an actor needs as arguments instead.

## Tests

```sh
sbcl --script test/run.lisp                                   # :threads on SBCL
modus --script test/run.lisp                                  # :modus-actors on x86-64 modus
BOBBIN_BACKEND=threads modus --script test/run.lisp           # :threads on modus
```

50 checks: channels (round trip, copies, timeouts, full, refusals, a bounded channel carrying more
than its capacity), and actors (ordering, copies under mutation, timeouts, selective receive,
refusals in main and in actors, closures from main / symbols from actors, link and monitor on a
normal end and on an error, `:noproc`, as many actors as the backend allows and their slots reused,
ping-pong throughput main<->actor and actor<->actor).  `BOBBIN_TEST_MANY` / `BOBBIN_TEST_PINGS`
change the counts.
