# stand-alone custom per-server generic thread pool
# no locks and no heap allocation on the hot path
import std/atomics
import posix

proc eventfd*(initval: cuint, flags: cint): cint {.importc, header: "<sys/eventfd.h>".}

type
  TaskHandler*[T] = proc(taskdata: T) {.nimcall, gcsafe.}

  WorkerRing*[T] = object
    slots*: ptr UncheckedArray[T]
    cap*: int
    head*: Atomic[int]       ## producer side (reader thread)
    tail*: Atomic[int]       ## consumer side (the worker thread)
    wakeFd*: int             ## eventfd; worker does a blocking read on it
    waiting*: Atomic[bool]

  ThreadPool*[T] = object
    handler*: TaskHandler[T]
    rings*: seq[WorkerRing[T]]
    threads*: seq[Thread[tuple[p: ptr ThreadPool[T], i: int]]]
    next*: Atomic[int]       ## round-robin ring assignment
    stop*: Atomic[bool]

proc workerEntry(args: tuple[p: ptr ThreadPool, i: int]) {.thread, nimcall.} =
  {.gcsafe.}:
    let pool = args.p
    let i = args.i
    let ring = addr pool.rings[i]
    while true:
      let h = ring.head.load(moAcquire)
      let t = ring.tail.load(moRelaxed)
      if t == h:
        if pool.stop.load(moRelaxed): break
        ring.waiting.store(true, moRelease)
        # re-check after marking waiting, so a poke that raced us is not lost
        if ring.tail.load(moRelaxed) == ring.head.load(moAcquire):
          var n: uint64 = 0
          discard read(cint(ring.wakeFd), addr n, sizeof(n))  # blocks until poked
        ring.waiting.store(false, moRelaxed)
        continue
      try:
        # pool.handler(addr ring.slots[t mod ring.cap])
        pool.handler(ring.slots[t mod ring.cap])
      except:
        when not defined(release): echo "serverpool task fault: ", getCurrentExceptionMsg()
        else: discard
      ring.tail.store(t + 1, moRelease)

proc newPool*[T](workerCount, ringCap: int, handler: TaskHandler[T]): ptr ThreadPool[T] =
  ## ringCap should be >= the server's max in-flight requests so push never spins.
  result = createShared(ThreadPool[T])
  result.handler = handler
  result.rings = newSeq[WorkerRing[T]](workerCount)   # zero-initialized
  result.threads = newSeq[Thread[tuple[p: ptr ThreadPool[T], i: int]]](workerCount)
  for i in 0..<workerCount:
    result.rings[i].slots = cast[ptr UncheckedArray[T]](allocShared0(ringCap * sizeof(T)))
    result.rings[i].cap = ringCap
    result.rings[i].wakeFd = eventfd(0, O_CLOEXEC)  # blocking read side
  for i in 0..<workerCount:
    createThread(result.threads[i], workerEntry, (result, i))

proc push*[T](pool: ptr ThreadPool[T], task: T) =
  ## Called only from the server's reader thread (single producer per pool).
  pool.next.atomicInc()
  let i = pool.next.load(moRelaxed) mod pool.rings.len
  let ring = addr pool.rings[i]
  while true:
    let h = ring.head.load(moRelaxed)
    let t = ring.tail.load(moAcquire)
    if h - t < ring.cap:
      ring.slots[h mod ring.cap] = task
      ring.head.store(h + 1, moRelease)
      if ring.waiting.load(moAcquire):
        var one: uint64 = 1
        discard write(cint(ring.wakeFd), addr one, sizeof(one))
      return
    discard schedYield()  # ring full: only possible if ringCap < in-flight; spin briefly

proc stopAndJoin*(pool: ptr ThreadPool) =
  if pool == nil: return
  pool.stop.store(true, moRelease)
  for i in 0..<pool.rings.len:
    var one: uint64 = 1
    discard write(cint(pool.rings[i].wakeFd), addr one, sizeof(one))
  for i in 0..<pool.threads.len:
    joinThread(pool.threads[i])
