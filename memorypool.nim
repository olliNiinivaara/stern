import treiber

const BUFFER_SIZE* = 4096
const DEFAULT_REQUEST_BUFFERS* = 4096
const DEFAULT_RESPONSE_BUFFERS* = 5904
  # Response pool must stay >= request pool so workers can never
  # starve for a response buffer while all request buffers are in flight.

type
  BufferOverflow* = object of ValueError

  Span* = object
    # use more informative variable names...
    p*: ptr UncheckedArray[char]
    n*: int

  MemoryPool* = object
    # Per-server buffer pool so one server's load can never starve another server's buffers
    buffers*: seq[Span]
    bufferSize*: int
    reqStack*, respStack*: TreiberStack

proc putStr*(span: var Span, s: string) {.inline.} =
  doAssert(span.n + s.len <= BUFFER_SIZE)
  if s.len > 0:
    copyMem(addr span.p[span.n], unsafeAddr s[0], s.len)
    span.n += s.len

proc newBufferPool*(requestBuffers, responseBuffers: int, bufferSize = BUFFER_SIZE): ptr MemoryPool =
  ## Allocated on the shared heap; safe to use from all of the server's threads.
  result = createShared(MemoryPool)
  result.bufferSize = bufferSize
  let total = requestBuffers + responseBuffers
  result.buffers = newSeq[Span](total)
  for i in 0..<total:
    result.buffers[i].p = cast[ptr UncheckedArray[char]](allocShared(BUFFERSIZE))
  var reqIdx = newSeq[int](requestBuffers)
  for i in 0..<requestBuffers: reqIdx[i] = i
  var respIdx = newSeq[int](responseBuffers)
  for i in 0..<responseBuffers: respIdx[i] = requestBuffers + i
  result.reqStack = newTreiberStack(total, reqIdx)
  result.respStack = newTreiberStack(total, respIdx)

proc acquireRequestBuffer*(pool: ptr MemoryPool): int =
  ## Raises IOError when this server's request pool is empty — that is the read-side
  ## limiter: at most reqCount requests of this server can be in flight.
  {.gcsafe.}:
    result = pool.reqStack.pop()
    if result < 0: raise newException(IOError, "No free request buffers available")
    pool.buffers[result].n = 0

proc releaseRequestBuffer*(pool: ptr MemoryPool, idx: int) {.gcsafe.} =
  {.gcsafe.}:
    pool.reqStack.push(idx)

proc acquireResponseBuffer*(pool: ptr MemoryPool): int =
  {.gcsafe.}:
    result = pool.respStack.pop()
    if result < 0: raise newException(IOError, "No free response buffers available")
    pool.buffers[result].n = 0

proc releaseResponseBuffer*(pool: ptr MemoryPool, idx: int) {.gcsafe.} =
  {.gcsafe.}:
    pool.respStack.push(idx)

proc requestBuffersFree*(pool: ptr MemoryPool): int =
  if pool.reqStack.isEmpty(): 0 else: 1
