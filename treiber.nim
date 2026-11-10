# lock-free Treiber stack of int indices (used for buffer free lists).

import std/sysatomics

type
  TreiberStack* = object
    head*: uint64                      ## packed (tag shl 32) or top index; 0 = empty
    next*: ptr UncheckedArray[uint32]  ## next[i] = index below i in the stack

proc push*(s: var TreiberStack, idx: int) =
  var top = atomicLoadN(addr s.head, ATOMIC_RELAXED)
  while true:
    s.next[idx] = uint32(top and 0xFFFFFFFF'u64)   # link to old top; safe: idx not yet visible
    let newTop = (((top shr 32) + 1) shl 32) or (idx.uint64 + 1)
    var expected = top
    if atomicCompareExchangeN(addr s.head, addr expected, newTop, false, ATOMIC_SEQ_CST, ATOMIC_SEQ_CST):
      return
    top = expected

proc pop*(s: var TreiberStack): int =
  var top = atomicLoadN(addr s.head, ATOMIC_ACQUIRE)
  while (top and 0xFFFFFFFF'u64) != 0'u64:
    let idx = int(top and 0xFFFFFFFF'u64) - 1
    let newTop = (((top shr 32) + 1) shl 32) or s.next[idx].uint64
    var expected = top
    if atomicCompareExchangeN(addr s.head, addr expected, newTop, false, ATOMIC_SEQ_CST, ATOMIC_ACQUIRE):
      return idx
    top = expected
  return -1

proc isEmpty*(s: var TreiberStack): bool =
  (atomicLoadN(addr s.head, ATOMIC_RELAXED) and 0xFFFFFFFF'u64) == 0'u64

proc newTreiberStack*(capacity: int, indices: openArray[int]): TreiberStack =
  result.head = 0'u64
  result.next = cast[ptr UncheckedArray[uint32]](allocShared0(capacity * sizeof(uint32)))
  for i in 0..indices.high:
    push(result, indices[i])
