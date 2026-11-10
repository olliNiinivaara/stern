import std/[posix]
import memorypool

proc memchr*(s: pointer, c: cint, n: csize_t): pointer {.importc, header: "<string.h>".} ## libc memchr — SIMD

const
  HEADER_TERMINATOR* = "\r\n\r\n"
  HEADER_TERMINATOR_LEN* = 4
  ContentLengthPattern* = "content-length: "
  ContentLengthPatternLen* = ContentLengthPattern.len
  ContentLengthBadCharShift* = block:
    var result: array[high(char), int]
    for c in low(char)..high(char): result[c] = ContentLengthPatternLen
    for i in 0..<ContentLengthPatternLen - 1:
      let c = ContentLengthPattern[i]
      result[c] = ContentLengthPatternLen - 1 - i
      if c == 'c': result['C'] = ContentLengthPatternLen - 1 - i
      elif c == 'l': result['L'] = ContentLengthPatternLen - 1 - i
    result

template buffer(): untyped =
  pool.buffers[idx].p

proc findFirstLineEnd*(pool: ptr MemoryPool, idx: int, currentLen: int): int =
  ## memchr('\n') jumps in SIMD chunks to the next LF, then verify the preceding CR
  if currentLen < 2: return -1
  let b = pool.buffers[idx].p
  let base = cast[uint](addr b[0])
  var pos = 0
  while pos < currentLen:
    let p = memchr(cast[pointer](base + pos.uint), '\n'.cint, (currentLen - pos).csize_t)
    if p == nil: return -1
    let j = int(cast[uint](p) - base)          # position of '\n'
    if j > 0 and b[j - 1] == '\r': return j + 1  # position after "\r\n"
    pos = j + 1
  return -1

proc findHeaderEnd*(pool: ptr MemoryPool, idx: int, currentStart, currentLen: int): int =
  ## memchr('\n') to the last LF of a candidate "\r\n\r\n", verify the 3 bytes before it
  if currentLen < HEADER_TERMINATOR_LEN: return -1
  let searchStart = max(0, currentStart - HEADER_TERMINATOR_LEN - 1)
  let b = pool.buffers[idx].p
  let base = cast[uint](addr b[0])
  var pos = searchStart
  while pos < currentLen:
    let p = memchr(cast[pointer](base + pos.uint), '\n'.cint, (currentLen - pos).csize_t)
    if p == nil: return -1
    let j = int(cast[uint](p) - base)
    if j >= max(3, searchStart + 3) and b[j-1] == '\r' and b[j-2] == '\n' and b[j-3] == '\r':
      return j + 1                             # position after "\r\n\r\n"
    pos = j + 1
  return -1

proc findContentLengthInBuffer*(pool: ptr MemoryPool, idx: int, headerEnd: int): int =
  ## Boyer-Moore-Horspool search for Content-Length header
  ## Returns start index or -1 if not found
  var shift = 0
  let maxShift = headerEnd - ContentLengthPatternLen
  while shift <= maxShift:
    var j = ContentLengthPatternLen - 1
    while j >= 0:
      let tChar = buffer[shift + j]
      let pChar = ContentLengthPattern[j]
      if pChar == tChar:
        dec j
      elif pChar == 'c' and tChar == 'C':
        dec j
      elif pChar == 'l' and tChar == 'L':
        dec j
      else:
        break
    if j < 0:
      return shift
    let lastCharInWindow = buffer[shift + ContentLengthPatternLen - 1]
    shift += ContentLengthBadCharShift[lastCharInWindow]
  return -1

proc parseRequestLine*(pool: ptr MemoryPool, idx: int, length: int): tuple[mStart, mLen, pStart, pLen: int] =
  ## Parse the request line into SPANS into the request buffer — zero allocation.
  ## Format: METHOD /path HTTP/1.1\r\n ; zero spans if unparseable.
  result = (0, 0, 0, 0)
  if length < 3: return
  var methodEnd = 0
  while methodEnd < length and buffer[methodEnd] != ' ' and buffer[methodEnd] != '\r' and buffer[methodEnd] != '\n':
    inc(methodEnd)
  if methodEnd == 0: return
  result.mLen = methodEnd
  var pathStart = methodEnd
  while pathStart < length and buffer[pathStart] == ' ': inc(pathStart)
  if pathStart >= length: return
  var pathEnd = pathStart
  while pathEnd < length and buffer[pathEnd] != ' ' and buffer[pathEnd] != '\r' and buffer[pathEnd] != '\n':
    inc(pathEnd)
  result.pStart = pathStart
  result.pLen = pathEnd - pathStart

proc parseContentLength*(pool: ptr MemoryPool, idx: int, headerEnd: int): int =
  ## Parse Content-Length header from buffer up to headerEnd
  ## Returns the content length, or 0 if not found
  if headerEnd < ContentLengthPatternLen:
    return 0
  
  let start = findContentLengthInBuffer(pool, idx, headerEnd)
  if start == -1:
    return 0
  
  # Found Content-Length header, parse the number
  let valueStart = start + ContentLengthPatternLen
  var i = valueStart
  while i < headerEnd and buffer[i] == ' ':
    inc(i)
  var numStart = i
  while i < headerEnd and buffer[i] in {'0'..'9'}:
    inc(i)
  
  if numStart < i:
    # parse digits in place — no string alloc; cap to avoid overflow (caller rejects oversize)
    var v = 0
    for j in numStart..<i:
      v = v * 10 + (ord(buffer[j]) - ord('0'))
      if v > 1_000_000_000: return 1_000_000_000
    return v
  return 0

proc methodEq(b: ptr UncheckedArray[char], start, len: int, lit: string): bool {.inline.} =
  len == lit.len and equalMem(b, unsafeAddr lit[0], len)

proc hasRequestBody*(b: ptr UncheckedArray[char], mStart, mLen: int): bool =
  ## Methods are assumed UPPERCASE (RFC 9110 convention)
  ## GET and HEAD do not have bodies (RFC 7231)
  not (methodEq(b, mStart, mLen, "GET") or methodEq(b, mStart, mLen, "HEAD") or
       methodEq(b, mStart, mLen, "DELETE") or methodEq(b, mStart, mLen, "OPTIONS") or
       methodEq(b, mStart, mLen, "TRACE") or methodEq(b, mStart, mLen, "CONNECT"))