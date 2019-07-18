import
  system/ansi_c, bplustree, intsets, random, sequtils, strformat, times

type
  BlkInfo[Bn] = object
    useCount: int
    name: Bn
    modified: bool
    data: float64   # For alignment

  TestBlockMgr[Bn] = ref object
    ## Block manager used to test for bad scoping of load/finished calls.
    blocks: seq[ptr BlkInfo[Bn]] # Memory block with header.
    saved: seq[pointer] # pointer to saved data for blocks that have been "unloaded" when not being used.

proc newTestBlockMgr*[Bn](blockSize: Natural) : BlockMgr[Bn] = 
  ## This is an example block manager that hands out integer handles for blocks of
  ## memory.
  let bm = TestBlockMgr[Bn]()
  let rv = BlockMgr[Bn](blockSize: blockSize, zerosMem: true)
  const checkIllegalWrites = true # Can disable to cut down block writes when using memory watchpoints in GDB to track down bugs.

  # Add a dummy entry, so we always return block numbers > 0
  setLen(bm.blocks, 1)
  setLen(bm.saved, 1)

  rv.destroy = proc () = 
    for i in 1..<len(bm.blocks):
      if bm.blocks[i] != nil:
        dealloc(bm.blocks[i])
        dealloc(bm.saved[i])

  rv.alloc = proc () : Bn = 
    result = Bn(len(bm.blocks))
    let bp = cast[ptr BlkInfo[Bn]](alloc0(offsetof(BlkInfo[Bn], data) + blockSize))
    let backup = alloc0(blockSize)
    add(bm.blocks, bp)
    add(bm.saved, backup)
    bp.name = result

    if checkIllegalWrites:
      # Fill the unused "in memory" block with 0xFF so we can detect when it has
      # been modified outside of the scope of a load/finished.
      c_memset(addr(bp.data), cint(0xFF), cint(blockSize))

  rv.free = proc (blk: Bn) = 
    # Freeing just frees the resulting blocks.  They are not re-used, 
    # so we can detect references to dead blocks.
    let bi = int(blk)
    let bp = bm.blocks[bi]
    assert bp != nil, &"Double free of {blk}"
    dealloc(bp)
    dealloc(bm.saved[bi])
    bm.blocks[bi] = nil
    bm.saved[bi] = nil

  rv.load = proc (blk: Bn) : pointer = 
    let p = bm.blocks[int(blk)] 

    assert p != nil, &"Use of freed block {blk}"
    if p.useCount == 0:
      # Test inactive in-memory block to see if it has been modified outside of the scope of
      # load/finish calls.
      if checkIllegalWrites:
        for i in 0..<blockSize:
          let byp = cast[ptr byte](cast[int](addr(p.data)) + i)
          assert byp[] == byte(0xFF), &"Block {blk} has been modified at offset {i} outside of a load/finish call pair."

        # "load" from saved blocks.
        copyMem(addr(p.data), bm.saved[int(blk)], blockSize)

    inc(p.useCount)
    result = addr(p.data)

  rv.modified = proc (blk: Bn) = 
    let bp = bm.blocks[int(blk)]

    if bp != nil: # Could be nil if the block was freed.
      bp.modified = true

  rv.finished = proc (blk: Bn) = 
    let p = bm.blocks[int(blk)]
    
    # Given the way finished is handled by destructors, we do expect that finished can be 
    # called on a freed block.
    if p != nil:
      assert p.useCount > 0, "Finish before load call?"
      
      dec(p.useCount)
      if p.useCount == 0:
        # No more references to this block so "unload" it and 
        if checkIllegalWrites:
          # fill the block with garbage that will make the caller
          # fall over if it's still referencing the pointer.
          let saved = bm.saved[int(blk)]
          if not p.modified:
            assert equalMem(addr(p.data), saved, blockSize), &"Modified block {blk} without modify call."
          else:
            copyMem(saved, addr(p.data), blockSize)

          # For the tree, all 0xFF will foul things faster than zeroing the memory.
          c_memset(addr(p.data), cint(0xFF), cint(blockSize))
        p.modified = false

  return rv

proc populate[Bn,int32,float32](tr: var BPlusTree[Bn,int32,float32]; count: Natural; hi: int32) : IntSet = 
  ## Fills a tree with `count` random keys.  Returns seq of all keys added.
  result = initIntSet()
  var na = 0

  while na < count:
    let k = int32(rand(int(hi)))
    if k notin result:
      incl(result, k)
      add(tr, k, float32(k) * 0.7f)
      na += 1

proc badInsert() = 
  ## Sequence that triggered a bug with leaf inserts when they split.
  var keys = @[int32(551579599), 336666328, 2000148485, 1113308261, 2026589088,
               553166780, 1281276772, 1395580239, 1817045090, 65757242,
               545445668, 284948476, 1167646666, 1167356782, 1978683550,
               1216476975, 92647912, 389394498, 972002964, 511853933, 1548946595,
               925924150, 1871467971, 692187584, 718417267, 1413146997, 294512493]
  let bm = newTestBlockMgr[uint16](128)
  var tr = initBPlusTree[uint16,int32,float32](bm)

  try:
    for k in keys:
      add(tr, k, 30.0f)

    assert int32(1113308261) in tr
  finally:
    bm.destroy()

proc testIteration[Bn,K,V](tr: var BPlusTree[Bn,K,V]; keys: IntSet) = 
  ## Make sure forward and backward iteration of the values works.
  var numFound = 0
  var iter = leastValue(tr)

  while (let (ok, k) = key(tr, iter); ok):
    inc(numFound)
    assert int(k) in keys
    if not moveNext(tr, iter):
      break

  assert numFound == len(tr)
  numFound = 0

  while (let (ok, k) = key(tr, iter); ok):
    inc(numFound)
    assert int(k) in keys
    if not movePrev(tr, iter):
      break

  assert numFound == len(tr), &"{numFound} vs {len(tr)}"
  finished(tr, iter)

proc testCore(numItems: int; blkSize: int) = 
  let bm = newTestBlockMgr[uint16](blkSize)
  var tr = initBPlusTree[uint16, int32, float32](bm)

  try:
    var keys = populate(tr, numItems, high(int32))

    echo &"testCore {numItems} start = {tr}"
    testIteration(tr, keys)
    graphviz(tr, "full.dot", false)
    for k in keys:
      assert int32(k) in tr, &"{k} notin tree"

      # Exercise the iterators returned by valuePosition.
      let iter = valuePosition(tr, int32(k))
      let (ok1, k1) = key(tr, iter)
      assert ok1 and k1 == int32(k), &"wrong iteration position for {k}, vs {k1}"
      if moveNext(tr, iter):
        let (ok2, k2) = key(tr, iter)
        assert ok2 and k2 > k1, "moveNext from {k} not >, k2={k2}"
      finished(tr, iter)

    var kseq = toSeq(items(keys))
    while len(kseq) > 0:
      let i = rand(len(kseq) - 1)
      let k = int32(kseq[i])
      #echo &"deleting {k}"
      del(kseq, i)
      del(tr, k)

      #echo "check removed"
      assert k notin tr, &"{k} not deleted"

      for k in kseq:
        #echo &"checking {k}"
        assert int32(k) in tr

    graphviz(tr, "test.dot", false)
    echo "testCore finish = " & $tr
  except:
    graphviz(tr, "problem.dot", false)
    raise
  finally:
    bm.destroy()

proc ensureKeys[Bn,K,V](tr: var BPlusTree[Bn,K,V]; known: seq[K]; contextKey: K; ctxName: string = "operation") = 
  for k in known:
    if k notin tr:
      graphvizPath(tr, "ensure.dot", k)
      assert false, &"{k} no longer in tree after {ctxName} on {contextKey}"

proc adjustSize[Bn,V](tr: var BPlusTree[Bn,int,V]; known: var seq[int]; targetSize: Natural) = 
  while len(tr) != targetSize:
    if len(tr) > targetSize:
      let pick = rand(len(known)-1)
      let key = known[pick]

      del(known, pick)
      del(tr, key)
      if key in tr:
        graphvizPath(tr, "adjust.dot", key)
        assert false, "{key} still in tree after del"

      ensureKeys(tr, known, key, "del")

    else:
      var nk: int
      while true:
        nk = rand(int(high(int32)))
        if nk notin tr:
          break

      add(tr, nk, 0.0f)
      add(known, nk)

      if nk notin tr:
        graphvizPath(tr, "adjust.dot", nk)
        assert false, &"{nk} notin tree after adding it"

      ensureKeys(tr, known, nk, "adding")


proc ramps(setpoints: openarray[int]; blkSize: Positive) = 
  ## For each value in setpoints, adds or subtracts random items from the key.
  ## Good for finding bugs that can happen after deleting a bunch of items, and 
  ## then adding more.
  let bm = newTestBlockMgr[uint16](blkSize)
  var tr = initBPlusTree[uint16, int, float32](bm)
  var known: seq[int]

  echo &"ramps {tr} setpoints={setpoints}, blksize={blkSize}"
  try:
    for numItems in setpoints:
      echo "\tadjusting to " & $numItems
      adjustSize(tr, known, numItems)
    graphviz(tr, "ramp.dot", false)

  except:
    graphviz(tr, "problem.dot", false)
    raise

  finally:
    bm.destroy()
  
  echo &"\tdone {tr}"
    

# One implementation of duplicates for a integer key.
# Maintains a separate field in the key to keep the keys
# unique for the B+ tree.
type
  UKey[K] = object
    key: K
    unid: int32  

proc `<=`[K](a, b: UKey[K]) : bool = 
  if a.key < b.key:
    true
  elif a.key == b.key:
    a.unid <= b.unid
  else:
    false

proc addDup[Bn,K,V](tr: var BPlusTree[Bn,UKey[K],V]; key: K, val: V) = 
  # There may already be a key == `key` in the tree, so 
  # we look for `key+1` in the tree.  This allows us to look at
  # previous value, and quickly get the largest `unid` for key `key`
  # so we can use that + 1 to make this insertion unique.
  let iter = valuePosition(tr, UKey[K](key: key+1, unid: 0))

  if movePrev(tr, iter):
    let (ok, prevKey) = key(tr, iter)

    if ok and prevKey.key == key:
      # This is a duplicate, so make it unique by adding 1 to unid
      finished(tr, iter)
      add(tr, UKey[K](key: key, unid: prevKey.unid + 1), val)
    else:
      # No duplicate, add normally.
      finished(tr, iter)
      add(tr, UKey[K](key: key, unid: 0), val)
  else:
    # Nothing before us, so no duplicate.
    finished(tr, iter)
    add(tr, UKey[K](key: key, unid: 0), val)

proc delDup[Bn,K,V](tr: var BPlusTree[Bn,UKey[K],V]; key: K) = 
  ## With duplicates, we always search for the key with the lowest unid and 
  ## whack that one.
  let iter = valuePosition(tr, UKey[K](key: key, unid: 0))
  let (ok, k) = key(tr, iter)

  finished(tr, iter)
  if k.key == key:
    del(tr, k)


proc testDupImpl(blkSize: int) = 
  let bm = newTestBlockMgr[uint16](blkSize)
  var tr = initBPlusTree[uint16, UKey[int], float32](bm)
  const maxVal = 100
  var occurs = newSeq[int](maxVal+1)

  echo &"Testing dup impl blkSize={blkSize}"
  for i in 0..3000:
    let rv = rand(maxVal)
    inc(occurs[rv])
    addDup(tr, rv, 12.0)

  var p = 0

  while p <= maxVal:
    while occurs[p] > 0:
      dec(occurs[p])
      delDup(tr, p)
    inc(p)

  assert len(tr) == 0

proc test*() = 
  echo "Testy"

  # Using small block size to keep branching down to managable
  # levels when creating different tree depths.
  const blkSize = 128
  badInsert()
  testCore(100, blkSize) # depth = 2
  testDupImpl(blkSize*2)
  testCore(230, blkSize) # depth = 3
  testCore(230*15, blkSize) # depth = 4
  ramps([300, 200, 400, 100], blkSize)
  ramps([1000, 0, 500], blkSize)

  var pts: seq[int]
  for i in 0..100:
    add(pts, random(500))
  ramps(pts, blkSize)

