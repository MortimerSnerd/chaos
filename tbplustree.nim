import
  system/ansi_c, bplustree, intsets, random, sequtils, strformat

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

  # Add a dummy entry, so we always return block numbers > 0
  setLen(bm.blocks, 1)
  setLen(bm.saved, 1)

  rv.destroy = proc () = 
    for i in 1..<len(bm.blocks):
      dealloc(bm.blocks[i])
      dealloc(bm.saved[i])

  rv.alloc = proc () : Bn = 
    result = Bn(len(bm.blocks))
    let bp = cast[ptr BlkInfo[Bn]](alloc0(offsetof(BlkInfo[Bn], data) + blockSize))
    let backup = alloc0(blockSize)
    add(bm.blocks, bp)
    add(bm.saved, backup)
    bp.name = result

    # Fill the unused "in memory" block with 0xFF so we can detect when it has
    # been modified outside of the scope of a load/finished.
    c_memset(addr(bp.data), cint(0xFF), cint(blockSize))

  rv.free = proc (blk: Bn) = 
    assert blk == Bn(0), "Not handling free"

  rv.load = proc (blk: Bn) : pointer = 
    let p = bm.blocks[int(blk)]
    if p.useCount == 0:
      # Test inactive in-memory block to see if it has been modified outside of the scope of
      # load/finish calls.
      for i in 0..<blockSize:
        let byp = cast[ptr byte](cast[int](addr(p.data)) + i)
        assert byp[] == byte(0xFF), &"Block {blk} has been modified at offset {i} outside of a load/finish call pair."

      # "load" from saved blocks.
      copyMem(addr(p.data), bm.saved[int(blk)], blockSize)

    inc(p.useCount)
    result = addr(p.data)

  rv.modified = proc (blk: Bn) = 
    bm.blocks[int(blk)].modified = true

  rv.finished = proc (blk: Bn) = 
    let p = bm.blocks[int(blk)]
    assert p.useCount > 0, "Finish before load call?"
    
    dec(p.useCount)
    if p.useCount == 0:
      # No more references to this block so "unload" it and 
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

proc testCore() = 
  let bm = newTestBlockMgr[uint16](128)
  var tr = initBPlusTree[uint16, int32, float32](bm)

  try:
    var keys = populate(tr, 100, high(int32))

    echo "testCore start = " & $tr
    for k in keys:
      assert int32(k) in tr, &"{k} notin tree"

    var kseq = toSeq(items(keys))
    while len(kseq) > 0:
      let i = rand(len(kseq) - 1)
      let k = int32(kseq[i])
      echo &"deleting {k}"
      del(kseq, i)
      del(tr, k)
      assert k notin tr, &"{k} not deleted"

      for k in kseq:
        echo &"checking {k}"
        assert int32(k) in tr

    graphviz(tr, "test.dot", false)
    echo "testCore finish = " & $tr
  except:
    graphviz(tr, "problem.dot", false)
    raise
  finally:
    bm.destroy()

proc test*() = 
  echo "Testy"

  badInsert()
  testCore()
