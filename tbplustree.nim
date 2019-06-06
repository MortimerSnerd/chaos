import
  system/ansi_c, bplustree, strformat

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

proc test*() = 
  echo "Testy"
  var tr = initBPlusTree[uint16, int32, float32](newTestBlockMgr[uint16](128))

  echo "BAT"
  for i in 1..300:
  #for i in countdown(300, 1):
    tr.add(int32(i), float32(i) * 7)

  tr.updateValue(int32(299), 34544.0f)

  assert int32(55) in tr

  for k, v in pairs(tr):
    echo &"k={k} v={v}"

  withValue(tr, int32(122), x):
    echo "HAH " & $x

  tr.withValue(int32(900), x):
    echo "NOPE " & $x

  del(tr, int32(122))
  withValue(tr, int32(122), x):
    echo "HAH AGAIN " & $x

  #graphviz(tr, "test.dot", false)
  echo "TREE = " & $tr
