#
# Generic B+ Tree.  
import
  macros, strformat

type
  BlockMgr*[Bn] = ref object
    ## Interface for block managers.  The manager can allocate and free blocks, 
    ## and load blocks into memory and return their pointer.
    ##
    ## Parameters:
    ##  `Bn`: Name used to reference blocks.  Some sort of int, could be distinct.
    blockSize*: Natural 
      ## Size of the blocks in bytes.  Public, so it can be referenced.
    zerosMem*: bool
      ## True if the block manager zeros blocks on alloc.
    alloc*: proc () : Bn
      ## Allocates a block and returns a name for the block.
    free*: proc (blk: Bn)
      ## Frees a block. Once a block is freed, it can be immediately reused.
    load*: proc (blk: Bn) : pointer
      ## Loads the block into memory (if necessary), and returns the pointer for it.
    finished*: proc (blk: Bn)
      ## Should be called the caller is finished using a pointer returned by `load`.
    destroy*: proc ()
      ## Called to destroy all memory held by the block manager.
    modified*: proc (blk: Bn) 
      ## Called to mark `blk` as modified. 

  BPtr*[Bn] = object
    ## A reference to a pointer loaded from the block manager.
    ## When it goes out of scope, `finished` is called for the block.
    ##
    ## Warning:
    ##   Until the newruntime is in working order, these pointers can not be copied safely.
    p*: pointer
    blk: Bn
    bm: BlockMgr[Bn]

proc `=destroy`[Bn](bp: var BPtr[Bn]) =
  if bp.p != nil:
    bp.bm.finished(bp.blk)

proc `=sink`*[Bn](d: var BPtr[Bn]; s: BPtr[Bn]) = 
  if d.p != nil:
    d.bm.finished(d.blk)
  d.p = s.p
  d.blk = s.blk
  d.bm = s.bm

converter toPointer*[Bn](bp: BPtr[Bn]) : pointer = bp.p
  ## Auto convert to pointers since these can't be copied yet.

proc mkPtr*[Bn](bm: BlockMgr[Bn]; blk: Bn) : BPtr[Bn] = 
  ## Loads a pointer from the block manager and returns it.
  BPtr[Bn](p: bm.load(blk), blk: blk, bm: bm)

type
  InMemoryBlockMgr[Bn] = ref object
    ## Example block manager where all of the blocks are in memory, and
    ## `Bn` is the "name" used to refer to block.
    ## number of blocks you'll have to deal with, you can pick a shorter width int
    ## to save extra space in the blocks.  Written as an example, and used for testing.
    ##
    ## :Parameters:
    ##   Bn: integer type used as names for blocks. Or a ``distinct`` version of an integer type. The zero value of this type is considered to be "no block", so we should not hand that number out.
    blocks: seq[tuple[p: pointer; link: int]] 
      ## Associate block names with their pointers. The link field allows us to re-use freed blocks
      ## without scanning the array.
    nextFree: int 
      ## Index of next free item in the list. =0 if there is none.

proc newInMemoryBlockMgr*[Bn](blockSize: Natural) : BlockMgr[Bn] = 
  ## This is an example block manager that hands out integer handles for blocks of
  ## memory.
  let bm = InMemoryBlockMgr[Bn]()
  let rv = BlockMgr[Bn](blockSize: blockSize, zerosMem: true)

  # Add a dummy entry, so we always return block numbers > 0
  setLen(bm.blocks, 1)

  rv.destroy = proc () = 
    for i in 1..<len(bm.blocks):
      dealloc(bm.blocks[i].p)

  rv.alloc = proc () : Bn = 
    if bm.nextFree > 0:
      result = Bn(bm.nextFree)
      bm.nextFree = bm.blocks[bm.nextFree].link
    else:
      result = Bn(len(bm.blocks))
      add(bm.blocks, (p: alloc0(rv.blockSize), link: 0))

  rv.free = proc (blk: Bn) = 
    let bi = int(blk)
    bm.blocks[bi].link = bm.nextFree
    bm.nextFree = bi

  rv.load = proc (blk: Bn) : pointer = 
    result = bm.blocks[int(blk)].p

  rv.modified = proc (blk: Bn) = discard
  rv.finished = proc (blk: Bn) = discard

  return rv

type
  NodeKind = enum
    nkIntern, nkLeaf

  KeyChild[K,Bn] = tuple[key: K; child: Bn]

  InternalNode[K,Bn] = object
    ## Nodes of the tree. An incomplete object.  These are allocated to be the blocksize for the tree.
    kind: NodeKind
    numKeys: uint16
    last: Bn
      ## Child to traverse if a key is greater than all of the 
      ## keys in `keys`.
    keys: UncheckedArray[KeyChild[K,Bn]]
      ## Note: by packing k and Bn, if there's an alignment mismatch
      ## between the two types, a lot of space will be wasted by padding.
      ## Did this for simplicity initially.

  LeafNode[K,V, Bn] = object
    ## A leaf node.  
    kind: NodeKind ## nkLeaf
    next: Bn ## Next leaf node in the tree, in order.
    numValues: uint16
    values: UncheckedArray[ValueEnt[K,V]] 
      ## Key value pairs, in key order.  Not doing anything fancy yet to mitigate
      ## having to move things when inserting new keys.

  ValueEnt[K,V] = object
    ## A single Key/Value.  There's an array of these in leaf nodes.
    key: K
    value: V

template hokeyOffset(t: typedesc; field: untyped) : int = 
  #NB workaround - system.offsetOf doesn't deal with types it can't instantiate.
  let vp: ptr t = nil
  cast[int](addr(vp.field))

proc calcNumInternKeys[K,Bn](blockSize: Natural) : int = 
  ## For the given types, how many keys can we hold in an internal node?
  let used = hokeyOffset(InternalNode[K,Bn], keys)

  result = (blockSize - used) div sizeof(KeyChild[K,Bn])
  assert result > 0

proc calcNumLeafKeys[K,V,Bn](blockSize: Natural) : int = 
  let used = hokeyOffset(LeafNode[K,V,Bn], values)
  result = (blockSize - used) div sizeof(ValueEnt[K,V])
  assert result > 0

proc insert[V](arr: var UncheckedArray[V]; arrLen, arrMaxLen: int; loc: int; val: V) = 
  ## Inserts `val` into `arr` at `loc`, moving the following entries if necessary.
  assert arrLen < arrMaxLen
  assert loc >= 0 and loc <= arrLen

  # a0 b1 c2 d3 e4 f5 f6 ^
  if loc == arrLen:
    arr[loc] = val
  else:
    # a b c d e f
    #     ^
    # arrLen=6, loc = 2
    let numTrailing = arrLen - loc 
    moveMem(addr(arr[loc+1]), addr(arr[loc]), sizeof(V) * numTrailing)
    arr[loc] = val

proc delete[V](arr: var UncheckedArray[V]; arrLen, arrMaxLen: int; loc: int) = 
  ## Deletes a value from the array, without changing the order of the remaining 
  ## elements.
  assert arrLen < arrMaxLen
  assert loc >= 0 and loc < arrLen

  let numTrailing = arrLen - loc - 1
  if numTrailing > 0:
    moveMem(addr(arr[loc]), addr(arr[loc+1]), sizeof(V) * numTrailing)

type
  PathPart[Bn] = tuple[nextIndex: int32; blk: Bn]
    ## A path through the tree.  
    ## Parameters:
    ##   nextIndex:  node.keys[path[n-1].nextIndex] == path[n].blk

  BPlusTree*[Bn, K,V] = object
    ## A B+ tree.
    ##
    ## :Parameters:
    ##   `Bn`: What type used to name a block.  An int type, or a distinct version of an int type.
    ##
    ##   `K`:  Key type the tree is indexed on. Should be a value type, not a reference. 
    ##
    ##   `V`: Value type held in the leaves of the B+ tree.  For values small relative to the block size.  Traditionally, this would be a pointer or name for a block containing the data.
    ##
    ##
    ## *Limitations/Bugs*
    ##    - There's a choice whether to pack node contents as tightly as possible, or keep them like an object, 
    ##      where all fields are aligned and won't cause a fault if you de-reference a pointer to them.
    ##      This implementation chooses to keep fields aligned to avoid the extra cost of having to provide 
    ##      compare functions that don't care if the pointers to the keys are aligned. 
    ##
    ##    - Keys and values need to be value types.  Or, the caller needs to register ref types with the GC so it knows they are reachable.
    ##
    ##    - Key search just does linear scans.  Should switch up to binary search once a threshold number of keys/node is reached.
    ##
    ##    - To avoid copying, nodes can't have 100% occupancy, the most they can sustain is `keysPerNode - 1`. 
    ##      This can probably  be worked around with some clever logic, but no point in doing that until some tests are built up.
    ##
    numInternKeys, numLeafKeys: int
    mgr: BlockMgr[Bn]
    root: Bn
    count: Natural
    path: seq[PathPart[Bn]] # Temp used for lookups.

  DuplicateKey* = object of Defect

proc initInternal[Bn,K,V](tr: BPlusTree[Bn,K,V]; p: ptr InternalNode[K,Bn]) = 
  if not tr.mgr.zerosMem:
    zeroMem(p, tr.mgr.blockSize)
  #p.kind = nkIntern

proc initLeaf[Bn,K,V](tr: BPlusTree[Bn,K,V]; p: ptr LeafNode[K,V,Bn]) = 
  if not tr.mgr.zerosMem:
    zeroMem(p, tr.mgr.blockSize)
  p.kind = nkLeaf

template goodBlk(b: untyped) : bool = 
  int(b) > 0

proc asInternal[Bn,K,V](tr: BPlusTree[Bn,K,V]; p: pointer) : ptr InternalNode[K,Bn] = result = cast[ptr InternalNode[K,Bn]](p)
proc asLeaf[Bn,K,V](tr: BPlusTree[Bn,K,V]; p: pointer) : ptr LeafNode[K,V,Bn] = cast[ptr LeafNode[K,V,Bn]](p)

template loadInternal[Bn,K,V](tr: BPlusTree[Bn,K,V]; blk: Bn) : ptr InternalNode[K,Bn] = 
  var bp = mkPtr(tr.mgr, blk)
  asInternal(tr, bp.p)

template loadLeaf[Bn,K,V](tr: BPlusTree[Bn,K,V]; blk: Bn) : ptr LeafNode[K,V,Bn] = 
  var bp = mkPtr(tr.mgr, blk)
  asLeaf(tr, bp.p)

template switchBlock(tr: BPlusTree; blk: untyped; varName: untyped; internalAction, leafAction: untyped) = 
  ## Switches on the node kind of a block.  If it is an internal node, `varName` is bound to a `InternalNode`, and
  ## internalAction is executed.  Otherwise, `varName` is bound to a `LeafNode`, and leafAction is executed.
  block:
    var bp = mkPtr(tr.mgr, blk)
    let p = cast[ptr NodeKind](bp.p)
    assert int(p[]) < 2 and int(p[]) >= 0 # Haha.  Yeah, this can happen if you screw up the pointers or don't zero memory when you said you did.
    case p[]
    of nkIntern:
      let varName = asInternal(tr, p)
      internalAction

    of nkLeaf:
      let varName = asLeaf(tr, p)
      leafAction

proc `$`*[Bn,K,V](tr: BPlusTree[Bn,K,V]) : string = 
  &"B+Tree(numItems={tr.count}, keysPerInternalNode={tr.numInternKeys}, keysPerLeaf={tr.numLeafKeys})"

proc initBPlusTree*[Bn,K,V](mgr: BlockMgr[Bn]) : BPlusTree[Bn,K,V] = 
  ## Creates a new empty B+ tree, with a leaf as root.
  result = BPlusTree[Bn,K,V](mgr: mgr, 
                             numInternKeys: calcNumInternKeys[K,Bn](mgr.blockSize), 
                             numLeafKeys: calcNumLeafKeys[K,V,Bn](mgr.blockSize), 
                             root: mgr.alloc())
  initLeaf(result, loadLeaf[Bn,K,V](result, result.root))
  mgr.modified(result.root)

proc findLeaf[Bn,K,V](tr: BPlusTree[Bn,K,V]; blk: Bn; key: K; path: var seq[PathPart[Bn]]) : Bn = 
  ## Builds a path the start node down to the block that contains the leaf node for `key`.
  assert goodBlk(blk)
  switchBlock(tr, blk, node):
    add(path, (nextIndex: int32(-1), blk: blk))
    for i in 0..<int(node.numKeys):
      if key <= node.keys[i].key:
        path[^1].nextIndex = int32(i)
        let nxtBlk = node.keys[i].child
        switchBlock(tr, nxtBlk, _):
          return findLeaf(tr, nxtBlk, key, path)
        do:
          return nxtBlk

    if goodBlk(node.last):
      switchBlock(tr, node.last, _):
        return findLeaf(tr, node.last, key, path)
      do:
        return node.last
  do:
    return blk

proc keyInsertPoint[Bn,K,V](tr: BPlusTree[Bn,K,V]; node: ptr LeafNode[K,V,Bn]; key: K) : int = 
  ## Find insert point for `key` inside of the leaf.  Returns node.numKeys if none was found.
  while result < int(node.numValues):
    if key <= node.values[result].key:
      break
    else:
      inc(result)

proc keyInsertPoint[Bn,K,V](tr: BPlusTree[Bn,K,V]; node: ptr InternalNode[K,Bn]; key: K) : int = 
  while result < int(node.numKeys):
    if key <= node.keys[result].key:
      break
    else:
      inc(result)

proc insertNoSplit[Bn,K,V](tr: BPlusTree[Bn,K,V]; node: ptr InternalNode[K,Bn]; key: K; blk: Bn) = 
  ## Inserts a key/blk in an internal node that has enough space for it.
  assert int(node.numKeys) < tr.numInternKeys
  let i = keyInsertPoint(tr, node, key)
  insert(node.keys, int(node.numKeys), tr.numInternKeys, i, (key: key, child: blk))
  inc(node.numKeys)

proc insertNoSplit[Bn,K,V](tr: BPlusTree[Bn,K,V]; node: ptr LeafNode[K,V,Bn]; key: K; val: V) = 
  ## Inserts a key/value in a leaf node that has enough space for it.
  assert int(node.numValues) < tr.numLeafKeys
  let i = keyInsertPoint(tr, node, key)
  if i < int(node.numValues) and node.values[i].key == key:
    raise newException(DuplicateKey, &"Adding duplicate key {key}")
  else:
    insert(node.values, int(node.numValues), tr.numLeafKeys, i, ValueEnt[K,V](key: key, value: val))
    inc(node.numValues)

template full(tr: BPlusTree; n: ptr LeafNode) : untyped = int(n.numValues) == tr.numLeafKeys
template full(tr: BPlusTree; n: ptr InternalNode) : untyped = int(n.numKeys) == tr.numInternKeys
  ## The node full of keys?

proc addToInternal[Bn,K,V](tr: var BPlusTree[Bn,K,V]; parents: var seq[PathPart[Bn]]; node: Bn; key: K; blk: Bn) = 
  ## Add to the index so everything < `key` goes to `blk`.  Callers need to remember that
  ## after this function returns, `parents` may no longer be accurate.
  let lp = loadInternal(tr, node)

  assert not full(tr, lp)
  insertNoSplit(tr, lp, key, blk)
  tr.mgr.modified(node)
  
  if full(tr, lp):
    # Need to split the internal node so the mid key goes into the parent.
    #   a b c d e f g
    #        ]^[
    let mid = tr.numInternKeys div 2
    let newNode = tr.mgr.alloc()
    let nrp = loadInternal(tr, newNode); initInternal(tr, nrp)

    lp.numKeys = uint16(mid)
    nrp.numKeys = uint16(tr.numInternKeys - mid - 1)

    nrp.last = lp.last
    lp.last = lp.keys[mid].child # Don't lose pointer associated with this mid key that's being pushed up.

    # Populate new node.
    copyMem(addr(nrp.keys[0]), addr(lp.keys[mid+1]), int(nrp.numKeys) * sizeof(nrp.keys[0]))
    tr.mgr.modified(newNode)

    if len(parents) == 0:
      # Grow a new root.
      tr.root = tr.mgr.alloc()
      let rp = loadInternal(tr, tr.root); initInternal(tr, rp)
      insertNoSplit(tr, rp, lp.keys[mid].key, node)
      rp.last = newNode
      tr.mgr.modified(tr.root)
    else:
      let parent = parents.pop()
      let pp = loadInternal(tr, parent.blk)

      if parent.nextIndex < 0:
        pp.last = newNode
      else:
        pp.keys[parent.nextIndex].child = newNode

      addToInternal(tr, parents, parent.blk, lp.keys[mid].key, node)



proc addToLeaf[Bn,K,V](tr: var BPlusTree[Bn,K,V]; parents: var seq[PathPart[Bn]]; leaf: Bn; key: K; val: V) = 
  ## Does all of the work adding K/V to the leaf, including splitting the nodes if necessary.
  let lp = loadLeaf(tr, leaf)

  assert not full(tr, lp)
  # NB The way we do this, a node won't ever have full occupancy, just n-1.  Revisit once tests are in place.
  insertNoSplit(tr, lp, key, val)
  tr.mgr.modified(leaf)

  if full(tr, lp):
    # Splitting a leaf.
    let mid = tr.numLeafKeys div 2
    let newRightLeaf = tr.mgr.alloc()
    let nrp = loadLeaf(tr, newRightLeaf)

    initLeaf(tr, nrp)

    # Link new right leaf into leaf lists
    nrp.next = lp.next
    lp.next = newRightLeaf

    # a b c d e f g
    #       ^ to right including mid
    lp.numValues = uint16(mid)
    nrp.numValues = uint16(tr.numLeafKeys - mid)

    assert nrp.numValues > uint16(0)
    copyMem(addr(nrp.values[0]), addr(lp.values[mid]), sizeof(nrp.values[0]) * int(nrp.numValues))
    tr.mgr.modified(newRightLeaf)

    if len(parents) == 0:
      # Special case, a new tree where the root was a leaf node.
      assert leaf == tr.root
      tr.root = tr.mgr.alloc()
      let rp = loadInternal(tr, tr.root)
      initInternal(tr, rp)
      rp.numKeys = 1
      rp.keys[0] = (key: lp.values[lp.numValues-1].key, child: leaf)
      rp.last = newRightLeaf
      tr.mgr.modified(tr.root)
    else:
      let parent = parents.pop()
      let pp = loadInternal(tr, parent.blk)

      if parent.nextIndex < 0:
        # The last pointer led here. So we need to add a key for the lhs, and point last to
        # rhs. Change last first, as addToInternal may switch up parent nodes if it has to split.
       pp.last = newRightLeaf 
       addToInternal(tr, parents, parent.blk, lp.values[lp.numValues-1].key, leaf)
        
      else:
        # Modify link that brought us here so it has the right maxkey, and push up key for the new rhs.
        pp.keys[parent.nextIndex].key = lp.values[lp.numValues-1].key
        addToInternal(tr, parents, parent.blk, nrp.values[nrp.numValues-1].key, newRightLeaf)

template atLeastHalfFull(tr: BPlusTree; n: ptr LeafNode) : bool = 
  int(n.numValues) >= (tr.numLeafKeys div 2)

template moreThanHalfFull(tr: BPlusTree; n: ptr LeafNode) : bool = 
  int(n.numValues) > (tr.numLeafKeys div 2)

proc graphviz*[Bn,K,V](tr: BPlusTree[Bn,K,V]; file: string; showLeafLinks = false) 
proc deleteFromLeaf[Bn,K,V](tr: var BPlusTree[Bn,K,V]; parents: var seq[PathPart[Bn]]; leaf: Bn; key: K) = 
  let lp = loadLeaf(tr, leaf)
  let i = keyInsertPoint(tr, lp, key)

  if i < int(lp.numValues) and key == lp.values[i].key:
    tr.mgr.modified(leaf)
    delete(lp.values, int(lp.numValues), tr.numLeafKeys, i)
    dec(lp.numValues)
    dec(tr.count)

    if not atLeastHalfFull(tr, lp):
      if goodBlk(lp.next):
        let neighbor = loadLeaf(tr, lp.next)

        if moreThanHalfFull(tr, neighbor):
          # Pull least from neighbor to fill ourselves out to stay above minimum size.
          tr.mgr.modified(lp.next)
          lp.values[lp.numValues] = neighbor.values[0]
          inc(lp.numValues)
          delete(neighbor.values, int(neighbor.numValues), tr.numLeafKeys, 0)
          dec(neighbor.numValues)
          # Our test should be the key of the new lowest entry in the neighbor.
          if len(parents) > 0:
            let parent = loadInternal(tr, parents[^1].blk)
            parent.keys[parents[^1].nextIndex].key = neighbor.values[0].key
            tr.mgr.modified(parents[^1].blk)
        else:
          assert false, &"should merge values from {leaf} into {lp.next}"
      else:
        assert false, "not implemented, check left neighbor for merge or distribute options"

proc leastLeaf[Bn,K,V](tr: BPlusTree[Bn,K,V]) : Bn = 
  ## Returns the leaf containing the lowest key.  
  result = tr.root
  var foundLeaf = false
   
  while not foundLeaf and goodBlk(result):
    switchBlock(tr, result, node):
      assert node.numKeys > uint16(0)
      result = node.keys[0].child
    do:
      foundLeaf = true

iterator pairs*[Bn,K,V](tr: BPlusTree[Bn,K,V]) : (K, V) = 
  ## Iterates all key value pairs in the tree in order.
  var leaf = leastLeaf(tr)

  while goodBlk(leaf):
    let n = loadLeaf(tr, leaf)
    for i in 0..<int(n.numValues):
      yield (n.values[i].key, n.values[i].value)
    leaf = n.next

proc add*[Bn,K,V](tr: var BPlusTree[Bn,K,V]; key: K; val: V) = 
  ## Adds a K,V association. No duplicates allowed.
  var path: seq[PathPart[Bn]]
  let leaf = findLeaf(tr, tr.root, key, path)

  # We should always start out with a leaf in an empty tree, so we should always be able to find something.
  assert goodBlk(leaf)
  addToLeaf(tr, path, leaf, key, val)
  inc(tr.count)

proc del*[Bn,K,V](tr: var BPlusTree[Bn,K,V]; key: K) = 
  ## Deletes `key` from the tree.  Does nothing if the key is not found.
  var path: seq[PathPart[Bn]]
  let leaf = findLeaf(tr, tr.root, key, path); assert goodBlk(leaf)

  deleteFromLeaf(tr, path, leaf, key)

template withValPtr(tr: var BPlusTree; keyval: typed; varName: untyped; action: untyped) : untyped = 
  setLen(tr.path, 0)
  let leaf = findLeaf(tr, tr.root, keyval, tr.path); assert goodBlk(leaf)
  let lp = loadLeaf(tr, leaf)
  let i = keyInsertPoint(tr, lp, keyval)

  if i < int(lp.numValues) and lp.values[i].key == keyval:
    let varName = (blk: leaf, val: addr(lp.values[i].value))
    action

proc updateValue*[Bn,K,V](tr: var BPlusTree[Bn,K,V]; key: K; newVal: V) = 
  ## Updates the value associated with `key` to `newVal`. Does nothing if
  ## `K` isn't in the tree.
  withValPtr(tr, key, res):
    res.val[] = newVal
    tr.mgr.modified(res.blk)

proc contains*[Bn,K,V](tr: var BPlusTree[Bn,K,V]; key: K) : bool = 
  ## Containment test for `key`.
  withValPtr(tr, key, res):
    result = true

template withValue*(tr: var BPlusTree; key: typed; varName, action: untyped) : untyped = 
  ## If `key` is in the tree, `action` is executed with `varName` bound to a copy of the value.
  ## Otherwise, nothing happens.
  ##
  ## Example:
  ## ```nim
  ##    withValue(someTree, 98, val):
  ##      echo &"someTree[98] = {val}"
  ## ```
  withValPtr(tr, key, res):
    let varName = res.val[]
    action

proc gvnodedef[Bn,K,V](tr: BPlusTree[Bn,K,V]; blk: Bn) : string = 
  ## Returns a node definition for a graphviz file.
  ## Internal are oval, leaves are squares.
  switchBlock(tr, blk, node):
    return &"""{$blk} [label="{$blk}",shape=oval];"""
  do:
    return &"""{blk} [label="{blk}({node.values[0].key}..{node.values[node.numValues-1].key})",shape=box];"""

iterator allNodes[Bn,K,V](tr: BPlusTree[Bn,K,V]) : Bn = 
  ## Iterates all nodes, in no guaranteed order.
  var q = @[tr.root]
  while len(q) > 0:
    let n = pop(q)
    yield n
    switchBlock(tr, n, node):
      for i in 0..<int(node.numKeys):
        add(q, node.keys[i].child)
      if goodBlk(node.last):
        add(q, node.last)
    do:
      discard

  #TODO once we support overflow blocks or something similar for dealing with
  #      duplicates, this will need to be modified to follow the leaf list and 
  #      yield any leaves not directly pointed to by an internal node.

proc graphviz*[Bn,K,V](tr: BPlusTree[Bn,K,V]; file: string; showLeafLinks = false) = 
  ## Writes out a graphviz file to `file`.
  var fh = open(file, fmWrite)
  try:
    write(fh, "digraph bplustree {\N")
    # Define nodes.
    for blk in allNodes(tr):
      write(fh, gvnodedef(tr, blk))
      write(fh, "\N")

    # Link them.
    for blk in allNodes(tr):
      switchBlock(tr, blk, node):
        for i in 0..<int(node.numKeys):
          write(fh, &"""{blk} -> {node.keys[i].child} [label="{node.keys[i].key}"];""")
          write(fh, "\N")
        if goodBlk(node.last):
          write(fh, &"""{blk} -> {node.last} [label="*"];""")
          write(fh, "\N")
      do:
        if showLeafLinks and goodBlk(node.next):
          write(fh, &"""{blk} -> {node.next} [label=">"];""")
          write(fh, "\N")

    write(fh, "}\N")
  finally:
    close(fh)


