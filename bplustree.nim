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
      ## Accessing a block outside of a load/finished pair is undefined: block managers
      ## are free to reuse buffers when finished() puts a block out of scope.
    finished*: proc (blk: Bn)
      ## Should be called when the caller is finished using a pointer returned by `load`.
      ## There should be as many calls to finish as there are to "load".
    destroy*: proc ()
      ## Called to destroy all memory held by the block manager.
      ## Doesn't make sense for all block managers.
    modified*: proc (blk: Bn) 
      ## Called to mark `blk` as modified. It's an error to modify a block without
      ## calling modified() on the block before handing it back to the manager with 
      ## a call to finished.

  BPtr*[Bn] = object
    ## A reference to a pointer loaded from the block manager.
    ## When it goes out of scope, `finished` is called for the block.
    ## The BPlusTree uses these extensively to bound accesses to blocks
    ## within load/finished pairs.
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
  ## Convenient auto convert to pointers.

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
    next, prev: Bn ## Linked list of leaves.
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
  assert arrLen <= arrMaxLen
  assert loc >= 0 and loc < arrLen

  let numTrailing = arrLen - loc - 1
  if numTrailing > 0:
    moveMem(addr(arr[loc]), addr(arr[loc+1]), sizeof(V) * numTrailing)

type
  PathPart[Bn] = tuple[nextIndex: int32; blk: Bn]
    ## A path through the tree.  
    ## Parameters:
    ##   nextIndex:  node.keys[path[n-1].nextIndex] == path[n].blk

  TreePath[Bn] = object
    ## Path from the root all the way down to a leaf.
    ## [0] is the root node, [^1] is the leaf.
    ## `childIdx` is the index of the child to follow to go
    ## down the path.
    ## NB - didn't get as much code reuse out of this as I hoped.
    parts: seq[tuple[childIdx: int32; blk: Bn]]
    pos: Natural 
      ## Index of the current position in the tree.
      ## As this is intended for algorithms that move up the
      ## tree, the only way to move down the path is to add a new
      ## link to the path.  When the tree is changed, the path 
      ## is not changed, so moving down the path can't be safe.

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
    path: TreePath[Bn] # Temp used for lookups.

  DuplicateKey* = object of Defect

const
  LastPathIndex = high(int32)
  EndOfPath = high(int32) - int32(1)
    ## For a TreePath, the index used for paths that go through the `InternalNode.last` member.

#TODO add flag in BPlusTree that can enable lazy deletes, that don't merge nodes. (don't merge any, or just merge leaves?)
#TODO bulk load operation.  could also be used to re-index a tree that has lazy deletes and needs it.

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

template loadInternal[Bn,K,V](tr: BPlusTree[Bn,K,V]; blk: Bn; newBlock: bool = false) : ptr InternalNode[K,Bn] = 
  var bp = mkPtr(tr.mgr, blk)
  let ip = asInternal(tr, bp.p)
  if not newBlock:
    doAssert ip.kind == nkIntern
  ip

template loadLeaf[Bn,K,V](tr: BPlusTree[Bn,K,V]; blk: Bn; newBlock: bool = false) : ptr LeafNode[K,V,Bn] = 
  var bp = mkPtr(tr.mgr, blk)
  let lp = asLeaf(tr, bp.p)
  if not newBlock:
    doAssert lp.kind == nkLeaf
  lp

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

proc `$`[Bn](path: TreePath[Bn]) : string = 
  result = newStringOfCap(len(path.parts) * 10)
  result &= "Path["
  for i in 0..<len(path.parts):
    let pp = path.parts[i]
    result &= &"{pp.blk}"
    if i == path.pos:
      result &= "*"
    if pp.childIdx != EndOfPath:
      result &= &" ({pp.childIdx}> "
  result &= "]"

proc init[Bn](path: var TreePath[Bn]) = 
  ## Resets a path to be empty.
  setLen(path.parts, 0)
  path.pos = 0

iterator blocksAlong[Bn](path: TreePath[Bn]) : Bn = 
  for i in 0..<len(path.parts):
    yield path.parts[i].blk

proc add[Bn](path: var TreePath[Bn]; nodeBlock: Bn; nextChildIdx: int) = 
  ## Adds a node and a link out of the node to the path.
  ## The position in the path is updated to this newly added
  ## block.  
  path.pos = len(path.parts)
  add(path.parts, (childIdx: int32(nextChildIdx), blk: nodeBlock))

proc atRoot[Bn](path: TreePath[Bn]) : bool = 
  assert len(path.parts) > 0
  path.pos == 0

proc atLeaf[Bn](path: TreePath[Bn]) : bool = 
  assert len(path.parts) > 0
  path.pos == len(path.parts) - 1

template curLeaf[Bn,K,V](path: TreePath[Bn]; tr: BPlusTree[Bn,K,V]) : ptr LeafNode[K,V,Bn] = 
  doAssert atLeaf(path)
  loadLeaf(tr, path.parts[path.pos].blk)

template curInternal[Bn,K,V](path: TreePath[Bn]; tr: BPlusTree[Bn,K,V]) : ptr InternalNode[K,Bn] = 
  doAssert not atLeaf(path)
  loadInternal(tr, path.parts[path.pos].blk)

template curParent[Bn,K,V](path: TreePath[Bn]; tr: BPlusTree[Bn,K,V]) : ptr InternalNode[K,Bn] = 
  doAssert not atRoot(path)
  loadInternal(tr, path.parts[path.pos-1].blk)

template currentBlock[Bn](path: TreePath[Bn]) : Bn = path.parts[path.pos].blk 


proc atLastChildOfParent[Bn](path: TreePath[Bn]) : bool = 
  if atRoot(path):
    false
  else:
    path.parts[path.pos-1].childIdx == LastPathIndex

proc atFirstChildOfParent[Bn](path: TreePath[Bn]) : bool = 
  if atRoot(path):
    false
  else:
    path.parts[path.pos-1].childIdx == 0

proc moveUp[Bn](path: var TreePath[Bn]) = 
  ## Moves from the current block to the parent of the current block.
  ## Not valid to call this when the current block is the root.
  doAssert not atRoot(path)
  dec(path.pos)

proc deleteLinkToCurrentBlock[Bn,K,V](path: TreePath[Bn]; tr: var BPlusTree[Bn,K,V]) = 
  ## Deletes the link from the parent block to the current block.  
  ## Only deletes it from the `InternalNode` records, doesn't worry
  ## about tree invariants.  If the current node is the root, the tree root will
  ## be replaced with an empty leaf node.
  if atRoot(path):
    # There always have to be a leaf in the tree.
    tr.root = tr.mgr.alloc()
    let lp = loadLeaf(tr, tr.root, true)
    initLeaf(tr, lp)
    tr.mgr.modified(tr.root)
  else:
    let pinf = path.parts[path.pos-1]
    let parent = loadInternal(tr, pinf.blk)

    tr.mgr.modified(pinf.blk)
    if pinf.childIdx == LastPathIndex:
      doAssert parent.numKeys > uint16(0)
      dec(parent.numKeys)
      parent.last = parent.keys[parent.numKeys].child
    else:
      delete(parent.keys, int(parent.numKeys), tr.numInternKeys, pinf.childIdx)
      dec(parent.numKeys)

proc modifyLinkToCurPos[Bn,K,V](path: TreePath[Bn]; tr: var BPlusTree[Bn,K,V]; newChild: Bn) = 
  ## Modifies the link from the parent of the current position to the current position to 
  ## `newChild`.  If the current position is the root of the path, `tr.root` is updated to 
  ## point to newChild.
  if atRoot(path):
    tr.root = newChild
  else:
    let pinf = path.parts[path.pos-1]
    let parent = loadInternal(tr, pinf.blk)

    tr.mgr.modified(pinf.blk)
    if atLastChildOfParent(path):
      parent.last = newChild
    else:
      parent.keys[pinf.childIdx].child = newChild

proc modifyKeyToCurPos[Bn,K,V](path: TreePath[Bn]; tr: var BPlusTree[Bn,K,V]; newKey: K) = 
  ## Modifies the key associated with the link between the parent of the current position and the
  ## current position. Does nothing for root nodes, or for the `last` node of an `InternalNode`.
  if not atRoot(path) and not atLastChildOfParent(path):
    let pinf = path.parts[path.pos-1]
    let parent = loadInternal(tr, pinf.blk)

    tr.mgr.modified(pinf.blk)
    parent.keys[pinf.childIdx].key = newKey

proc modifyKeyToPreviousSibling[Bn,K,V](path: TreePath[Bn]; tr: var BPlusTree[Bn,K,V]; newKey: K) = 
  ## Modifies the key associated with the link from the parent to the previous sibling
  ## of the current position.  Not valid to call if the current position is the first child of the parent.
  doAssert not atFirstChildOfParent(path)
  if not atRoot(path):
    let pinf = path.parts[path.pos-1]
    let parent = loadInternal(tr, pinf.blk)

    tr.mgr.modified(pinf.blk)
    if pinf.childIdx == LastPathIndex:
      doAssert parent.numKeys > uint16(0)
      parent.keys[parent.numKeys-1].key = newKey
    else:
      parent.keys[pinf.childIdx-1].key = newKey

proc previousSibling[Bn,K,V](path: TreePath[Bn]; tr: var BPlusTree[Bn,K,V]) : Bn = 
  ## For internal nodes, returns the previous sibliing under the same parent.
  ## Returns Bn(0) if there is none.
  if atRoot(path):
    Bn(0)
  else:
    let pinf = path.parts[path.pos-1]
    let parent = loadInternal(tr, pinf.blk)

    if parent.numKeys == 0:
      Bn(0)
    elif pinf.childIdx == LastPathIndex:
      parent.keys[parent.numKeys-1].child
    else:
      if pinf.childIdx > 0:
        parent.keys[pinf.childIdx-1].child
      else:
        Bn(0)


proc nextSiblingOfParent[Bn,K,V](path: TreePath[Bn]; tr: BPlusTree[Bn,K,V]) : Bn = 
  ## If there is a next sibling to the parent of the current node under the same grandparent, returns
  ## the block for it.
  ## Ex: If the current block is 900, the nextSiblingOfParent will be 300.
  ## root
  ##  - 123
  ##    - 900
  ##  - 300 
  ## +---
  if path.pos < 2:
    result = Bn(0)
  else:
    let pinf = path.parts[path.pos - 2]

    if pinf.childIdx == LastPathIndex:
      result = Bn(0)
    else:
      let parent = loadInternal(tr, pinf.blk)

      if pinf.childIdx == int32(parent.numKeys-1):
        result = parent.last
      else:
        result = parent.keys[pinf.childIdx+1].child

proc initBPlusTree*[Bn,K,V](mgr: BlockMgr[Bn]) : BPlusTree[Bn,K,V] = 
  ## Creates a new empty B+ tree, with a leaf as root.
  result = BPlusTree[Bn,K,V](mgr: mgr, 
                             numInternKeys: calcNumInternKeys[K,Bn](mgr.blockSize), 
                             numLeafKeys: calcNumLeafKeys[K,V,Bn](mgr.blockSize), 
                             root: mgr.alloc())
  initLeaf(result, loadLeaf[Bn,K,V](result, result.root, true))
  mgr.modified(result.root)

proc findLeaf[Bn,K,V](tr: BPlusTree[Bn,K,V]; blk: Bn; key: K; path: var TreePath[Bn]) : Bn = 
  ## Builds a path the start node down to the block that contains the leaf node for `key`.
  assert goodBlk(blk)
  switchBlock(tr, blk, node):
    for i in 0..<int(node.numKeys):
      if key <= node.keys[i].key:
        add(path, blk, i)
        let nxtBlk = node.keys[i].child
        switchBlock(tr, nxtBlk, _):
          return findLeaf(tr, nxtBlk, key, path)
        do:
          add(path, nxtBlk, EndOfPath)
          return nxtBlk

    if goodBlk(node.last):
      add(path, blk, LastPathIndex)
      switchBlock(tr, node.last, _):
        return findLeaf(tr, node.last, key, path)
      do:
        add(path, node.last, EndOfPath)
        return node.last
  do:
    add(path, blk, EndOfPath)
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

proc addToInternal[Bn,K,V](tr: var BPlusTree[Bn,K,V]; path: var TreePath[Bn];  key: K; blk: Bn) = 
  ## Add to the index so everything < `key` goes to `blk`.  Callers need to remember that
  ## after this function returns, `parents` may no longer be accurate.
  let lp = curInternal(path, tr)
  let node = currentBlock(path)

  assert not full(tr, lp)
  insertNoSplit(tr, lp, key, blk)
  tr.mgr.modified(node)
  
  if full(tr, lp):
    # Need to split the internal node so the mid key goes into the parent.
    #   a b c d e f g
    #        ]^[
    let mid = tr.numInternKeys div 2
    let newNode = tr.mgr.alloc()
    let nrp = loadInternal(tr, newNode, true); initInternal(tr, nrp)

    lp.numKeys = uint16(mid)
    nrp.numKeys = uint16(tr.numInternKeys - mid - 1)

    nrp.last = lp.last
    lp.last = lp.keys[mid].child # Don't lose pointer associated with this mid key that's being pushed up.

    # Populate new node.
    copyMem(addr(nrp.keys[0]), addr(lp.keys[mid+1]), int(nrp.numKeys) * sizeof(nrp.keys[0]))
    tr.mgr.modified(newNode)

    if atRoot(path):
      # Grow a new root.
      tr.root = tr.mgr.alloc()
      let rp = loadInternal(tr, tr.root, true); initInternal(tr, rp)
      insertNoSplit(tr, rp, lp.keys[mid].key, node)
      rp.last = newNode
      tr.mgr.modified(tr.root)
    else:
      modifyLinkToCurPos(path, tr, newNode)
      moveUp(path)
      addToInternal(tr, path, lp.keys[mid].key, node)

proc addToLeaf[Bn,K,V](tr: var BPlusTree[Bn,K,V]; path: var TreePath[Bn]; key: K; val: V) = 
  ## Does all of the work adding K/V to the leaf, including splitting the nodes if necessary.
  let lp = curLeaf(path, tr)
  let leaf = currentBlock(path)

  assert not full(tr, lp)
  # NB The way we do this, a node won't ever have full occupancy, just n-1.  Revisit once tests are in place.
  insertNoSplit(tr, lp, key, val)
  tr.mgr.modified(currentBlock(path))

  if full(tr, lp):
    # Splitting a leaf.
    let mid = tr.numLeafKeys div 2
    let newRightLeaf = tr.mgr.alloc()
    let nrp = loadLeaf(tr, newRightLeaf, true)

    initLeaf(tr, nrp)

    # Link new right leaf into leaf lists
    nrp.prev = leaf
    nrp.next = lp.next
    if goodblk(lp.next):
      let rhn = loadLeaf(tr, lp.next)
      rhn.prev = newRightLeaf
      tr.mgr.modified(lp.next)
    lp.next = newRightLeaf

    # a b c d e f g
    #       ^ to right including mid
    lp.numValues = uint16(mid)
    nrp.numValues = uint16(tr.numLeafKeys - mid)

    assert nrp.numValues > uint16(0)
    copyMem(addr(nrp.values[0]), addr(lp.values[mid]), sizeof(nrp.values[0]) * int(nrp.numValues))
    tr.mgr.modified(newRightLeaf)

    if atRoot(path):
      # Special case, a new tree where the root was a leaf node.
      assert leaf == tr.root
      tr.root = tr.mgr.alloc()
      let rp = loadInternal(tr, tr.root, true)
      initInternal(tr, rp)
      rp.numKeys = 1
      rp.keys[0] = (key: lp.values[lp.numValues-1].key, child: leaf)
      rp.last = newRightLeaf
      tr.mgr.modified(tr.root)
    else:
      if atLastChildOfParent(path):
        # The last pointer led here. So we need to add a key for the lhs, and point last to
        # rhs. Change last first, as addToInternal may switch up parent nodes if it has to split.
       moveUp(path)
       let pp = curInternal(path, tr)
       pp.last = newRightLeaf 
       addToInternal(tr, path, lp.values[lp.numValues-1].key, leaf)
        
      else:
        # Modify link that brought us here so it has the right maxkey, and push up key for the new rhs.
        modifyKeyToCurPos(path, tr, lp.values[lp.numValues-1].key)
        moveUp(path)
        addToInternal(tr, path, nrp.values[nrp.numValues-1].key, newRightLeaf)

template atLeastHalfFull(tr: BPlusTree; n: ptr LeafNode) : bool = 
  int(n.numValues) >= (tr.numLeafKeys div 2)

template atLeastHalfFull(tr: BPlusTree; n: ptr InternalNode) : bool = 
  int(n.numKeys) >= (tr.numInternKeys div 2)

template moreThanHalfFull(tr: BPlusTree; n: ptr LeafNode) : bool = 
  int(n.numValues) > (tr.numLeafKeys div 2)

template moreThanHalfFull(tr: BPlusTree; n: ptr InternalNode) : bool = 
  int(n.numKeys) > (tr.numInternKeys div 2)

proc lastLeaf[Bn,K,V](tr: BPlusTree[Bn,K,V]; node: Bn) : Bn = 
  ## Returns the block of leaf with the largest keys found from 
  ## starting at `node` in the tree.
  var cur = node

  while goodBlk(cur):
    switchBlock(tr, cur, np):
      if goodBlk(np.last):
        cur = np.last
      elif np.numKeys > uint16(0):
        cur = np.keys[np.numKeys-1].child
    do:
      result = cur
      cur = Bn(0)

proc greatestKey[Bn,K,V](tr: BPlusTree[Bn,K,V]; node: Bn) : (bool, K) =  
  ## Returns the greatest key stored under `node`. 
  let lb = lastLeaf(tr, node)
  if goodBlk(lb):
    let lp = loadLeaf(tr, lb)

    if lp.numValues > uint16(0):
      result = (true, lp.values[lp.numValues-1].key)

proc deleteFromInternal[Bn,K,V](tr: var BPlusTree[Bn,K,V]; path: var TreePath[Bn]) = 
  ## Deletes link to the current node in the `path` from the parent, 
  ## merging or borrowing among the internal nodes to try to maintain
  ## the size invariants for internal nodes.
  # Get the neighbor to the parent before we delete the link and between parent and child.
  let nbblk = nextSiblingOfParent(path, tr)
  deleteLinkToCurrentBlock(path, tr)

  if not atRoot(path):
    moveUp(path)

    let parentBlk = currentBlock(path)
    let np = curInternal(path, tr)
    if np.numKeys == 0:
      # Nothing left in the current block, except for possibly a last pointer.
      # If it is the root node, remove it.  (new tree levels are created and
      # and removed from the root level).
      doAssert goodblk(np.last)
      if atRoot(path):
        # This is how the tree is supposed to shorten, with the root node
        # going away.
        modifyLinkToCurPos(path, tr, np.last)
        tr.mgr.free(parentBlk)

    else:
      if not atRoot(path) and not atLeastHalfFull(tr, np):
        # If we're not the root, try to maintain the minimum number of children.
        let node = currentBlock(path)

        if goodBlk(nbblk):
          let neighbor = loadInternal(tr, nbblk)
          
          if moreThanHalfFull(tr, neighbor):
            # We can take the least key from our neighbor to keep above the minimum 
            # child limit.
            #                   root
            #            30             last
            #             /                \
            #  np[10 20 last=30]  neighbor[50 60 70 last=100]

            # Promote last link to be a regular child. Seems clunky to have to 
            # do a walk with lastLeaf.
            assert goodBlk(np.last)
            # There are two branches that we need to get the greatest keys for - 
            # np.last, so it can be promoted to a regular child in keys[], and 
            # the first key of neighbor, so we know what to update the key of the link
            # from the parent to np with.  If either of these trees have no values in the leaves, 
            # we can't borrow.
            let (ok, largestLeftKey) = greatestKey(tr, np.last)
            let (ok2, largestLastKey) = greatestKey(tr, neighbor.keys[0].child)

            if ok and ok2:
              np.keys[np.numKeys] = (key: largestLeftKey, child: np.last)
              inc(np.numKeys)
              tr.mgr.modified(parentBlk)

              # Now take our last link from least value of neighbor.
              np.last = neighbor.keys[0].child
              delete(neighbor.keys, int(neighbor.numKeys), tr.numInternKeys, 0)
              dec(neighbor.numKeys)
              tr.mgr.modified(nbblk)

              modifyKeyToCurPos(path, tr, largestLastKey)

          else:
            # Neighbor has half or less max, so merge our keys into it and delete this node.
            #                     root
            #        30                    100
            #        /                       \
            # np[10 last=30]    neighbor[50, 60, last=100]
            assert goodBlk(np.last)
            let (ok, largestLastKey) = greatestKey(tr, np.last)

            if ok:
              # Move neighbor keys over to make room for np.keys + np.last
              moveMem(addr(neighbor.keys[np.numKeys+1]), addr(neighbor.keys[0]), int(neighbor.numKeys)*sizeof(neighbor.keys[0]))
              copyMem(addr(neighbor.keys[0]), addr(np.keys[0]), int(np.numKeys)*sizeof(np.keys[0]))
              neighbor.numKeys += np.numKeys + 1
              tr.mgr.modified(nbblk)

              neighbor.keys[np.numKeys].key = largestLastKey
              neighbor.keys[np.numKeys].child = np.last

              # Remove link to now empty node, and free it.
              deleteFromInternal(tr, path)
              tr.mgr.free(node)

        else:
          assert atLastChildOfParent(path)
          let parent = curParent(path, tr)
          let nbblk = previousSibling(path, tr)

          if goodBlk(nbblk) and parent.numKeys > uint16(0):
            # ie, previous leaf is our sibling, and doesn't belong to a different parent.
            let neighbor = loadInternal(tr, nbblk)

            if atLeastHalfFull(tr, neighbor):
              # We can try to borrow the last entry from our neighbor.
              let (ok, borrowedEntKey) = greatestKey(tr, neighbor.last)
              
              if ok:
                insert(np.keys, int(np.numKeys), tr.numInternKeys, 0, (key: borrowedEntKey, child: neighbor.last))
                inc(np.numKeys)
                tr.mgr.modified(parentBlk)
                dec(neighbor.numKeys)
                let neighborNewLastKey = neighbor.keys[neighbor.numKeys].key
                neighbor.last = neighbor.keys[neighbor.numKeys].child
                modifyKeyToPreviousSibling(path, tr, neighborNewLastKey)
                tr.mgr.modified(nbblk)
            else:
              # Try to merge into our previous sibling, and remove this block.
              let (ok, keyForNbLast) = greatestKey(tr, neighbor.last)
              let (ok2, keyForLast) = greatestKey(tr, np.last)

              if ok and ok2:
                neighbor.keys[neighbor.numKeys] = (key: keyForNbLast, child: neighbor.last)
                inc(neighbor.numKeys)
                if np.numKeys > uint16(0):
                  copyMem(addr(neighbor.keys[neighbor.numKeys]), addr(np.keys[0]), int(np.numKeys) * sizeof(np.keys[0]))
                  neighbor.numKeys += np.numKeys

                neighbor.last = np.last
                tr.mgr.modified(parentBlk)
                tr.mgr.modified(nbblk)
                modifyKeyToPreviousSibling(path, tr, keyForLast)
                deleteFromInternal(tr, path)
                

proc deleteLeafBlock[Bn,K,V](tr: var BPlusTree[Bn,K,V]; path: var TreePath[Bn]; lp: ptr LeafNode[K,V,Bn]) = 
  ## Deletes the leaf at the current position in the path, fixing up the leaf linked list. 
  ## Also deletes the link to the leaf from the parent.
  # Unlink from leaf list.
  doAssert atLeaf(path)

  if goodBlk(lp.prev):
    let pp = loadLeaf(tr, lp.prev)
    pp.next = lp.next
    tr.mgr.modified(lp.prev)

  if goodBlk(lp.next):
    let np = loadLeaf(tr, lp.next)
    np.prev = lp.prev
    tr.mgr.modified(lp.next)

  let leaf = currentBlock(path)

  # Kill any links to this leaf before we free it.
  if not atRoot(path):
    deleteFromInternal(tr, path)

  tr.mgr.free(leaf)


proc deleteFromLeaf[Bn,K,V](tr: var BPlusTree[Bn,K,V]; path: var TreePath[Bn]; key: K) = 
  let leaf = currentBlock(path)
  let lp = curLeaf(path, tr)
  let i = keyInsertPoint(tr, lp, key)

  if i < int(lp.numValues) and key == lp.values[i].key:
    tr.mgr.modified(leaf)
    delete(lp.values, int(lp.numValues), tr.numLeafKeys, i)
    dec(lp.numValues)
    dec(tr.count)

    if not atLeastHalfFull(tr, lp):
      if goodBlk(lp.next) and not atLastChildOfParent(path): 
        # We have a neighbor that is under the same parent internal node.
        let neighbor = loadLeaf(tr, lp.next)

        if moreThanHalfFull(tr, neighbor):
          # Pull least from neighbor to fill ourselves out to stay above minimum size.
          tr.mgr.modified(lp.next)
          lp.values[lp.numValues] = neighbor.values[0]
          inc(lp.numValues)
          delete(neighbor.values, int(neighbor.numValues), tr.numLeafKeys, 0)
          dec(neighbor.numValues)
          modifyKeyToCurPos(path, tr, lp.values[lp.numValues-1].key)
        else:
          # Pack this leaf's values into the neighbor, and then delete the leaf.
          # Since we know the ordering between the two leaves, we can do this with 
          # bulk moves and copys. 
          assert int(lp.numValues + neighbor.numValues) < tr.numLeafKeys
          moveMem(addr(neighbor.values[lp.numValues]), addr(neighbor.values[0]), int(neighbor.numValues) * sizeof(lp.values[0]))
          copyMem(addr(neighbor.values[0]), addr(lp.values[0]), int(lp.numValues) * sizeof(lp.values[0]))
          neighbor.numValues += lp.numValues
          tr.mgr.modified(lp.next)
          deleteLeafBlock(tr, path, lp)

      else:
        # We're the last child of the leaf, so we need to look to the previous neighbor
        # for opportunities to shuffle values around.
        if atRoot(path):
          # We're just a root leaf.  No size minimum for us.
          discard
        else:
          assert atLastChildOfParent(path)
          let parent = curParent(path, tr)

          if goodBlk(lp.prev) and parent.numKeys > uint16(0):
            # ie, previous leaf is our sibling, and doesn't belong to a different parent.
            let neighbor = loadLeaf(tr, lp.prev)

            if moreThanHalfFull(tr, neighbor):
              # neighbor[10 20 23 33] -> lp[45 60] -> nil
              # Take the greatest value from our neighbor and insert it in leaf.
              # Since we know the relative ordering, we can just insert it without 
              # searching.
              insert(lp.values, int(lp.numValues), tr.numLeafKeys, 0, neighbor.values[neighbor.numValues-1])
              inc(lp.numValues)
              dec(neighbor.numValues)
              tr.mgr.modified(lp.prev)
              modifyKeyToPreviousSibling(path, tr, neighbor.values[neighbor.numValues-1].key)

            else:
              # Neighbor is less than half full, so there should be room
              # for our values in the neighbor. 
              # neighbor[10 20] -> lp[45 60] -> nil
              # Add our values to end of neighbor, and delete leaf from tree.
              copyMem(addr(neighbor.values[neighbor.numValues]), addr(lp.values[0]), int(lp.numValues) * sizeof(lp.values[0]))
              neighbor.numValues += lp.numValues
              tr.mgr.modified(lp.prev)
              modifyKeyToPreviousSibling(path, tr, neighbor.values[neighbor.numValues-1].key)

              # Kill the now empty leaf, and delete the link from the parent.
              deleteLeafBlock(tr, path, lp)

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
  init(tr.path)
  let leaf = findLeaf(tr, tr.root, key, tr.path)

  # We should always start out with a leaf in an empty tree, so we should always be able to find something.
  doAssert goodBlk(leaf)
  addToLeaf(tr, tr.path, key, val)
  inc(tr.count)

proc del*[Bn,K,V](tr: var BPlusTree[Bn,K,V]; key: K) = 
  ## Deletes `key` from the tree.  Does nothing if the key is not found.
  init(tr.path)
  let leaf = findLeaf(tr, tr.root, key, tr.path); doAssert goodBlk(leaf)

  deleteFromLeaf(tr, tr.path, key)

template withValPtr(tr: var BPlusTree; keyval: typed; varName: untyped; action: untyped) : untyped = 
  init(tr.path)
  let leaf = findLeaf(tr, tr.root, keyval, tr.path); doAssert goodBlk(leaf)
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
    if node.numValues == 0:
      return &"""{blk} [label="{blk}(nil)",shape=box];"""
    elif node.numValues == 1:
      return &"""{blk} [label="{blk}({node.values[0].key})",shape=box];"""
    else:
      return &"""{blk} [label="{blk}({node.values[0].key}..{node.values[node.numValues-1].key}, n={node.numValues})",shape=box];"""

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

proc graphvizPath*[Bn,K,V](tr: BPlusTree[Bn, K, V]; file: string; key: K) = 
  ## Writes a graph of just the nodes along the path
  ## to the leaf node that would contain `key`.
  var fh = open(file, fmWrite)
  try:
    write(fh, "digraph bplustree {\N")
    # Define nodes.
    var path: TreePath[Bn]
    let leaf = findLeaf(tr, tr.root, key, path); assert goodBlk(leaf)
    let lp = loadLeaf(tr, leaf)

    write(fh, gvnodedef(tr, leaf))
    write(fh, "\N")
    write(fh, &"""KEY_{key} [shape=parallelogram];""")
    write(fh, "\N")

    if goodblk(lp.next):
      # Show the next leaf from our target leaf, as it's sometimes involved in borrows
      # and merges.
      write(fh, gvnodedef(tr, lp.next))
      write(fh, "\N")

    if goodblk(lp.prev):
      # Show the prev leaf from our target leaf, as it's sometimes involved in borrows
      # and merges.
      write(fh, gvnodedef(tr, lp.prev))
      write(fh, "\N")
    
    for blk in blocksAlong(path):
      write(fh, gvnodedef(tr, blk))
      write(fh, "\N")
      switchBlock(tr, blk, np):
        for i in 0..<int(np.numKeys):
          write(fh, gvnodedef(tr, np.keys[i].child))
          write(fh, "\N")

        if goodblk(np.last):
          write(fh, gvnodedef(tr, np.last))
          write(fh, "\N")

      do:
        discard

      
    # Link them.
    for blk in blocksAlong(path):
      switchBlock(tr, blk, node):
        for i in 0..<int(node.numKeys):
          write(fh, &"""{blk} -> {node.keys[i].child} [label="{node.keys[i].key}"];""")
          write(fh, "\N")
        if goodBlk(node.last):
          write(fh, &"""{blk} -> {node.last} [label="*"];""")
          write(fh, "\N")
      do:
        discard

    # Show previous and next for the leaf.  Usually helpful for debugging.
    if goodblk(lp.next):
      write(fh, &"""{leaf} -> {lp.next} [label="Next"];""")
    if goodblk(lp.prev):
      write(fh, &"""{leaf} -> {lp.prev} [label="Prev"];""")

    write(fh, "}\N")
  finally:
    close(fh)

proc len*[Bn,K,V](tr: BPlusTree[Bn,K,V]) : int = tr.count
