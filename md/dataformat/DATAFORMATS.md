# Data Formats
*Trying to pile up all the format specifications*

## ROOT Data Types
TDataType class is responsible for knowing these guys

Enumerator | Type | Description
-----------|------|------------
1 | Char\_t | ...
2 | Short\_t | ...
3 | Int | ...
4 | Long | ...
5 | Float | ...
6 | Int | Counter
7 | char\* | Char Star
8 | Double | ...
9 | Double32 | ...
10 | char | ...
11 | UChar | ...
12 | UShort | ...
13 | UInt | ...
14 | ULong | ...
15 | UInt | Bits
16 | Long64 | ...
17 | ULong64 | ...
18 | Bool | ...
19 | Float16 | ...
20 | void | Void
21 | UInt | DataTypeAliasUnsigned Int
22 | SignedChar | DataTypeAliasSigned Char
-1 | Other | Other type
0 | NoType | ....

## TTree
- TBranch -> TLeaf
  - TLeafI/B/C/D/F/L/S - simple leaf types
- TBranchElement -> TLeafElement

### Types of the BranchElement
Each instance of the TBranchElement class identifies the type of the node it represents.

BranchElementType | Int | Description
------------------|-----|------------
kLeafNode         | 0 | Leaf Node - not clear exactly what constitutes a leaf node
kBaseClassNode    | 1 | Base Class Node - see ROOT src for TBranchElement.h
kObjectNode       | 2 | Node to represent an object
kClonesNode       | 3 | Node of Clones
kSTLNode          | 4 | STL Container of some objects
kCLonesMemberNode | 31 | Member of a Clone
kSTLMemberNode    | 41 | A Node that is a subbranch of a STLNode branch

## TStreamerInfo
- TStreamerInfo -> TStreamerElement
  - TStreamerBase
  - TStreamerBasicPointer
  - TStreamerBasicType
  - TStreamerLoop
  - TStreamerObject
  - TStreamerObjectAny
  - TStreamerObjectPointer
  - TStreamerSTL
  - TStreamerString

### Streamer Types
Enumerator | Int | Description
-----------|-----|------------
kBase | 0 | Base Class Element
kOffsetL | 20 | Fixed Size Array
kOffsetP | 40 | Pointer to an object
kCounter | 6 | Counter for array size
kCharStar | 7 | Pointer to array of char
kChar | 1 | ...
kShort | 2 | ...
kInt | 3 | ...
kLong | 4 | ...
kFloat | 5 | ...
kDouble | 8 | ...
kDouble32 | 9 | ...
kLegacyChar | 10 | Equal to TDataType's kchar
kUChar | 11 | ...
kUShort | 12 | ...
kUInt | 13 | ...
kULong | 14 | ...
kBits | 15 | TObject::fBits in case of a referenced objet
kLong64 | 16 | ...
kULong64 | 17 | ...
kBool | 18 | ...
kFloat16 | 19 | ...
kObject | 61 | Class derived from TObject
kAny | 62 | Class not derived from TObject
kObjectp | 63 | Pointer to a class derived from TObject and comment field
kObjectP | 64 | POinter to a class derived from TObject and with NO comment field
kTString | 65 | TString special case
kTObject | 66 | TObject special case
kTNamed | 67 | special case...
kAnyp | 68 | Pointer to a class not derived from TObject with comment field
kAnyP | 69 | POinter to a class not derived from TObject with NO comment field
kAnyPnoVT | 70 | Pointer to a class not derived from TObject with NO comment field and no virtual table
kSTLp | 71 | Pointer to an STL container
kSTL | 300 | STL Container ???
kSTLstring | 365 | std::string
kStreamer | 500 | Don't understand why, but typically all the vectors have this streamer type.

### STL Container Types
Enumerator | Int
-----------|----
kNotSTL | 0
kSTLvector | 1
kSTLlist | 2
kSTLdeque | 3
kSTLmap | 4
kSTLmultimap | 5
kSTLset | 6
kSTLmultiset | 7
kSTLbitset | 8
kSTLforwardlist | 9
kSTLunorderedset | 10
kSTLunorderedmultiset | 11
kSTLunorderedmap | 12
kSTLunorderedmultimap | 13
kSTLend | 14
kSTLany | 300
kSTLstring | 365
