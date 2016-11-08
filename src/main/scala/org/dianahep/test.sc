package org.dianahep

import org.dianahep.root4j._
import org.dianahep.root4j.interfaces._

object test {

  def findTree(dir: TDirectory): TTree =
    {
      for (i <- 0 until dir.nKeys) {
        val obj = dir.getKey(i).getObject.asInstanceOf[core.AbstractRootObject]
        if (obj.getRootClass.getClassName == "TDirectory" ||
          obj.getRootClass.getClassName == "TTree") {
          if (obj.getRootClass.getClassName == "TDirectory")
            return findTree(obj.asInstanceOf[TDirectory])
          else (obj.getRootClass.getClassName == "TTree")
          return obj.asInstanceOf[TTree]
        }
      }
      null
    }                                             //> findTree: (dir: org.dianahep.root4j.interfaces.TDirectory)org.dianahep.root4
                                                  //| j.interfaces.TTree

  val filename = "/Users/vk/software/diana-hep/test_data/test_1.root"
                                                  //> filename  : String = /Users/vk/software/diana-hep/test_data/test_1.root
  //  get the reader
  //  find the TTree - for now assume there is only 1
  val reader = new RootFileReader(new java.io.File(filename))
                                                  //> reader  : org.dianahep.root4j.RootFileReader = org.dianahep.root4j.RootFileR
                                                  //| eader@708f5957
  val tree = findTree(reader.getTopDir)           //> tree  : org.dianahep.root4j.interfaces.TTree = org.dianahep.root4j.proxy.TTr
                                                  //| ee@38afe297
  //  print the tree
  val branches = tree.getBranches                 //> branches  : org.dianahep.root4j.interfaces.TObjArray = org.dianahep.root4j.p
                                                  //| roxy.TObjArray@2df3b89c
  val leaves = tree.getLeaves                     //> leaves  : org.dianahep.root4j.interfaces.TObjArray = org.dianahep.root4j.pro
                                                  //| xy.TObjArray@23348b5d
  println(s"Branch Size = ${branches.size}", s"Leaves size = ${leaves.size}")
                                                  //> (Branch Size = 8,Leaves size = 141)
  for (x <- 0 until branches.size; y = branches.get(x).asInstanceOf[TBranchElement])
    println(y.getName, "  ", y.getTitle, "  ", y.getType, "  ", y.getStreamerType, "  ",
      y.getParentName, "  ", y.getClassName)      //> (Muons,  ,Muons_,  ,4,  ,-1,  ,,  ,vector<analysis::core::Muon>)
                                                  //| (Jets,  ,Jets_,  ,4,  ,-1,  ,,  ,vector<analysis::core::Jet>)
                                                  //| (Vertices,  ,Vertices_,  ,4,  ,-1,  ,,  ,vector<analysis::core::Vertex>)
                                                  //| (Event,  ,Event,  ,0,  ,-1,  ,,  ,analysis::core::Event)
                                                  //| (EventAuxiliary,  ,EventAuxiliary,  ,0,  ,-1,  ,,  ,analysis::core::EventAu
                                                  //| xiliary)
                                                  //| (MET,  ,MET,  ,0,  ,-1,  ,,  ,analysis::core::MET)
                                                  //| (Electrons,  ,Electrons_,  ,4,  ,-1,  ,,  ,vector<analysis::core::Electron>
                                                  //| )
                                                  //| (Taus,  ,Taus_,  ,4,  ,-1,  ,,  ,vector<analysis::core::Tau>)

  for (
    x <- 0 until branches.size; y = branches.get(x).asInstanceOf[TBranchElement]; bs = y.getBranches;
    j <- 0 until bs.size; b = bs.get(j).asInstanceOf[TBranchElement]
  ) println(b.getName, "  ", b.getTitle, "  ", b.getType, "  ", b.getStreamerType, "  ",
    b.getParentName, "  ", b.getClassName)        //> (Muons._charge,  ,_charge[Muons_],  ,41,  ,3,  ,analysis::core::Muon,  ,ana
                                                  //| lysis::core::Track)
                                                  //| (Muons._pt,  ,_pt[Muons_],  ,41,  ,5,  ,analysis::core::Muon,  ,analysis::c
                                                  //| ore::Track)
                                                  //| (Muons._pterr,  ,_pterr[Muons_],  ,41,  ,5,  ,analysis::core::Muon,  ,analy
                                                  //| sis::core::Track)
                                                  //| (Muons._eta,  ,_eta[Muons_],  ,41,  ,5,  ,analysis::core::Muon,  ,analysis:
                                                  //| :core::Track)
                                                  //| (Muons._phi,  ,_phi[Muons_],  ,41,  ,5,  ,analysis::core::Muon,  ,analysis:
                                                  //| :core::Track)
                                                  //| (Muons._isTracker,  ,_isTracker[Muons_],  ,41,  ,18,  ,analysis::core::Muon
                                                  //| ,  ,analysis::core::Muon)
                                                  //| (Muons._isStandAlone,  ,_isStandAlone[Muons_],  ,41,  ,18,  ,analysis::core
                                                  //| ::Muon,  ,analysis::core::Muon)
                                                  //| (Muons._isGlobal,  ,_isGlobal[Muons_],  ,41,  ,18,  ,analysis::core::Muon, 
                                                  //|  ,analysis::core::Muon)
                                                  //| (Muons._isTight,  ,_isTight[Muons_],  ,41,  ,18,  ,analysis::core::Muon,  ,
                                                  //| analysis::core::Muon)
                                                  //| (Muons._isMedium,  ,_isMedium[Muons_], 
                                                  //| Output exceeds cutoff limit.

  if (tree == null) throw new Exception("TTree is not found")
  val slist = (for (i <- 0 until reader.streamerInfo.size) yield reader.streamerInfo.get(i)).filter(_.asInstanceOf[core.AbstractRootObject].
    getRootClass.getClassName == "TStreamerInfo") //> slist  : scala.collection.immutable.IndexedSeq[Any] = Vector(org.dianahep.r
                                                  //| oot4j.proxy.TStreamerInfo@2b4bac49, org.dianahep.root4j.proxy.TStreamerInfo
                                                  //| @fd07cbb, org.dianahep.root4j.proxy.TStreamerInfo@3571b748, org.dianahep.ro
                                                  //| ot4j.proxy.TStreamerInfo@3e96bacf, org.dianahep.root4j.proxy.TStreamerInfo@
                                                  //| 484970b0, org.dianahep.root4j.proxy.TStreamerInfo@4470f8a6, org.dianahep.ro
                                                  //| ot4j.proxy.TStreamerInfo@7c83dc97, org.dianahep.root4j.proxy.TStreamerInfo@
                                                  //| 7748410a, org.dianahep.root4j.proxy.TStreamerInfo@740773a3, org.dianahep.ro
                                                  //| ot4j.proxy.TStreamerInfo@37f1104d, org.dianahep.root4j.proxy.TStreamerInfo@
                                                  //| 55740540, org.dianahep.root4j.proxy.TStreamerInfo@60015ef5, org.dianahep.ro
                                                  //| ot4j.proxy.TStreamerInfo@2f54a33d, org.dianahep.root4j.proxy.TStreamerInfo@
                                                  //| 1018bde2, org.dianahep.root4j.proxy.TStreamerInfo@65b3f4a4, org.dianahep.ro
                                                  //| ot4j.proxy.TStreamerInfo@f2ff811, org.dianahep.root4j.proxy.TStreamerInfo@5
                                                  //| 68ff82, org.dianahep.ro
                                                  //| Output exceeds cutoff limit.
  //  printing the TStreamerInfo Record
  val lStreamers = for (
    x <- slist; y = x.asInstanceOf[TStreamerInfo]; i <- 0 until y.getElements.size;
    elem = y.getElements.get(i).asInstanceOf[TStreamerElement]
  ) yield (y, elem)                               //> lStreamers  : scala.collection.immutable.IndexedSeq[(org.dianahep.root4j.in
                                                  //| terfaces.TStreamerInfo, org.dianahep.root4j.interfaces.TStreamerElement)] =
                                                  //|  Vector((org.dianahep.root4j.proxy.TStreamerInfo@2b4bac49,org.dianahep.root
                                                  //| 4j.proxy.TStreamerBase@5f16132a), (org.dianahep.root4j.proxy.TStreamerInfo@
                                                  //| 2b4bac49,org.dianahep.root4j.proxy.TStreamerString@69fb6037), (org.dianahep
                                                  //| .root4j.proxy.TStreamerInfo@2b4bac49,org.dianahep.root4j.proxy.TStreamerStr
                                                  //| ing@36d585c), (org.dianahep.root4j.proxy.TStreamerInfo@fd07cbb,org.dianahep
                                                  //| .root4j.proxy.TStreamerBasicType@87a85e1), (org.dianahep.root4j.proxy.TStre
                                                  //| amerInfo@fd07cbb,org.dianahep.root4j.proxy.TStreamerBasicType@671a5887), (o
                                                  //| rg.dianahep.root4j.proxy.TStreamerInfo@3571b748,org.dianahep.root4j.proxy.T
                                                  //| StreamerBase@5552768b), (org.dianahep.root4j.proxy.TStreamerInfo@3e96bacf,o
                                                  //| rg.dianahep.root4j.proxy.TStreamerBase@3c947bc5), (org.dianahep.root4j.prox
                                                  //| y.TStreamerInfo@484970b
                                                  //| Output exceeds cutoff limit.
  for ((y, elem) <- lStreamers) println(y.getName, "  ", y.getTitle, "  ", elem.getType, "  ", elem.getTypeName, "  ",
    elem.getName, "  ", elem.getTitle, "  ", elem.getSize, "  ", elem.getArrayLength)
                                                  //> (TNamed,  ,,  ,66,  ,BASE,  ,TObject,  ,Basic ROOT object,  ,0,  ,0)
                                                  //| (TNamed,  ,,  ,65,  ,TString,  ,fName,  ,object identifier,  ,24,  ,0)
                                                  //| (TNamed,  ,,  ,65,  ,TString,  ,fTitle,  ,object title,  ,24,  ,0)
                                                  //| (TObject,  ,,  ,13,  ,unsigned int,  ,fUniqueID,  ,object unique identifier
                                                  //| ,  ,4,  ,0)
                                                  //| (TObject,  ,,  ,15,  ,unsigned int,  ,fBits,  ,bit field status word,  ,4, 
                                                  //|  ,0)
                                                  //| (TList,  ,,  ,0,  ,BASE,  ,TSeqCollection,  ,Sequenceable collection ABC,  
                                                  //| ,0,  ,0)
                                                  //| (TSeqCollection,  ,,  ,0,  ,BASE,  ,TCollection,  ,Collection abstract base
                                                  //|  class,  ,0,  ,0)
                                                  //| (TCollection,  ,,  ,66,  ,BASE,  ,TObject,  ,Basic ROOT object,  ,0,  ,0)
                                                  //| (TCollection,  ,,  ,65,  ,TString,  ,fName,  ,name of the collection,  ,24,
                                                  //|   ,0)
                                                  //| (TCollection,  ,,  ,3,  ,int,  ,fSize,  ,number of elements in collection, 
                                                  //|  ,4,  ,0)
                                                  //| (TTree,  ,,  ,67,  ,BASE,  ,TNamed,  ,The basis for a named object (name, t
                                                  //| itle),  ,0,  ,0)
                                                  //| (TTree,  ,,  ,0
                                                  //| Output exceeds cutoff limit.

  for ((y, elem) <- lStreamers if y.getName=="analysis::core::Muon") println(y.getName, "  ", y.getTitle, "  ", elem.getType, "  ", elem.getTypeName, "  ",
    elem.getName, "  ", elem.getTitle, "  ", elem.getSize, "  ", elem.getArrayLength)
                                                  //> (analysis::core::Muon,  ,,  ,0,  ,BASE,  ,analysis::core::Track,  ,,  ,0,  
                                                  //| ,0)
                                                  //| (analysis::core::Muon,  ,,  ,18,  ,bool,  ,_isTracker,  ,,  ,1,  ,0)
                                                  //| (analysis::core::Muon,  ,,  ,18,  ,bool,  ,_isStandAlone,  ,,  ,1,  ,0)
                                                  //| (analysis::core::Muon,  ,,  ,18,  ,bool,  ,_isGlobal,  ,,  ,1,  ,0)
                                                  //| (analysis::core::Muon,  ,,  ,18,  ,bool,  ,_isTight,  ,,  ,1,  ,0)
                                                  //| (analysis::core::Muon,  ,,  ,18,  ,bool,  ,_isMedium,  ,,  ,1,  ,0)
                                                  //| (analysis::core::Muon,  ,,  ,18,  ,bool,  ,_isLoose,  ,,  ,1,  ,0)
                                                  //| (analysis::core::Muon,  ,,  ,18,  ,bool,  ,_isPF,  ,,  ,1,  ,0)
                                                  //| (analysis::core::Muon,  ,,  ,5,  ,float,  ,_normChi2,  ,,  ,4,  ,0)
                                                  //| (analysis::core::Muon,  ,,  ,5,  ,float,  ,_d0BS,  ,,  ,4,  ,0)
                                                  //| (analysis::core::Muon,  ,,  ,5,  ,float,  ,_dzBS,  ,,  ,4,  ,0)
                                                  //| (analysis::core::Muon,  ,,  ,5,  ,float,  ,_d0PV,  ,,  ,4,  ,0)
                                                  //| (analysis::core::Muon,  ,,  ,5,  ,float,  ,_dzPV,  ,,  ,4,  ,0)
                                                  //| (analysis::core::Muon,  ,,  ,3,  ,int,  ,_nPLs
                                                  //| Output exceeds cutoff limit.




}