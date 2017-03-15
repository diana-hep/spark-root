# Performance Benchmarks for public CMS Dataset
Description of objects and operations/queries performed for benchmarking

## List of Queries/Operations performed
### Dataframe/Dataset style
- Dataset[Row] Action: count 
- Dataset[Row] Action: reduce (func=max/sum/min) on fields {pt, eta, phi}
- 


### RDD style
- 

## List of Objects used
Most-widely used and most important Physics Objects are used.

- Muons as 
```
1. recoMuons_muons__RECO_
    fullPath: recoMuons_muons__RECO_.recoMuons_muons__RECO_obj.reco::RecoCandidate.reco::LeafCandidate
2. recoMuons_muonsFromCosmics1Leg__RECO_
    fullPath: recoMuons_muonsFromCosmics1Leg__RECO_.recoMuons_muonsFromCosmics1Leg__RECO_obj.reco::RecoCandidate.reco::LeafCandidate
3. recoMuons_muonsFromCosmics__RECO_
    fullPath: recoMuons_muonsFromCosmics__RECO_.recoMuons_muonsFromCosmics__RECO_obj.reco::RecoCandidate.reco::LeafCandidate
```
- Photons as 
```
1. recoPhotons_photons__RECO_
    fullPath: recoPhotons_photons__RECO_.recoPhotons_photons__RECO_obj.reco::RecoCandidate.reco::LeafCandidate
```
- Electrons as 
```
1. recoGsfElectrons_gsfElectrons__RECO_
    fullPath: recoGsfElectrons_gsfElectrons__RECO_.recoGsfElectrons_gsfElectrons__RECO_obj.reco::RecoCandidate.reco::LeafCandidate
```
- PFTaus as 
```
1. recoPFTaus_shrinkingConePFTauProducer__RECO_
    fullPath: recoPFTaus_shrinkingConePFTauProducer__RECO_.recoPFTaus_shrinkingConePFTauProducer__RECO_obj.reco::BaseTau.reco::RecoCandidate.reco::LeafCandidate
2. recoPFTaus_hpsTancTaus__RECO_
    fullPath: recoPFTaus_hpsTancTaus__RECO_.recoPFTaus_hpsTancTaus__RECO_obj.reco::BaseTau.reco::RecoCandidate.reco::LeafCandidate
3. recoPFTaus_hpsPFTauProducer__RECO_
    fullPath: recoPFTaus_hpsPFTauProducer__RECO_.recoPFTaus_hpsPFTauProducer__RECO_obj.reco::BaseTau.reco::RecoCandidate.reco::LeafCandidate
```
- MET as 
```
recoPFMETs_pfMet__RECO_
recoMETs_tcMetWithPFclusters__RECO_
recoMETs_tcMet__RECO_
recoMETs_htMetKT6__RECO_
```
- PFJets as 
```
1. recoJPTJets_TCTauJetPlusTrackZSPCorJetAntiKt5__RECO_
    fullPath: recoJPTJets_TCTauJetPlusTrackZSPCorJetAntiKt5__RECO_.recoJPTJets_TCTauJetPlusTrackZSPCorJetAntiKt5__RECO_obj.reco::Jet.reco::CompositePtrCandidate.reco::LeafCandidate
2. recoPFJets_kt6PFJets__RECO_
    fullPath: recoPFJets_kt6PFJets__RECO_.recoPFJets_kt6PFJets__RECO_obj.reco::Jet.reco::CompositePtrCandidate.reco::LeafCandidate
3. recoPFJets_kt4PFJets__RECO_
    fullPath: recoPFJets_kt4PFJets__RECO_.recoPFJets_kt4PFJets__RECO_obj.reco::Jet.reco::CompositePtrCandidate.reco::LeafCandidate
4. recoPFJets_ak7PFJets__RECO_
    fullPath: recoPFJets_ak7PFJets__RECO_.recoPFJets_ak7PFJets__RECO_obj.reco::Jet.reco::CompositePtrCandidate.reco::LeafCandidate
5. recoPFJets_ak5PFJets__RECO_
    fullPath: recoPFJets_ak5PFJets__RECO_.recoPFJets_ak5PFJets__RECO_obj.reco::Jet.reco::CompositePtrCandidate.reco::LeafCandidate
```
