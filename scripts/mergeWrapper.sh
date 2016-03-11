#!/bin/bash

# grab params
# inputDir="/hadoop/cms/store/user/namin/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/160220_235603/0000"
# inputIndices="2,8"
# mergedIndex=1
# nevents=25000
# nevents_effective=21000
# xsec=0.0123
# kfactor=1.10
# filtEff=1.0

inputDir=$1          # "/hadoop/cms/store/user/namin/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/160220_235603/0000"
inputIndices=$2      # "2,8"
mergedIndex=$3       # 1
nevents=$4           # 25000
nevents_effective=$5 # 21000
xsec=$6              # 0.0123
kfactor=$7           # 1.10
filtEff=$8           # 1.0

inFile="merged_ntuple.root"
outFile="merged_ntuple_${mergedIndex}.root"
outDir="$inputDir/merged/"

# prevents the ntuples from staging out in your home directory.
mkdir tempdir; cd tempdir; cp ../* .

# setup environment
CMSSW_VERSION=CMSSW_7_4_14
echo "[merge wrapper] setting env"
export SCRAM_ARCH=slc6_amd64_gcc491
source /cvmfs/cms.cern.ch/cmsset_default.sh
OLDDIR=$(pwd)
cd /cvmfs/cms.cern.ch/slc6_amd64_gcc491/cms/cmssw/$CMSSW_VERSION/src
eval `scramv1 runtime -sh`
cd $OLDDIR

# debugging printouts 
echo "[merge wrapper] pwd: $(pwd)"
echo "[merge wrapper] scramarch: $SCRAM_ARCH"
echo "[merge wrapper] host: $(hostname)" 
echo "[merge wrapper] slc6 vs slc5: $(cat /etc/redhat-release)"
echo "[merge wrapper] current files in directory:"
echo `ls -l`

# merge and add branches
echo "[merge wrapper] t before mergeScript.C: $(date +%s)"
root -b -q -l "mergeScript.C (\"$inputDir\",\"$inputIndices\",\"merged_ntuple.root\")"
echo "[merge wrapper] t before addBranches.C: $(date +%s)"
root -b -q -l "addBranches.C (\"$inFile\",\"$outFile\",$nevents,$nevents_effective,$xsec,$kfactor,$filtEff)"
echo "[merge wrapper] t after addBranches.C: $(date +%s)"

# stageout
localFile=$(pwd)/$outFile
echo "[merge wrapper] copying file from $localFile to $outDir/$outFile"
lcg-cp -b -D srmv2 --vo cms --verbose file:$localFile srm://bsrm-3.t2.ucsd.edu:8443/srm/v2/server?SFN=$outDir/$outFile
echo "[merge wrapper] t after lcg-cp: $(date +%s)"

# cleanup
echo "[merge wrapper] cleaning up."
rm $outFile merged_ntuple.root *.txt

