#!/bin/bash

source /code/osgcode/cmssoft/cms/cmsset_default.sh

export SCRAM_ARCH=$(python -c "import params; print params.scram_arch")
export CMS3TAG=$(python -c "import params; print params.cms3tag")
export CMSSW_VER=$(python -c "import params; print params.cmssw_ver")
export BASEDIR=`pwd`

echo "[setup] Using $CMS3TAG and $CMSSW_VER for this campaign"

if [ ! -d $CMSSW_VER ]; then
    scramv1 p -n ${CMSSW_VER} CMSSW $CMSSW_VER
    cd ${CMSSW_VER}
    cmsenv

    if [ ! -e /nfs-7/userdata/libCMS3/lib_${CMS3TAG}.tar.gz ]
    then
      echo "[setup] Making tar on-the-fly"
      source $BASEDIR/scripts/make_libCMS3.sh ${CMS3TAG} $CMSSW_VER
      mv lib_${CMS3TAG}.tar.gz /nfs-7/userdata/libCMS3/lib_${CMS3TAG}.tar.gz
      cd $CMSSW_BASE
    else
      cd $CMSSW_BASE
      cmsenv
      cp /nfs-7/userdata/libCMS3/lib_${CMS3TAG}.tar.gz . 
      echo "[setup] Extracting tar"
      tar -xzf lib_${CMS3TAG}.tar.gz
      scram b -j 10
    fi
else
    echo "[setup] $CMSSW_VER already exists, only going to set environment then"
    cd ${CMSSW_VER}
    cmsenv
fi

source /cvmfs/cms.cern.ch/crab3/crab.sh

cd $BASEDIR
