#!/bin/bash

source /code/osgcode/cmssoft/cms/cmsset_default.sh

export SCRAM_ARCH=$(python -c "import params; print params.scram_arch")
export CMS3TAG=$(python -c "import params; print params.cms3tag")
export CMSSW_VER=$(python -c "import params; print params.cmssw_ver")
export DASHBASE=$(python -c "import params; print params.dashboard_name")
export BASEDIR=`pwd`


echo "[setup] Using $CMS3TAG and $CMSSW_VER for this campaign"

# if the cmssw dir doesn't exist or the current tag hasn't been extracted
if [ ! -d $CMSSW_VER ] || [ ! -e $CMSSW_VER/lib_${CMS3TAG}.tar.gz ]; then
    scramv1 p -n ${CMSSW_VER} CMSSW $CMSSW_VER
    cd ${CMSSW_VER}
    cmsenv

    if [ ! -e /nfs-7/userdata/libCMS3/lib_${CMS3TAG}.tar.gz ]
    then
      echo "[setup] Making tar on-the-fly"
      source $BASEDIR/scripts/make_libCMS3.sh ${CMS3TAG} $CMSSW_VER
      cp lib_${CMS3TAG}.tar.gz /nfs-7/userdata/libCMS3/lib_${CMS3TAG}.tar.gz
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

if [ ! -e ~/public_html/.htaccess ]; then
    echo "[setup] don't have .htaccess file. copying one to ~/public_html/ for you"
    cp dashboard/htaccess ~/public_html/.htaccess
    chmod 755 ~/public_html/.htaccess
fi
export DASHBOARD=$(python -c "import utils; utils.make_dashboard()")
chmod -R 755 "$HOME/public_html/$DASHBASE/"
echo "[setup] dashboard is at: $DASHBOARD"
