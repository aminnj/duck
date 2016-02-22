import datetime
import commands
import os

# supply these things (they should all come from the sample object)
taskdir="../crab/crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1"
base="/home/users/namin/sandbox/duck/scripts/"
shortname="ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1"
unmergedDir="/hadoop/cms/store/user/namin/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/160220_235603/0000"
xsec=0.0123
kfactor=1.10
filtEff=1.0

submitFile = taskdir+"/submit.cmd"
workingDirectory = base
executableScript = base+"mergeWrapper.sh"
mergeScript = base+"mergeScript.C"
addBranchesScript = base+"addBranches.C"
status, proxyFile = commands.getstatusoutput("find /tmp/x509up_u* -user $USER")
condorLogFile="/data/tmp/%s/%s/%s.log" % (os.getenv("USER"),shortname,datetime.datetime.now().strftime("+%Y.%m.%d-%H.%M.%S"))
stdLogFiles="/data/tmp/%s/%s/std_logs/" % (os.getenv("USER"),shortname)
inputFiles = ",".join([executableScript, mergeScript, addBranchesScript])


# this is the only stuff that is different between different merged files
inputIndices=",".join(map(str,[2,8]))
mergedIndex=1
nevents=25000
nevents_effective=21000
inputArguments=" ".join(map(str,[unmergedDir, inputIndices, mergedIndex, nevents, nevents_effective, xsec, kfactor, filtEff]))

condorParams = {
        "exe": executableScript,
        "args": inputArguments,
        "inpfiles": inputFiles,
        "condorlog": condorLogFile,
        "stdlog": stdLogFiles,
        "proxy": proxyFile,
        }

# print './submit.sh -t -e %s -a "%s" -i %s -u %s -l %s -L %s' % (executableScript, inputArguments, inputFiles, base+shortname, condorLogFile, stdLogFiles)

cfg = """universe=grid
grid_resource = condor cmssubmit-r1.t2.ucsd.edu glidein-collector.t2.ucsd.edu
+remote_DESIRED_Sites="T2_US_UCSD" 
executable={exe}
arguments={args}
transfer_executable=True
when_to_transfer_output = ON_EXIT
transfer_input_files={inpfiles}
+Owner = undefined 
log={condorlog}
output={stdlog}/1e.$(Cluster).$(Process).out
error={stdlog}/1e.$(Cluster).$(Process).err
notification=Never
x509userproxy={proxy}
should_transfer_files = yes
queue
""".format(**condorParams)

with open(submitFile, "w") as fhout:
    fhout.write(cfg)


# status, submitOutput = commands.getstatusoutput("condor_submit %s" % submitFile)
# print submitOutput
submitOutput = """
Submitting job(s).
1 job(s) submitted to cluster 312905.
universe=grid
grid_resource = condor cmssubmit-r1.t2.ucsd.edu glidein-collector.t2.ucsd.edu
+remote_DESIRED_Sites="T2_US_UCSD"
executable=/home/users/namin/sandbox/duck/scripts/mergeWrapper.sh
arguments=/hadoop/cms/store/user/namin/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/160220_235603/0000 2,8 1 25000 21000 0.0123 1.1 1.0
transfer_executable=True
when_to_transfer_output = ON_EXIT
transfer_input_files=/home/users/namin/sandbox/duck/scripts/mergeWrapper.sh,/home/users/namin/sandbox/duck/scripts/mergeScript.C,/home/users/namin/sandbox/duck/scripts/addBranches.C
+Owner = undefined
log=/data/tmp/namin/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/+2016.02.21-18.31.40.log
output=/data/tmp/namin/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/std_logs//1e.$(Cluster).$(Process).out
error=/data/tmp/namin/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/std_logs//1e.$(Cluster).$(Process).err
notification=Never
x509userproxy=/tmp/x509up_u31567
should_transfer_files = yes
queue
"""
if " submitted " in submitOutput: 
    print "submitted successfully!"


# print cfg
