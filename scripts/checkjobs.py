import datetime
import commands
import os



def get_condor_running():
    # status, output = commands.getstatusoutput("condor_q $USER -autoformat CMD ARGS")
    output = """
    /home/users/namin/sandbox/duck/scripts/mergeWrapper.sh /hadoop/cms/store/user/namin/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/160220_235603/0000 2,8 1 25000 21000 0.0123 1.1 1.0
    /home/users/namin/sandbox/duck/scripts/mergeWrapper.sh /hadoop/cms/store/user/namin/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/160220_235603/0000 2,8 2 25000 21000 0.0123 1.1 1.0
    /home/users/namin/sandbox/duck/scripts/mergeWrapper.sh /hadoop/cms/store/user/namin/TT_TuneCUETP8M1_13TeV-amcatnlo-pythia8/crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/160220_235603/0000 2,8 4 25000 21000 0.0123 1.1 1.0
    """
    therequestname = "crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1"



    running_condor_set = set()
    for line in output.split("\n"):
        if "mergeWrapper" not in line: continue
        _, unmerged_dir, _, merged_index = line.split(" ")[:4]
        requestname = "crab_"+unmerged_dir.split("crab_")[1].split("/")[0]

        if requestname.replace("/", "") == therequestname.replace("/", ""):
            running_condor_set.add(merged_index)

    print running_condor_set
