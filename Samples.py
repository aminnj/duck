import os, sys, glob
import datetime, ast, tarfile, pprint

try:
    from WMCore.Configuration import Configuration
    from CRABAPI.RawCommand import crabCommand
    from CRABClient.UserUtilities import setConsoleLogLevel
    from CRABClient.ClientUtilities import LOGLEVEL_MUTE
    from ROOT import TFile, TH1F
except:
    print ">>> Make sure to source setup.sh first!"

import params
import utils as u

class Sample:

    def __init__(self, dataset=None, gtag=None, kfact=None, efact=None, xsec=None):

        # debug bools
        self.fake_submission = False
        self.fake_status = True
        self.fake_crab_done = True
        self.fake_legit_sweeproot = False
        self.fake_miniaod_map = True
        self.fake_merge_lists = True
        self.specialdir_test = True

        # dirs are wrt the base directory where this script is located
        self.pfx_pset = 'pset' # where to hold the psets
        self.pfx_crab = 'crab' # where to keep all crab tasks
        self.sample = {
                "basedir" : os.getcwd()+"/",
                "dataset" : dataset,
                "user" : os.getenv("USER"),
                "cms3tag" : params.cms3tag,
                "gtag" : gtag,
                "kfact" : kfact,
                "efact" : efact,
                "xsec" : xsec,
                "sparms": [], # always keep as list. e.g., ["mlsp","mstop"]
                "isdata": False, # by default, MC
                "pset": "", # *_cfg.py pset location
                "specialdir": "", # /hadoop/cms/store/group/snt/{specialdir}/ (e.g., run2_25ns, run2_fastsim)
                "finaldir": "", # where final files will live
                "status" : "new", # general sample status
                "crab": { }, # crab task information here
                "postprocessing": { }, # postprocessing counts for monitor
                "ijob_to_miniaod": { }, # map from ijob to list of miniaod
                "imerged_to_ijob": { }, # map from imerged to iunmerged
                "ijob_to_nevents": { }, # map from ijob to (nevents, nevents_eff)
                }
        self.sample["shortname"] = dataset.split("/")[1]+"_"+dataset.split("/")[2]
        self.sample["shortname"] = dataset.split("/")[1]+"_"+dataset.split("/")[2]

        self.sample["crab"]["requestname"] = self.sample["shortname"][:99] # damn crab has size limit for name
        self.sample["crab"]["outputdir"] = None
        self.sample["crab"]["taskdir"] = self.pfx_crab+"/crab_"+self.sample["crab"]["requestname"]
        self.sample["crab"]["datetime"] = None # "160220_151313" from crab request name

        self.crab_config = None
        self.crab_status_res = { }

        self.rootfiles = []
        self.logfiles = []

        self.handled_more_than_1k = False

        self.pfx = self.sample["shortname"][:17] + "..."
        setConsoleLogLevel(LOGLEVEL_MUTE)

        self.set_sample_specifics()

    def __getitem__(self, i): return self.sample[i]

    def __setitem__(self, k, v): self.sample[k] = v

    def __str__(self):
        buff  = "[%s] %s: %s\n" % (self.pfx, self.sample["status"], self.sample["dataset"])
        buff += "[%s]   cms3tag, gtag = %s, %s\n" % (self.pfx, self.sample["cms3tag"], self.sample["gtag"])
        buff += "[%s]   xsec, kfactor, eff = %.4f, %.2f, %.2f\n" % (self.pfx, self.sample["xsec"], self.sample["kfact"], self.sample["efact"])
        buff += "[%s]   shortname = %s\n" % (self.pfx, self.sample["shortname"])
        buff += "[%s]   requestname = %s\n" % (self.pfx, self.sample["crab"]["requestname"])
        buff += "[%s]   pset = %s\n" % (self.pfx, self.sample["pset"])

        if self.sample["crab"]:
            buff += "[%s]   CRAB status %s for %i jobs using schedd %s\n" \
                    % (self.pfx, self.sample["crab"]["status"], self.sample["crab"]["njobs"], self.sample["crab"]["schedd"])
            buff += "[%s]   Output dir: %s\n" % (self.pfx, self.sample["crab"]["outputdir"])
            for cstat, num in self.sample["crab"]["breakdown"].items():
                if num == 0: continue
                buff += "[%s]     %s: %i\n" % (self.pfx, cstat, num)
        return buff

    def do_log(self, text):
        print "[%s] %s" % (self.pfx, text)


    def set_sample_specifics(self):
        ds = self.sample["dataset"]

        # figure out pset automatically
        if ds.endswith("SIM"): self.sample["pset"] = params.pset_mc
        if ds.startswith("/SMS"): self.sample["pset"] = params.pset_mc_fastsim
        if len(self.sample["sparms"]) > 0: self.sample["pset"] = params.pset_mc_fastsim
        if "FSPremix" in ds: self.sample["pset"] = params.pset_mc_fastsim
        if "FastAsympt" in ds: self.sample["pset"] = params.pset_mc_fastsim
        if self.sample["isdata"]: self.sample["pset"] = params.pset_data

        # figure out specialdir automatically
        if self.specialdir_test:
            self.sample["specialdir"] = "test"
        else:

            if "50ns" in ds: self.sample["specialdir"] = "run2_50ns"
            elif "RunIISpring15MiniAODv2" in ds: self.sample["specialdir"] = "run2_fastsim"
            elif "RunIISpring15FSPremix" in ds: self.sample["specialdir"] = "run2_fastsim"
            elif "RunIISpring15MiniAODv2" in ds: self.sample["specialdir"] = "run2_25ns_MiniAODv2"
            elif "25ns" in ds: self.sample["specialdir"] = "run2_25ns"


        self.sample["finaldir"] = "/hadoop/cms/store/group/snt/%s/%s/%s/" \
                % (self.sample["specialdir"], self.sample["shortname"], self.sample["cms3tag"].split("_")[-1])

    def make_crab_config(self):
        if self.crab_config is not None: 
            self.do_log("crab config already made, not remaking")
            return

        config = Configuration()
        config.section_('General')
        config.General.workArea = self.pfx_crab # all crab output goes into crab/
        config.General.transferOutputs = True
        config.General.transferLogs = True
        config.General.requestName = self.sample["crab"]["requestname"]
        config.section_('JobType')
        config.JobType.inputFiles = params.jecs
        config.JobType.pluginName = 'Analysis'
        config.JobType.psetName = "%s/%s_cfg.py" % (self.pfx_pset, self.sample["shortname"])
        config.section_('Data')
        config.Data.allowNonValidInputDataset = True
        config.Data.inputDataset = self.sample["dataset"]
        config.Data.unitsPerJob = 1
        config.Data.splitting = 'FileBased'
        config.Data.inputDBS = "phys03" if self.sample["dataset"].endswith("/USER") else "global"
        config.section_('Site')
        config.Site.storageSite = 'T2_US_UCSD'
        self.crab_config = config

    
    def make_pset(self):
        if not os.path.isdir(self.pfx_pset): os.makedirs(self.pfx_pset)

        pset_in_fname = params.cmssw_ver+"/src/CMS3/NtupleMaker/test/"+self.sample["pset"]
        pset_out_fname = "%s/%s_cfg.py" % (self.pfx_pset, self.sample["shortname"])

        if os.path.isfile(pset_out_fname): 
            self.do_log("pset already made, not remaking")
            return

        if not os.path.isfile(pset_in_fname):
            self.do_log("skeleton pset %s does not exist!" % (pset_in_fname))
            return

        newlines = []
        with open(pset_in_fname, "r") as fhin:
            lines = fhin.readlines()
            newlines.append("import sys, os\n")
            newlines.append("sys.path.append(os.getenv('CMSSW_BASE')+'/src/CMS3/NtupleMaker/test')\n\n")
            for iline, line in enumerate(lines):
                if line.strip().startswith("fileName") and "process.out" in lines[iline-1]:
                    line = line.split("(")[0]+"('ntuple.root'),\n"
                elif ".GlobalTag." in line: line = line.split("=")[0]+" = '"+self.sample["gtag"]+"'\n"
                elif ".reportEvery" in line: line = line.split("=")[0]+" = 1000\n"
                elif ".eventMaker.datasetName." in line: line = line.split("(")[0]+"('%s')\n" % self.sample["dataset"]
                elif "cms.Path" in line:
                    newlines.append( "process.eventMaker.datasetName = cms.string(\"%s\")\n" % self.sample["dataset"] )
                    newlines.append( "process.eventMaker.CMS3tag = cms.string(\"%s\")\n\n" % self.sample["cms3tag"] )

                newlines.append(line)
                
            sparms = self.sample["sparms"]
            if len(sparms) > 0:
                sparms = list(set(map(lambda x: x.strip(), sparms)))
                sparms = ['"%s"' % sp for sp in sparms]
                newlines.append('process.sParmMaker.vsparms = cms.untracked.vstring(' + ",".join(sparms) + ')\n')
                newlines.append('process.p.insert( -1, process.sParmMakerSequence )\n')

        with open(pset_out_fname, "w") as fhout:
            fhout.write( "".join(newlines) )
            self.do_log("made pset %s!" % (pset_out_fname))


    def copy_jecs(self):
        for jec in params.jecs:
            if not os.path.isfile(jec):
                os.system("cp /nfs-7/userdata/JECs/%s ." % jec)


    def crab_kill(self):
        try:
            out = crabCommand('kill', dir=self.sample["crab"]["taskdir"], proxy=u.get_proxy_file())
        except Exception as e:
            self.dolog("ERROR killing:",e)
            return 0
        return out["status"] == "SUCCESS"


    def crab_delete_dir(self):
        self.do_log("deleting %s" % (self.sample["crab"]["taskdir"]))
        self.do_log("deleting pset: %s/%s_cfg.py" % (self.pfx_pset, self.sample["shortname"]))
        os.system("rm -rf %s" % self.sample["crab"]["taskdir"])
        os.system("rm %s/%s_cfg.py" % (self.pfx_pset, self.sample["shortname"]))


    def crab_submit(self):
        # first try to see if the job already exists naively
        if "uniquerequestname" in self.sample["crab"]:
            self.do_log("already submitted crab jobs")
            return 1

        # more robust check
        crablog = "%s/crab.log" % self.sample["crab"]["taskdir"]
        if os.path.isfile(crablog):
            taskline = u.get("/bin/grep 'Success' -A 1 -m 1 %s | /bin/grep 'Task name'" % crablog)
            uniquerequestname = taskline.split("Task name:")[1].strip()
            self.sample["crab"]["uniquerequestname"] = uniquerequestname
            self.sample["crab"]["datetime"] = uniquerequestname.split(":")[0].strip()
            self.do_log("already submitted crab jobs")
            return 1

        try:
            if self.fake_submission:
                out = {'uniquerequestname': '160222_012655:namin_crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1', 'requestname': 'crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1'}
            else:
                out = crabCommand('submit', config = self.crab_config, proxy=u.get_proxy_file())

            datetime = out["uniquerequestname"].split(":")[0]
            self.sample["crab"]["uniquerequestname"] = out["uniquerequestname"]
            self.sample["crab"]["datetime"] = datetime
            self.do_log("submitted jobs. uniquerequestname: %s" % (out["uniquerequestname"]))
            return 1 # succeeded
        except Exception as e:
            self.dolog("ERROR submitting:",e)
            return 0 # failed


    def crab_status(self):
        try:
            if self.fake_status:
                # out = {'ASOURL': 'https://cmsweb.cern.ch/couchdb2', 'collector': 'cmssrv221.fnal.gov,vocms099.cern.ch', 'failedJobdefs': 0,
                #      'jobList': [['running', 1], ['running', 3], ['running', 2], ['running', 5], ['running', 4], ['running', 7], ['idle', 6], ['running', 8]], 'jobdefErrors': [],
                #      'jobs': {'1': {'State': 'running'}, '2': {'State': 'running'}, '3': {'State': 'running'}, '4': {'State': 'running'},
                #               '5': {'State': 'running'}, '6': {'State': 'idle'}, '7': {'State': 'running'}, '8': {'State': 'running'}},
                #      'jobsPerStatus': {'idle': 1, 'running': 7}, 'outdatasets': None, 'publication': {}, 'publicationFailures': {}, 'schedd': 'crab3-1@submit-5.t2.ucsd.edu',
                #      'status': 'SUBMITTED', 'statusFailureMsg': '', 'taskFailureMsg': '', 'taskWarningMsg': [], 'totalJobdefs': 0} 
                out = {'ASOURL': 'https://cmsweb.cern.ch/couchdb2', 'collector': 'cmssrv221.fnal.gov,vocms099.cern.ch', 'failedJobdefs': 0,
                     'jobList': [['finished', 1], ['finished', 3], ['finished', 2], ['finished', 5], ['finished', 4], ['finished', 7], ['finished', 6], ['finished', 8]], 'jobdefErrors': [],
                     'jobs': {'1': {'State': 'finished'}, '2': {'State': 'finished'}, '3': {'State': 'finished'}, '4': {'State': 'finished'},
                              '5': {'State': 'finished'}, '6': {'State': 'finished'}, '7': {'State': 'finished'}, '8': {'State': 'finished'}},
                     'jobsPerStatus': {'finished': 8}, 'outdatasets': None, 'publication': {}, 'publicationFailures': {}, 'schedd': 'crab3-1@submit-5.t2.ucsd.edu',
                     'status': 'COMPLETED', 'statusFailureMsg': '', 'taskFailureMsg': '', 'taskWarningMsg': [], 'totalJobdefs': 0} 
            else:
                out = crabCommand('status', dir=self.sample["crab"]["taskdir"], proxy=u.get_proxy_file())
            self.crab_status_res = out
            return 1 # succeeded
        except Exception as e:
            self.dolog("ERROR getting status:",e)
            return 0 # failed


    def crab_parse_status(self):
        stat = self.crab_status_res
        try:
            d_crab = {
                "status": stat.get("status"),
                "commonerror": None,
                "schedd": stat.get("schedd"),
                "njobs": len(stat["jobs"]),
                "time": int(datetime.datetime.now().strftime("%s")),
                "breakdown": {
                    "unsubmitted": 0, "idle": 0, "running": 0, "failed": 0,
                    "transferring": 0, "transferred": 0, "cooloff": 0, "finished": 0,
                }
            }
            self.sample["status"] = "crab"
        except:
            # must be the case that not all this info exists because it was recently submitted
            self.do_log("can't get status right now (is probably too new)")
            return

        # population of each status (running, failed, etc.)
        for status,jobs in stat["jobsPerStatus"].items():
            d_crab["breakdown"][status] = jobs

        # find most common error (if exists)
        error_codes, details = [], []
        for job in stat["jobs"].values():
            if "Error" in job.keys():
                error_codes.append(job["Error"][0])
                details.append(job["Error"][2]["details"])
        if len(error_codes) > 0 and len(details) > 0:
            most_common_error_code = max(set(error_codes), key=error_codes.count)
            count = error_codes.count(most_common_error_code)
            most_common_detail = details[error_codes.index(most_common_error_code)]
            d_crab["commonerror"] = "%i jobs (%.1f%%) failed with error code %s: %s" \
                    % (count, 100.0*count/d_crab["njobs"], most_common_error_code, most_common_detail)

        # extend the crab dict with these new values we just got
        for k in d_crab:
            self.sample["crab"][k] = d_crab[k]


    def handle_more_than_1k(self):
        if self.handled_more_than_1k: return

        output_dir = self.sample["crab"]["outputdir"]
        without_zeros = self.sample["crab"]["outputdir"].replace("0000","")

        for kilobatch in os.listdir(without_zeros):
            if kilobatch == "0000": continue
            u.cmd("mv {0}/{1}/*.root {0}/{2}/".format(without_zeros, kilobatch, "0000"))
            u.cmd("mv {0}/{1}/log/* {0}/{2}/log/".format(without_zeros, kilobatch, "0000"))

        self.do_log("copied files from .../*/ to .../0000/")
        self.handled_more_than_1k = True


    def is_crab_done(self):

        self.sample["crab"]["outputdir"] = "/hadoop/cms/store/user/%s/%s/crab_%s/%s/0000/" \
                % (self.sample["user"], self.sample["dataset"].split("/")[1], self.sample["crab"]["requestname"], self.sample["crab"]["datetime"])


        if self.fake_crab_done: return True
        if "status" not in self.sample["crab"] or self.sample["crab"]["status"] != "COMPLETED": return False

        self.handle_more_than_1k()


        njobs = self.sample["crab"]["njobs"]
        self.rootfiles = glob.glob(self.sample["crab"]["outputdir"] + "/*.root")
        self.logfiles = glob.glob(self.sample["crab"]["outputdir"] + "/log/*.tar.gz")
        if njobs == len(self.rootfiles) and njobs == len(self.logfiles):
            return True

        self.do_log("ERROR: crab says COMPLETED but not all files are there")
        self.do_log("# jobs, # root files, # log files = " % (njobs, len(rootfiles), len(logfiles)))
        return False


    def make_miniaod_map(self):
        if self.fake_miniaod_map:
            self.sample["ijob_to_miniaod"] = {
                1: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/065D3D09-CA6D-E511-A59C-D4AE526A0461.root'],
                2: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/20FF3B81-C96D-E511-AAB8-441EA17344AC.root'],
                3: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/2EF6C807-CA6D-E511-A9EE-842B2B758AD8.root'],
                4: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/30FDAF08-CA6D-E511-828C-D4AE526A0C7A.root'],
                5: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/4A1B071A-CA6D-E511-8D8E-441EA1733FD6.root'],
                6: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/90D98A05-CA6D-E511-B721-00266CFFBDB4.root'],
                7: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/A8935398-C96D-E511-86FA-1CC1DE18CFF0.root'],
                8: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/E80D9307-CA6D-E511-A3A7-003048FFCB96.root'],
            }
            return

        if not self.sample["ijob_to_miniaod"]:
            self.do_log("making map from unmerged number to miniaod name")
            for logfile in self.logfiles:
                with  tarfile.open(logfile, "r:gz") as tar:
                    for member in tar:
                        if "FrameworkJobReport" not in member.name: continue
                        jobnum = int(member.name.split("-")[1].split(".xml")[0])
                        fh = tar.extractfile(member)
                        lines = [line for line in fh.readlines() if "<PFN>" in line and "/store/" in line]
                        miniaod = list(set(map(lambda x: "/store/"+x.split("</PFN>")[0].split("/store/")[1], lines)))
                        self.sample["ijob_to_miniaod"][jobnum] = miniaod
                        fh.close()
                        break


    def get_rootfile_info(self, fname):
        if self.fake_legit_sweeproot: return (False, 1000, 900, 2.0)

        f = TFile.Open(fname,"READ")
        treename = "Events"

        if not f or f.IsZombie(): return (True, 0, 0, 0)

        tree = f.Get(treename)
        n_entries = tree.GetEntriesFast()
        if n_entries == 0: return (True, 0, 0, 0)

        pos_weight = tree.Draw("1", "genps_weight>0")
        neg_weight = n_entries - pos_weight
        n_entries_eff = pos_weight - neg_weight

        h_pfmet = TH1F("h_pfmet", "h_pfmet", 100, 0, 1000);
        tree.Draw("evt_pfmet >> h_pfmet")
        avg_pfmet = h_pfmet.GetMean()
        if avg_pfmet < 0.01 or avg_pfmet > 10000: return (True, 0, 0, 0)

        return (False, n_entries, n_entries_eff, f.GetSize()/1.0e9)


    def make_merging_chunks(self):
        if self.fake_merge_lists:
            self.sample['ijob_to_nevents'] = { 1: [43079L, 36953L], 2: [14400L, 12304L],
                                              3: [43400L, 37116L], 4: [29642L, 25430L],
                                              5: [48479L, 41261L], 6: [18800L, 16156L],
                                              7: [42000L, 35928L], 8: [10200L, 8702L] }
            self.sample['imerged_to_ijob'] = {1: [1, 2, 3, 4], 2: [5, 6, 7, 8]}
            return

        self.sample["status"] = "mergelists"
        if not self.sample["imerged_to_ijob"]: 
            self.do_log("making map from merged index to unmerged indicies")
            group, groups = [], []
            tot_size = 0.0
            for rfile in self.rootfiles:
                is_bad, nevents, nevents_eff, file_size = self.get_rootfile_info(rfile)
                ijob = int(rfile.split("_")[-1].replace(".root",""))
                self.sample["ijob_to_nevents"][ijob] = [nevents, nevents_eff]
                if is_bad: continue
                tot_size += file_size
                group.append(ijob)
                if tot_size >= 5.0: # in GB!
                    groups.append(group)
                    group = []
                    tot_size = 0.0
            if len(group) > 0: groups.append(group) # finish up last group
            for igp,gp in enumerate(groups):
                self.sample["imerged_to_ijob"][igp+1] = gp


    def get_condor_running(self):
        output = u.get("condor_q $USER -autoformat CMD ARGS")
        # output = """
        # /home/users/namin/sandbox/duck/scripts/mergeWrapper.sh /hadoop/cms/store/user/namin/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/160220_235603/0000 2,8 1 25000 21000 0.0123 1.1 1.0
        # /home/users/namin/sandbox/duck/scripts/mergeWrapper.sh /hadoop/cms/store/user/namin/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/160220_235603/0000 2,8 2 25000 21000 0.0123 1.1 1.0
        # /home/users/namin/sandbox/duck/scripts/mergeWrapper.sh /hadoop/cms/store/user/namin/TT_TuneCUETP8M1_13TeV-amcatnlo-pythia8/crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/160220_235603/0000 2,8 4 25000 21000 0.0123 1.1 1.0
        # """
        running_condor_set = set()
        for line in output.split("\n"):
            if "mergeWrapper" not in line: continue
            _, unmerged_dir, _, merged_index = line.split(" ")[:4]
            requestname = unmerged_dir.split("crab_")[1].split("/")[0]

            if self.sample["crab"]["requestname"] == requestname:
                running_condor_set.add(int(merged_index))

        # return set of merged indices
        return running_condor_set


    def get_merged_done(self):
        files = os.listdir(self.sample["crab"]["outputdir"]+"/merged/")
        # return set of merged indices
        return set(map(lambda x: int(x.split("_")[-1].split(".")[0]), files))


    def is_merging_done(self):
        # want 0 running condor jobs and all merged files in output area
        return len(self.get_condor_running()) == 0 and len(self.get_merged_done()) == len(self.sample["imerged_to_ijob"].keys())


    def submit_merge_jobs(self):
        working_dir = self.sample["basedir"]
        shortname = self.sample["shortname"]
        unmerged_dir = self.sample["crab"]["outputdir"]
        xsec = self.sample["xsec"]
        kfactor = self.sample["kfact"]
        efactor = self.sample["efact"]

        submit_file = self.sample["crab"]["taskdir"]+"/submit.cmd"
        executable_script = working_dir+"/scripts/mergeWrapper.sh"
        merge_script = working_dir+"/scripts/mergeScript.C"
        addbranches_script = working_dir+"/scripts/addBranches.C"
        proxy_file = u.get("find /tmp/x509up_u* -user $USER").strip()
        condor_log_files = "/data/tmp/%s/%s/%s.log" % (self.sample["user"],shortname,datetime.datetime.now().strftime("+%Y.%m.%d-%H.%M.%S"))
        std_log_files = "/data/tmp/%s/%s/std_logs/" % (self.sample["user"],shortname)
        input_files = ",".join([executable_script, merge_script, addbranches_script])

        condor_params = {
                "exe": executable_script,
                "inpfiles": input_files,
                "condorlog": condor_log_files,
                "stdlog": std_log_files,
                "proxy": proxy_file,
                }

        cfg_format = "universe=grid \n" \
                     "grid_resource = condor cmssubmit-r1.t2.ucsd.edu glidein-collector.t2.ucsd.edu \n" \
                     "+remote_DESIRED_Sites=\"T2_US_UCSD\" \n" \
                     "executable={exe} \n" \
                     "arguments={args} \n" \
                     "transfer_executable=True \n" \
                     "when_to_transfer_output = ON_EXIT \n" \
                     "transfer_input_files={inpfiles} \n" \
                     "+Owner = undefined  \n" \
                     "log={condorlog} \n" \
                     "output={stdlog}/1e.$(Cluster).$(Process).out \n" \
                     "error={stdlog}/1e.$(Cluster).$(Process).err \n" \
                     "notification=Never \n" \
                     "x509userproxy={proxy} \n" \
                     "should_transfer_files = yes \n" \
                     "queue \n" 

        # don't resubmit the ones that are already running or done
        imerged_set = set(self.sample['imerged_to_ijob'].keys())
        processing_set = self.get_condor_running()
        done_set = self.get_merged_done()
        imerged_list = list( imerged_set - processing_set - done_set ) 

        self.sample["status"] = "postprocessing"
        self.sample["postprocessing"]["total"] = len(imerged_set)
        self.sample["postprocessing"]["running"] = len(processing_set)
        self.sample["postprocessing"]["done"] = len(done_set)
        self.sample["postprocessing"]["tosubmit"] = len(imerged_list)

        if len(imerged_list) > 0:
            self.do_log("submitting %i merge jobs" % len(imerged_list))

        for imerged in imerged_list:
            input_indices=",".join(map(str,self.sample['imerged_to_ijob'][imerged]))
            nevents_both = [self.sample['ijob_to_nevents'][i] for i in self.sample['imerged_to_ijob'][imerged]]
            nevents = sum([x[0] for x in nevents_both])
            nevents_effective = sum([x[1] for x in nevents_both])

            input_arguments = " ".join(map(str,[unmerged_dir, input_indices, imerged, nevents, nevents_effective, xsec, kfactor, efactor]))
            condor_params["args"] = input_arguments

            cfg = cfg_format.format(**condor_params)
            with open(submit_file, "w") as fhout:
                fhout.write(cfg)

            submit_output = u.get("condor_submit %s" % submit_file)

            if " submitted " in submit_output: 
                self.do_log("job for merged_ntuple_%i.root submitted successfully" % imerged)

if __name__=='__main__':

    s = Sample( **{
              "dataset": "/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/MINIAODSIM",
              "gtag": "74X_mcRun2_asymptotic_v2",
              "kfact": 1.0,
              "efact": 1.0,
              "xsec": 0.0234,
              } )

    # flowchart:
    # === status: created
    # 0) renew proxy
    # 1) copy jecs, make crab config, make pset
    #
    # === status: crab
    # 2) submit crab jobs and get status
    # 3) keep getting status until is_crab_done
    #
    # ===> status: mergelists
    # 4) make miniaod map, make merging chunks
    #
    # === status: postprocessing
    # 5) submit merging jobs
    # 6) check merge output and re-submit outstanding jobs until all done
    # 7) checkCMS3
    # 8) make meta data
    #
    # === status: copying
    # 9) copy to final resting place
    #
    # === status: done

    # if u.proxy_hours_left() < 5:
    #     print "Proxy near end of lifetime, renewing."
    #     u.proxy_renew()
    # else:
    #     print "Proxy looks good"

    s.copy_jecs()
    s.make_crab_config()
    s.make_pset()

    s.crab_submit()
    if s.crab_status():
        s.crab_parse_status()

    if s.is_crab_done():
        pass

        # s.make_miniaod_map()
        # s.make_merging_chunks()
        # s.submit_merge_jobs()

    # print s.is_merging_done()

    pprint.pprint( s.sample )
