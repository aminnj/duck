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

        self.fake_submission = True
        self.fake_status = True
        self.fake_crab_done = False
        self.fake_legit_sweeproot = True
        self.fake_miniaod_map = True

        self.pfx_pset = './pset/'
        self.pfx_crab = './crab/'

        # dirs are wrt the base directory where this script is located
        self.sample = {
                "dataset" : dataset,
                "user" : os.getenv("USER"),
                "cms3tag" : params.cms3tag,
                "gtag" : gtag,
                "kfact" : kfact,
                "efact" : efact,
                "xsec" : xsec,
                "sparms": [], # always keep as list
                "pset": "", # *_cfg.py pset location
                "status" : "created", # general sample status
                "datetime" : None, # "160220_151313" from crab request name
                "crab": { }, # crab task information here
                "ijob_to_miniaod": { }, # map from ijob to list of miniaod
                "imerged_to_ijob": { }, # map from imerged to iunmerged
                "ijob_to_nevents": { }, # map from ijob to (nevents, nevents_eff)
                }
        self.sample["shortname"] = dataset.split("/")[1]+"_"+dataset.split("/")[2]
        self.sample["requestname"] = self.sample["shortname"][:99] # damn crab has size limit for name

        self.sample["crab"]["outputdir"] = None
        self.sample["crab"]["taskdir"] = self.pfx_crab+"crab_"+self.sample["requestname"]

        self.crab_config = None
        self.crab_status_res = { }

        self.rootfiles = []
        self.logfiles = []


        self.pfx = self.sample["shortname"][:10]
        setConsoleLogLevel(LOGLEVEL_MUTE)

    def __getitem__(self, i): return self.sample[i]

    def __setitem__(self, k, v): self.sample[k] = v

    def __str__(self):
        buff  = "[%s] %s: %s\n" % (self.pfx, self.sample["status"], self.sample["dataset"])
        buff += "[%s]   cms3tag, gtag = %s, %s\n" % (self.pfx, self.sample["cms3tag"], self.sample["gtag"])
        buff += "[%s]   xsec, kfactor, eff = %.4f, %.2f, %.2f\n" % (self.pfx, self.sample["xsec"], self.sample["kfact"], self.sample["efact"])
        buff += "[%s]   shortname = %s\n" % (self.pfx, self.sample["shortname"])
        buff += "[%s]   requestname = %s\n" % (self.pfx, self.sample["requestname"])
        buff += "[%s]   pset = %s\n" % (self.pfx, self.sample["pset"])

        if self.sample["crab"]:
            buff += "[%s]   CRAB status %s for %i jobs using schedd %s\n" \
                    % (self.pfx, self.sample["crab"]["status"], self.sample["crab"]["njobs"], self.sample["crab"]["schedd"])
            buff += "[%s]   Output dir: %s\n" % (self.pfx, self.sample["crab"]["outputdir"])
            for cstat, num in self.sample["crab"]["breakdown"].items():
                if num == 0: continue
                buff += "[%s]     %s: %i\n" % (self.pfx, cstat, num)
        return buff

    def make_crab_config(self):
        if self.crab_config is not None: 
            print "[%s] crab config already made, not remaking" % self.pfx
            return

        config = Configuration()
        config.section_('General')
        config.General.workArea = self.pfx_crab # all crab output goes into crab/
        config.General.transferOutputs = True
        config.General.transferLogs = True
        config.General.requestName = self.sample["requestname"]
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
            print "[%s] pset already made, not remaking" % self.pfx
            return
        if not os.path.isfile(pset_in_fname):
            print "[%s] skeleton pset %s does not exist!" % (self.pfx, pset_in_fname)
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
                elif ".eventMaker.CMS3tag." in line: line = line.split("(")[0]+"('%s')\n" % self.sample["cms3tag"]
                newlines.append(line)
                
            sparms = self.sample["sparms"]
            if len(sparms) > 0:
                sparms = list(set(map(lambda x: x.strip(), sparms)))
                sparms = ['"%s"' % sp for sp in sparms]
                newlines.append('process.sParmMaker.vsparms = cms.untracked.vstring(' + ",".join(sparms) + ')\n')
                newlines.append('process.p.insert( -1, process.sParmMakerSequence )\n')

        with open(pset_out_fname, "w") as fhout:
            fhout.write( "".join(newlines) )

    def copy_jecs(self):
        for jec in params.jecs:
            if not os.path.isfile(jec):
                os.system("cp /nfs-7/userdata/JECs/%s ." % jec)

    def crab_kill(self):
        try:
            out = crabCommand('kill', dir=self.sample["crab"]["taskdir"], proxy=u.get_proxy_file())
        except Exception as e:
            print "[%s] ERROR killing:" % self.pfx, e
            return 0
        return out["status"] == "SUCCESS"

    def crab_delete_dir(self):
        print "Deleting %s" % self.sample["crab"]["taskdir"]
        os.system("rm -rf %s" % self.sample["crab"]["taskdir"])

    def crab_submit(self):
        try:
            if self.fake_submission:
                out = {'uniquerequestname': '160220_081846:namin_crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1', 'requestname': 'crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1'}
                # out = {'uniquerequestname': '160220_235605:namin_crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1', 'requestname': 'crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1'}
            else:
                out = crabCommand('submit', config = self.crab_config, proxy=u.get_proxy_file())
            datetime = out["uniquerequestname"].split(":")[0]
            self.sample["datetime"] = datetime
            # FIXME deal with files in .../0001/ and so on
            self.sample["crab"]["outputdir"] = "/hadoop/cms/store/user/%s/%s/crab_%s/%s/0000/" \
                    % (self.sample["user"], self.sample["dataset"].split("/")[1], self.sample["requestname"], datetime)
            return 1 # succeeded
        except Exception as e:
            print "[%s] ERROR submitting:" % self.pfx, e
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
            print "[%s] ERROR getting status:" % self.pfx, e
            return 0 # failed

    def crab_parse_status(self):
        stat = self.crab_status_res
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

    def crab_is_done(self):
        if self.fake_crab_done: return True
        if not self.sample["crab"]["status"] == "COMPLETED": return False
        njobs = self.sample["crab"]["njobs"]
        self.rootfiles = glob.glob(self.sample["crab"]["outputdir"] + "/*.root")
        self.logfiles = glob.glob(self.sample["crab"]["outputdir"] + "/log/*.tar.gz")
        if njobs == len(self.rootfiles) and njobs == len(self.logfiles):
            return True

        print "[%s] ERROR: crab says COMPLETED but not all files are there:" % (self.pfx)
        print "[%s] # jobs, # root files, # log files = " % (self.pfx, njobs, len(rootfiles), len(logfiles))
        return False

    def make_miniaod_map(self):
        if not self.sample["ijob_to_miniaod"]:
            if self.fake_miniaod_map:
                self.sample["ijob_to_miniaod"] = {
                    1: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/065D3D09-CA6D-E511-A59C-D4AE526A0461.root'],
                    2: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/20FF3B81-C96D-E511-AAB8-441EA17344AC.root'],
                    3: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/2EF6C807-CA6D-E511-A9EE-842B2B758AD8.root'],
                    4: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/30FDAF08-CA6D-E511-828C-D4AE526A0C7A.root'],
                    5: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/4A1B071A-CA6D-E511-8D8E-441EA1733FD6.root'],
                    6: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/90D98A05-CA6D-E511-B721-00266CFFBDB4.root'],
                    7: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/A8935398-C96D-E511-86FA-1CC1DE18CFF0.root'],
                    8: ['/store/mc/RunIISpring15MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/E80D9307-CA6D-E511-A3A7-003048FFCB96.root']
                }
            else:
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
        if not self.sample["imerged_to_ijob"]: 
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


if __name__=='__main__':
    stuff = {
              "dataset": "/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/MINIAODSIM",
              "gtag": "74X_mcRun2_asymptotic_v2",
              "kfact": 1.0,
              "efact": 1.0,
              "xsec": 0.0123,
              }

    s = Sample(**stuff)
    # s["sparms"] = ["mlsp","mstop "]
    s["pset"] = params.pset_mc # FIXME figure out which one automatically

    s.copy_jecs()
    s.make_crab_config()
    s.make_pset()

    s.crab_submit()
    if s.crab_status():
        s.crab_parse_status()

    if s.crab_is_done():
        s.make_miniaod_map()

    s.make_merging_chunks()

    print s["imerged_to_ijob"]
    print s["ijob_to_nevents"]

    pprint.pprint( s.sample )
