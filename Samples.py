import os, sys, glob, select
import datetime, ast, tarfile, pprint
import pickle, json

try:
    from WMCore.Configuration import Configuration
    from CRABAPI.RawCommand import crabCommand
    from CRABClient.UserUtilities import setConsoleLogLevel
    from CRABClient.ClientUtilities import LOGLEVEL_MUTE
    # I recommend putting `Root.ErrorIgnoreLevel: Error` in your .rootrc file
    from ROOT import TFile, TH1F, TChain
except:
    print ">>> Make sure to source setup.sh first!"
    sys.exit()

import params
import utils as u

class Sample:

    def __init__(self, dataset=None, gtag=None, kfact=None, efact=None, 
                 xsec=None, sparms=[], debug=False, specialdir_test=False,
                 do_skip_tail=True,logger_callback=None):

        setConsoleLogLevel(LOGLEVEL_MUTE)

        # debug bools
        if debug:
            self.fake_submission = False
            self.fake_status = True
            self.fake_crab_done = True
            self.fake_legit_sweeproot = True
            self.fake_miniaod_map = True
            self.fake_merge_lists = True
            self.fake_check = True
            self.fake_copy = True
        else:
            self.fake_submission = False
            self.fake_status = False
            self.fake_crab_done = False
            self.fake_legit_sweeproot = False
            self.fake_miniaod_map = False
            self.fake_merge_lists = False
            self.fake_check = False
            self.fake_copy = False

        self.specialdir_test = specialdir_test
        self.do_skip_tail = do_skip_tail

        # dirs are wrt the base directory where this script is located

        self.misc = {}
        self.misc["pfx_pset"] = 'pset' # where to hold the psets
        self.misc["pfx_crab"] = 'crab' # where to keep all crab tasks
        self.misc["crab_config"] = None
        self.misc["handled_more_than_1k"] = False
        self.misc["rootfiles"] = []
        self.misc["logfiles"] = []
        self.misc["last_saved"] = None # when was the last time we backed up this sample data
        self.misc["can_skip_tail"] = False
        # self.misc["handled_prechecks"] = False
        # self.misc["passed_prechecks"] = True

        self.sample = {
                "basedir" : "",
                "dataset" : dataset,
                "shortname": dataset.split("/")[1]+"_"+dataset.split("/")[2],
                "user" : u.get_hadoop_name(),
                "cms3tag" : params.cms3tag,
                "cmsswver" : params.cmssw_ver,
                "gtag" : gtag,
                "kfact" : kfact,
                "efact" : efact,
                "xsec" : xsec,
                "sparms": sparms, # always keep as list. e.g., ["mlsp","mstop"]
                "isdata": False, # by default, MC
                "pset": "", # *_cfg.py pset location
                "specialdir": "", # /hadoop/cms/store/group/snt/{specialdir}/ (e.g., run2_25ns, run2_fastsim)
                "finaldir": "", # where final files will live
                "status" : "new", # general sample status
                "crab": { }, # crab task information here
                "postprocessing": { }, # postprocessing counts for monitor
                "checks": { }, # checkCMS3 info for monitor
                "ijob_to_miniaod": { }, # map from ijob to list of miniaod
                "imerged_to_ijob": { }, # map from imerged to iunmerged
                "ijob_to_nevents": { }, # map from ijob to (nevents, nevents_eff)
                "nevents_DAS": 0,
                "nevents_unmerged": 0,
                "nevents_merged": 0,
                }

        self.sample["crab"]["requestname"] = self.sample["shortname"][:99] # damn crab has size limit for name
        self.sample["crab"]["outputdir"] = None
        self.sample["crab"]["taskdir"] = self.misc["pfx_crab"]+"/crab_"+self.sample["crab"]["requestname"]
        self.sample["crab"]["datetime"] = None # "160220_151313" from crab request name
        self.sample["crab"]["resubmissions"] = 0 # number of times we've "successfully" resubmitted a crab job
        self.sample["crab"]["jobs_left"] = [] # keep track of job ids that are not done
        self.sample["crab"]["jobs_left_tail"] = [] # keep track of job ids that are taking forever (in the tail)

        self.logger_callback = None

        self.crab_status_res = { }


        self.set_sample_specifics()

        self.load() # load backup of this sample when we instantiate it



    def __getitem__(self, i):
        return self.sample[i]


    def __setitem__(self, k, v):
        self.sample[k] = v
    

    def __eq__(self, other):
        return "dataset" in other and other["dataset"] == self.sample["dataset"]


    def __str__(self):
        buff  = "[%s] %s: %s\n" % (self.pfx, self.sample["status"], self.sample["dataset"])
        buff += "[%s]   cms3tag, gtag = %s, %s\n" % (self.pfx, self.sample["cms3tag"], self.sample["gtag"])
        buff += "[%s]   xsec, kfactor, eff = %.4f, %.2f, %.2f\n" % (self.pfx, self.sample["xsec"], self.sample["kfact"], self.sample["efact"])
        buff += "[%s]   shortname = %s\n" % (self.pfx, self.sample["shortname"])
        buff += "[%s]   requestname = %s\n" % (self.pfx, self.sample["crab"]["requestname"])
        buff += "[%s]   pset = %s\n" % (self.pfx, self.sample["pset"])

        if "status" in self.sample["crab"]:
            buff += "[%s]   CRAB status %s for %i jobs using schedd %s\n" \
                    % (self.pfx, self.sample["crab"]["status"], self.sample["crab"]["njobs"], self.sample["crab"]["schedd"])
            buff += "[%s]   Output dir: %s\n" % (self.pfx, self.sample["crab"]["outputdir"])
            for cstat, num in self.sample["crab"]["breakdown"].items():
                if num == 0: continue
                buff += "[%s]     %s: %i\n" % (self.pfx, cstat, num)
        return buff


    def get_slimmed_dict(self):
        new_dict = self.sample.copy()
        del new_dict["imerged_to_ijob"]
        del new_dict["ijob_to_miniaod"]
        del new_dict["ijob_to_nevents"]
        if "jobs_left" in new_dict["crab"]:
            del new_dict["crab"]["jobs_left"]
        return new_dict


    def get_status(self):
        return self.sample["status"]

    def add_logger_callback(self, logger_callback):
        self.logger_callback = logger_callback

    def do_log(self, text):
        print "[%s] [%s] %s" % (datetime.datetime.now().strftime("%H:%M:%S"), self.pfx, text)
        if self.logger_callback is not None:
            self.logger_callback(datetime.datetime.now().strftime("%H:%M:%S"), self.pfx, text)

    def save(self):
        backup_file = self.sample["crab"]["taskdir"]+"/backup.pkl"
        self.misc["last_saved"] = u.get_timestamp()
        d_tot = {"sample": self.sample, "misc": self.misc}
        with open(backup_file,"w") as fhout:
            pickle.dump(d_tot, fhout)
        # self.do_log("successfully backed up to %s" % backup_file)
        # self.do_log("successfully backed up")

    def load(self):
        backup_file = self.sample["crab"]["taskdir"]+"/backup.pkl"
        if os.path.isfile(backup_file):
            with open(backup_file,"r") as fhin:
                d_tot = pickle.load(fhin)

            for key in d_tot["sample"].keys(): self.sample[key] = d_tot["sample"][key]
            for key in d_tot["misc"].keys(): self.misc[key] = d_tot["misc"][key]
            last_saved = self.misc["last_saved"]
            if last_saved:
                min_ago = round((u.get_timestamp() - last_saved) / 60.0)
                # self.do_log("successfully loaded %s which was last saved %i minutes ago" % (backup_file, min_ago))
                self.do_log("successfully loaded backup (last saved %i minutes ago)" % min_ago)
            else:
                self.do_log("successfully loaded %s" % (backup_file))
        else:
            self.do_log("can't load. probably a new sample.")


    def set_sample_specifics(self):
        ds = self.sample["dataset"]

        # figure out pset automatically
        if ds.endswith("SIM"): self.sample["pset"] = params.pset_mc
        if len(self.sample["sparms"]) > 0: self.sample["pset"] = params.pset_mc_fastsim
        if "FSPremix" in ds: self.sample["pset"] = params.pset_mc_fastsim
        if "FastAsympt" in ds: self.sample["pset"] = params.pset_mc_fastsim
        if "/Run2015" in ds: self.sample["pset"] = params.pset_data
        if "/Run2016" in ds: self.sample["pset"] = params.pset_data
        if self.sample["isdata"]: self.sample["pset"] = params.pset_data

        # figure out specialdir automatically
        if "50ns" in ds: self.sample["specialdir"] = "run2_50ns"
        elif "RunIISpring15MiniAODv2-FastAsympt25ns" in ds: self.sample["specialdir"] = "run2_fastsim"
        elif "RunIISpring15FSPremix" in ds: self.sample["specialdir"] = "run2_fastsim"
        elif "RunIISpring15MiniAODv2" in ds: self.sample["specialdir"] = "run2_25ns_MiniAODv2"
        elif "25ns" in ds: self.sample["specialdir"] = "run2_25ns"

        if self.specialdir_test:
            self.sample["specialdir"] = "test"

        self.sample["basedir"] = os.getcwd()+"/"
        self.sample["finaldir"] = "/hadoop/cms/store/group/snt/%s/%s/%s/" \
                % (self.sample["specialdir"], self.sample["shortname"], self.sample["cms3tag"].split("_",1)[1])
        self.pfx = self.sample["shortname"][:20] + "..."


    def update_params(self, d):
        for param in ["xsec", "kfact", "efact", "sparms"]:
            if param in d and d[param] and not(d[param] == self.sample[param]):
                self.do_log("found a new value of %s: %s. (old value: %s). updating." \
                        % (param, d[param], self.sample[param]) )


    def make_crab_config(self):
        if self.misc["crab_config"] is not None: 
            self.do_log("crab config already made, not remaking")
            return

        config = Configuration()
        config.section_('General')
        config.General.workArea = self.misc["pfx_crab"] # all crab output goes into crab/
        config.General.transferOutputs = True
        config.General.transferLogs = True
        config.General.requestName = self.sample["crab"]["requestname"]
        config.section_('JobType')
        config.JobType.inputFiles = params.jecs
        config.JobType.pluginName = 'Analysis'
        config.JobType.psetName = "%s/%s_cfg.py" % (self.misc["pfx_pset"], self.sample["shortname"])
        config.section_('Data')
        config.Data.allowNonValidInputDataset = True
        config.Data.publication = False
        config.Data.inputDataset = self.sample["dataset"]
        config.Data.unitsPerJob = 1
        config.Data.ignoreLocality = True
        config.Data.splitting = 'FileBased'
        config.Data.inputDBS = "phys03" if self.sample["dataset"].endswith("/USER") else "global"
        config.section_('User')
        config.section_('Site')
        config.Site.storageSite = 'T2_US_UCSD'
        config.Site.whitelist = ['T2_US_*']
        self.misc["crab_config"] = config
    
    def make_pset(self):
        if not os.path.isdir(self.misc["pfx_pset"]): os.makedirs(self.misc["pfx_pset"])

        pset_in_fname = params.cmssw_ver+"/src/CMS3/NtupleMaker/test/"+self.sample["pset"]
        pset_out_fname = "%s/%s_cfg.py" % (self.misc["pfx_pset"], self.sample["shortname"])

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


    def crab_kill(self):
        try:
            out = crabCommand('kill', dir=self.sample["crab"]["taskdir"], proxy=u.get_proxy_file())
        except Exception as e:
            self.do_log("ERROR killing: "+str(e))
            return 0
        return out["status"] == "SUCCESS"


    def crab_delete_dir(self):
        self.do_log("deleting %s" % (self.sample["crab"]["taskdir"]))
        self.do_log("deleting pset: %s/%s_cfg.py" % (self.misc["pfx_pset"], self.sample["shortname"]))
        os.system("rm -rf %s" % self.sample["crab"]["taskdir"])
        os.system("rm %s/%s_cfg.py" % (self.misc["pfx_pset"], self.sample["shortname"]))

    def nuke(self):
        self.crab_kill()
        self.crab_delete_dir()

    def crab_submit(self):
        # first try to see if the job already exists naively
        if "uniquerequestname" in self.sample["crab"]:
            self.do_log("already submitted crab jobs")
            self.sample["status"] = "crab"
            return 1

        # more robust check
        crablog = "%s/crab.log" % self.sample["crab"]["taskdir"]
        if os.path.isfile(crablog):
            try:
                taskline = u.get("/bin/grep 'Success' -A 1 -m 1 %s | /bin/grep 'Task name'" % crablog)
                uniquerequestname = taskline.split("Task name:")[1].strip()
                self.sample["crab"]["uniquerequestname"] = uniquerequestname
                self.sample["crab"]["datetime"] = uniquerequestname.split(":")[0].strip()
                self.do_log("already submitted crab jobs")
                self.sample["status"] = "crab"
                return 1
            except: pass

        try: 
            self.sample["nevents_DAS"] = u.dataset_event_count(self.sample["dataset"])["nevents"]
            self.do_log("sample has %i events according to DAS/DBS" % self.sample["nevents_DAS"])
        except: pass

        try:
            if self.fake_submission:
                out = {'uniquerequestname': '160222_073351:namin_crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1', 'requestname': 'crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1'}
            else:
                if not self.misc["crab_config"]: self.make_crab_config()
                self.make_pset()
                # # out = crabCommand('submit', config = self.misc["crab_config"], proxy=u.get_proxy_file())
                # gotta do this BS instead of the above because stupid crab didn't fix their issue
                # https://hypernews.cern.ch/HyperNews/CMS/get/computing-tools/1191/1/1/1.html
                from multiprocessing import Queue, Process
                q = Queue()
                def submit(q,config,proxy):
                    out = crabCommand('submit', config=config, proxy=proxy)
                    q.put(out)
                p = Process(target=submit, args=(q, self.misc["crab_config"], u.get_proxy_file()))
                p.start()
                p.join()
                out = q.get()


            dtstr = out["uniquerequestname"].split(":")[0]
            self.sample["crab"]["uniquerequestname"] = out["uniquerequestname"]
            self.sample["crab"]["datetime"] = dtstr
            self.do_log("submitted jobs. uniquerequestname: %s" % (out["uniquerequestname"]))
            self.sample["status"] = "crab"
            return 1 # succeeded
        except Exception as e:
            self.do_log("ERROR submitting: "+str(e))
            return 0 # failed


    def crab_status(self):

        if self.sample["nevents_DAS"] == 0:
            self.sample["nevents_DAS"] = u.dataset_event_count(self.sample["dataset"])["nevents"]

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
                out = crabCommand('status', dir=self.sample["crab"]["taskdir"], proxy=u.get_proxy_file(), json=True)
            self.crab_status_res = out
            return 1 # succeeded
        except Exception as e:
            self.do_log("ERROR getting status: "+str(e))
            return 0 # failed

    def crab_resubmit(self):
        try:
            out = crabCommand('resubmit', dir=self.sample["crab"]["taskdir"], proxy=u.get_proxy_file())
            return out["status"] == "SUCCESS"
        except Exception as e:
            self.do_log("ERROR resubmitting "+str(e))
            return 0 # failed

    def minutes_since_crab_submit(self):
        # minutes since the crab task was created
        dtstr = self.sample["crab"]["datetime"]
        then = datetime.datetime.strptime(dtstr, "%y%m%d_%H%M%S")
        now = datetime.datetime.now()
        return (then-now).seconds / 60.0

    def crab_parse_status(self):
        self.crab_status()
        stat = self.crab_status_res
        # print stat

        try:
            self.sample["crab"]["status"] = stat.get("status")
            self.sample["crab"]["task_failure"] = stat.get("taskFailureMsg")
            self.sample["crab"]["task_warning"] = stat.get("taskWarningMsg")
            self.sample["crab"]["status_failure"] = stat.get("statusFailureMsg")
            self.sample["crab"]["commonerror"] = None
            self.sample["crab"]["time"] = u.get_timestamp()
            self.sample["crab"]["schedd"] = stat.get("schedd")
            self.sample["crab"]["njobs"] = len(stat["jobList"])
            self.sample["crab"]["breakdown"] = {
                "unsubmitted": 0, "idle": 0, "running": 0, "failed": 0,
                "transferring": 0, "transferred": 0, "cooloff": 0, "finished": 0,
            }
        except Exception as e:
            # must be the case that not all this info exists because it was recently submitted
            self.do_log("can't get status right now (is probably too new): "+str(e))
            return

        if self.sample["crab"]["status"] == "FAILED":
            if self.crab_resubmit():
                self.sample["crab"]["resubmissions"] += 1

        if self.sample["crab"]["status"] == "SUBMITTED" and "taskWarningMsg" in stat:
            warning = stat["taskWarningMsg"]
            if len(warning) > 0 and "not yet bootstrapped" in warning[0]:
                mins = self.minutes_since_crab_submit()
                self.do_log("task has not bootstrapped yet, and it's been %i minutes" % mins)
                if mins > 300: # resubmit if been more than 5 hours
                    self.do_log("been more than 5 hours, so trying to resubmit")
                    self.crab_resubmit()

        # population of each status (running, failed, etc.)
        if "jobsPerStatus" in stat:
            for status,jobs in stat["jobsPerStatus"].items():
                self.sample["crab"]["breakdown"][status] = jobs

        if self.sample["crab"]["breakdown"]["finished"] > 0:
            done_frac = 1.0*self.sample["crab"]["njobs"]/self.sample["crab"]["breakdown"]["finished"]
        else: 
            done_frac = 0.0

        self.sample["crab"]["jobs_left"] = []
        self.sample["crab"]["jobs_left_tail"] = []
        if "jobs" in stat and "jobList" in stat:
            for status, ijob in stat["jobList"]:
                if not(status == "finished"):
                    # now look up job in the "jobs" dictionary. example of job_info below:
                    # {'Retries': 5, 'WallDurations': [3367.0, 13520.0, 15821.0, 8811.0, 10528.0, 1040.0], 'StartTimes':
                    # [1457345940.0, 1457381466.0, 1457396962.0, 1457415108.0, 1457436816.0, 1457449310.0], 'SubmitTimes':
                    # [1457345368.0, 1457381344.0, 1457396763.0, 1457414693.0, 1457436538.0, 1457448807.0], 'JobIds': ['7158216.0',
                    # '7188315.0', '7190921.0', '7198820.0', '7209773.0', '7217653.0'], 'EndTimes': [1457349306.0, 1457394751.0,
                    # 1457412626.0, 1457423672.0, 1457446619.0], 'Restarts': 0, 'RecordedSite': True, 'State': 'running',
                    # 'ResidentSetSize': [1207312, 1239416, 1258848, 1195008, 1254328, 1267400], 'TotalUserCpuTimeHistory': [2500,
                    # 12158, 14706, 7816, 8965, 1014.0], 'SiteHistory': ['T2_US_Purdue', 'T2_US_Vanderbilt', 'T2_US_Florida',
                    # 'T2_US_Vanderbilt', 'T2_US_Vanderbilt', 'T2_US_Nebraska'], 'TotalSysCpuTimeHistory': [73, 168, 286, 104, 144, 23.0]}
                    job_info = stat["jobs"][str(ijob)]
                    avg_walltime = 1.0*sum(job_info['WallDurations'])/len(job_info['WallDurations'])
                    state, nretries = job_info['State'], job_info['Retries']
                    # print ">>>> job %i (%s) has been retried %i times with an average walltime of %.1f" \
                    #         % (ijob, state, nretries, avg_walltime)
                    # print "done frac: %.1f" % done_frac

                    self.sample["crab"]["jobs_left"].append(ijob)

                    if nretries > 3 and done_frac > 0.97:
                        self.sample["crab"]["jobs_left_tail"].append(ijob)

        # print self.sample["crab"]["jobs_left"]
        # print self.sample["crab"]["jobs_left_tail"]
        if self.do_skip_tail and self.sample["crab"]["jobs_left"] == self.sample["crab"]["jobs_left_tail"] and (self.sample["crab"]["breakdown"]["finished"] > 0):
            # this means that all crab jobs left are jobs in the tail, so let's ignore them and forge onwards with merging
            self.do_log("there are %i tail jobs left that we will ignore from now on" % len(self.sample["crab"]["jobs_left_tail"]))
            self.misc["can_skip_tail"] = True
            self.sample["crab"]["status"] = "COMPLETED"


        # find most common error (if exists)
        error_codes, details = [], []
        most_common_detail = "n/a"
        if "jobs" in stat:
            for job in stat["jobs"].values():
                if "Error" in job.keys():
                    error_codes.append(job["Error"][0])
                    try:
                        details.append(job["Error"][2]["details"])
                    except: 
                        if len(job["Error"]) > 2: details.append(job["Error"][1])

        
        if len(details) > 0:
            most_common_detail = max(set(details), key=details.count)

        if len(error_codes) > 0:
            most_common_error_code = max(set(error_codes), key=error_codes.count)
            count = error_codes.count(most_common_error_code)

            self.sample["crab"]["commonerror"] = "%i jobs (%.1f%%) failed with error code %s: %s" \
                    % (count, 100.0*count/self.sample["crab"]["njobs"], most_common_error_code, most_common_detail)


    def handle_more_than_1k(self):
        if self.misc["handled_more_than_1k"]: return

        output_dir = self.sample["crab"]["outputdir"]
        without_zeros = self.sample["crab"]["outputdir"].replace("0000","")

        for kilobatch in os.listdir(without_zeros):
            if kilobatch == "0000": continue
            u.cmd("mv {0}/{1}/*.root {0}/{2}/".format(without_zeros, kilobatch, "0000"))
            u.cmd("mv {0}/{1}/log/* {0}/{2}/log/".format(without_zeros, kilobatch, "0000"))

        self.do_log("copied files from .../*/ to .../0000/")
        self.misc["handled_more_than_1k"] = True


    def is_crab_done(self):

        self.sample["crab"]["outputdir"] = "/hadoop/cms/store/user/%s/%s/crab_%s/%s/0000/" \
                % (self.sample["user"], self.sample["dataset"].split("/")[1], self.sample["crab"]["requestname"], self.sample["crab"]["datetime"])

        if self.fake_crab_done: return True
        if "status" not in self.sample["crab"] or self.sample["crab"]["status"] != "COMPLETED": return False

        # print "here"

        self.handle_more_than_1k()

        def get_num(fname): return int(fname.split("_")[-1].split(".")[0])

        njobs = self.sample["crab"]["njobs"]
        self.misc["rootfiles"] = glob.glob(self.sample["crab"]["outputdir"] + "/*.root")
        self.misc["logfiles"] = glob.glob(self.sample["crab"]["outputdir"] + "/log/*.tar.gz")

        if self.do_skip_tail and self.misc["can_skip_tail"]:
            self.do_log("pruning the ignored tail files from rootfiles and logfiles")
            self.misc["rootfiles"] = [fname for fname in self.misc["rootfiles"] if get_num(fname) not in self.sample["crab"]["jobs_left_tail"]]
            self.misc["logfiles"] = [fname for fname in self.misc["logfiles"] if get_num(fname) not in self.sample["crab"]["jobs_left_tail"]]
            njobs -= len(self.sample["crab"]["jobs_left_tail"])

        if njobs == len(self.misc["rootfiles"]) and not(njobs == len(self.misc["logfiles"])):
            # we have all the root files, but evidently some log files got lost. try to recover them
            # format: ntuple_1.root and cmsRun_1.log.tar.gz
            root_file_numbers = set([get_num(rfile) for rfile in self.misc["rootfiles"]])
            log_file_numbers = set([get_num(lfile) for lfile in self.misc["logfiles"]])
            log_dont_have = list(root_file_numbers - log_file_numbers)
            if len(log_dont_have) > 0:
                jobids = ",".join(map(str, log_dont_have))
                self.do_log("all rootfiles exist, but not all logfiles are there (missing %s), so recovering with crab getlog --short" % jobids)
                try: out = crabCommand('getlog', dir=self.sample["crab"]["taskdir"], short=True, proxy=u.get_proxy_file(), jobids=jobids)
                except: pass
                textlogs = glob.glob(self.sample["crab"]["taskdir"]+"/results/job_out*.txt") 
                textlogs = [log for log in textlogs if int(log.split("job_out.")[1].split(".")[0]) in log_dont_have]
                if len(textlogs) > 0:
                    self.do_log("got %i of 'em" % len(textlogs))
                    self.misc["logfiles"].extend(textlogs)

        if njobs == len(self.misc["rootfiles"]) and njobs == len(self.misc["logfiles"]):
            return True

        self.do_log("ERROR: crab says COMPLETED but not all files are there, even after getlog")
        self.do_log("# jobs, # root files, # log files = %i, %i, %i" % (njobs, len(self.misc["rootfiles"]), len(self.misc["logfiles"])))
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

        # print "ijob_to_miniaod", self.sample["ijob_to_miniaod"]
        if not self.sample["ijob_to_miniaod"]:
            self.do_log("making map from unmerged number to miniaod name")
            for logfile in self.misc["logfiles"]:
                # print logfile
                if ".tar.gz" in logfile:
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
                elif ".txt" in logfile:
                    # parse the recovered txt files if .tar.gz didn't stageout
                    with open(logfile, "r") as fh:
                        # job_out.7.0.txt
                        jobnum = int(logfile.split("job_out.")[1].split(".")[0])
                        lines = [line for line in fh.readlines() if "Initiating request to open file" in line]
                        miniaod = list(set(map(lambda x: "/store/"+x.split("/store/")[1].split(".root")[0]+".root", lines)))
                        self.sample["ijob_to_miniaod"][jobnum] = miniaod


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

    def get_events_in_chain(self, fname_wildcard):
        ch = TChain("Events")
        ch.Add(fname_wildcard)
        return ch.GetEntries()


    def make_merging_chunks(self):
        if self.fake_merge_lists:
            self.sample['ijob_to_nevents'] = { 1: [43079L, 36953L], 2: [14400L, 12304L],
                                              3: [43400L, 37116L], 4: [29642L, 25430L],
                                              5: [48479L, 41261L], 6: [18800L, 16156L],
                                              7: [42000L, 35928L], 8: [10200L, 8702L] }
            self.sample['imerged_to_ijob'] = {1: [1, 2, 3, 4], 2: [5, 6, 7, 8]}
            return

        # print "imerged_to_ijob", self.sample["imerged_to_ijob"]
        if not self.sample["imerged_to_ijob"]: 
            self.do_log("making map from merged index to unmerged indicies")
            group, groups = [], []
            tot_size = 0.0
            for rfile in self.misc["rootfiles"]:
                is_bad, nevents, nevents_eff, file_size = self.get_rootfile_info(rfile)
                # print is_bad, nevents, nevents_eff, file_size, rfile
                ijob = int(rfile.split("_")[-1].replace(".root",""))
                self.sample["ijob_to_nevents"][ijob] = [nevents, nevents_eff]
                if is_bad: continue
                tot_size += file_size
                group.append(ijob)
                if tot_size >= 4.7: # in GB!
                    groups.append(group)
                    group = []
                    tot_size = 0.0
            if len(group) > 0: groups.append(group) # finish up last group
            for igp,gp in enumerate(groups):
                self.sample["imerged_to_ijob"][igp+1] = gp

            self.sample['nevents_unmerged'] = sum([x[0] for x in self.sample['ijob_to_nevents'].values()])


    def get_condor_running(self):
        # return set of merged indices
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

        return running_condor_set


    def get_merged_done(self):
        # return set of merged indices
        merged_dir = self.sample["crab"]["outputdir"]+"/merged/"
        if not os.path.isdir(merged_dir): return set()
        files = os.listdir(merged_dir)
        files = [f for f in files if f.endswith(".root")]
        return set(map(lambda x: int(x.split("_")[-1].split(".")[0]), files))


    def pass_tsa_prechecks(self):
        # if we already did this sample, clearly it passes prechecks
        if self.sample["status"] == "done":
            return True

        # if self.misc["handled_prechecks"]:
        #     return self.misc["passed_prechecks"]

        # check is sample has already been done
        final_dir = self.sample["finaldir"]
        # print final_dir
        is_done = False
        if os.path.isdir(final_dir):
            files = [f for f in os.listdir(final_dir) if f.endswith(".root")]
            if len(files) > 0: is_done = True

        if is_done:
            self.do_log("NOTE: this sample is already in the final group area. move it to another folder if you want to remake it. skipping for now.")
            return False


        return True

        # self.do_log("hey! it looks like this sample already exists in the final hadoop directory.")
        # self.do_log("do you want to remake? [y/n] if you don't answer in 10 seconds, will assume no.")
        # i, o, e = select.select( [sys.stdin], [], [], 10 )
        # if i:
        #     inp = sys.stdin.readline().strip()
        #     if "y" in inp.lower():
        #         self.do_log("ok, will continue and remake it")
        # else:
        #     self.do_log("you ignored me. will not remake. sample will be put in 'done' status now")
        #     self.sample["status"] = "done"

        # self.misc["handled_prechecks"] = True

    def is_merging_done(self):
        # want 0 running condor jobs and all merged files in output area
        done = len(self.get_condor_running()) == 0 and len(self.get_merged_done()) == len(self.sample["imerged_to_ijob"].keys())
        if done:
            self.sample["postprocessing"]["running"] = 0
            self.sample["postprocessing"]["done"] = self.sample["postprocessing"]["total"]
            self.sample["postprocessing"]["tosubmit"] = 0

        return done


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
        condor_log_files = "/data/tmp/%s/%s/%s.log" % (os.getenv("USER"),shortname,datetime.datetime.now().strftime("+%Y.%m.%d-%H.%M.%S"))
        std_log_files = "/data/tmp/%s/%s/std_logs/" % (os.getenv("USER"),shortname)
        input_files = ",".join([executable_script, merge_script, addbranches_script])
        nevents_both = self.sample['ijob_to_nevents'].values()
        nevents = sum([x[0] for x in nevents_both])
        nevents_effective = sum([x[1] for x in nevents_both])

        if not os.path.isdir(std_log_files): os.makedirs(std_log_files)

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
        # subtract running jobs from done. we might think they're done if they begin
        # to stageout, but they're not yet done staging out
        done_set = self.get_merged_done() - processing_set
        imerged_list = list( imerged_set - processing_set - done_set ) 

        self.sample["postprocessing"]["total"] = len(imerged_set)
        self.sample["postprocessing"]["running"] = len(processing_set)
        self.sample["postprocessing"]["done"] = len(done_set)
        self.sample["postprocessing"]["tosubmit"] = len(imerged_list)

        if len(imerged_list) > 0:
            self.sample["status"] = "postprocessing"
            self.do_log("submitting %i merge jobs" % len(imerged_list))

        for imerged in imerged_list:
            input_indices=",".join(map(str,self.sample['imerged_to_ijob'][imerged]))

            input_arguments = " ".join(map(str,[unmerged_dir, input_indices, imerged, nevents, nevents_effective, xsec, kfactor, efactor]))
            condor_params["args"] = input_arguments

            cfg = cfg_format.format(**condor_params)
            with open(submit_file, "w") as fhout:
                fhout.write(cfg)

            submit_output = u.get("condor_submit %s" % submit_file)

            if " submitted " in submit_output: 
                self.do_log("job for merged_ntuple_%i.root submitted successfully" % imerged)

    
    def make_metadata(self):
        metadata_file = self.sample["crab"]["taskdir"]+"/metadata.txt"
        metadata_file_json = metadata_file.replace(".txt",".json")
        with open(metadata_file, "w") as fhout:
            print >>fhout,"sampleName: %s" % self.sample["dataset"]
            print >>fhout,"xsec: %s" % self.sample["xsec"]
            print >>fhout,"k-fact: %s" % self.sample["kfact"]
            print >>fhout,"e-fact: %s" % self.sample["efact"]
            print >>fhout,"cms3tag: %s" % self.sample["cms3tag"]
            print >>fhout,"gtag: %s" % self.sample["gtag"]
            print >>fhout,"sparms: %s" % (",".join(self.sample["sparms"]) if self.sample["sparms"] else "_")
            print >>fhout, ""
            print >>fhout,"unmerged files are in: %s" % self.sample["crab"]["outputdir"]
            print >>fhout, ""
            for ijob in sorted(self.sample["ijob_to_miniaod"]):
                print >>fhout, "unmerged %i %s" % (ijob, ",".join(self.sample["ijob_to_miniaod"][ijob]))
            print >>fhout, ""
            for imerged in sorted(self.sample["imerged_to_ijob"]):
                print >>fhout, "merged file constituents %i: %s" % (imerged, " ".join(map(str,self.sample["imerged_to_ijob"][imerged])))
            print >>fhout, ""
            for imerged in sorted(self.sample["imerged_to_ijob"]):
                nevents_both = [self.sample["ijob_to_nevents"][ijob] for ijob in self.sample["imerged_to_ijob"][imerged]]
                nevents = sum([x[0] for x in nevents_both])
                nevents_effective = sum([x[1] for x in nevents_both])
                print >>fhout, "merged file nevents %i: %i %i" % (imerged, nevents, nevents_effective)

        d_tot = self.sample.copy()
        with open(metadata_file_json, "w") as fhout:
            json.dump(d_tot, fhout, sort_keys = True, indent = 4)

        # mirror the central snt directory structure for metadata files
        metadatabank_dir = "/nfs-7/userdata/metadataBank/%s/%s/%s/" \
                % (self.sample["specialdir"], self.sample["shortname"], self.sample["cms3tag"].split("_",1)[1])

        # copy txt to merged and backup. copy json to backup only
        u.cmd('chmod a+w %s %s' % (metadata_file, metadata_file_json))
        u.cmd("cp %s %s/" % (metadata_file, self.sample["crab"]["outputdir"]+"/merged/"))
        u.cmd('mkdir -p {0} ; chmod a+w {0}'.format(metadatabank_dir))
        u.cmd('cp %s %s %s/' % (metadata_file, metadata_file_json, metadatabank_dir))

        self.do_log("made metadata and copied it to merged and backup areas")

    def copy_files(self):
        self.do_log("started copying files to %s" % self.sample["finaldir"])
        if self.fake_copy:
            print "Will do: mv %s/merged/* to %s/" % (self.sample["crab"]["outputdir"], self.sample["finaldir"])
        else:
            u.cmd("mkdir -p %s/" % self.sample["finaldir"])
            u.cmd( "mv %s/merged/* to %s/" % (self.sample["crab"]["outputdir"], self.sample["finaldir"]) )
        self.do_log("finished copying files")

        if self.get_events_in_chain(self.sample["finaldir"]+"/*.root") == self.sample['nevents_merged']:
            # if finaldir doesn't have nevents_merged, must've been a mv error, so redo merging and mv again
            self.sample["status"] = "done"
        else:
            self.do_log("lost some events after moving into final directory. re-merging now.")
            self.submit_merge_jobs()


    def check_output(self):
        if self.fake_check:
            problems = []
            tot_problems = 0
        else:
            output_dir = self.sample["crab"]["outputdir"]
            cmd = """( cd scripts; root -n -b -q -l "checkCMS3.C(\\"{0}/merged\\", \\"{0}\\", 0,0)"; )""".format(output_dir)
            # print cmd
            self.do_log("started running checkCMS3")
            out = u.get(cmd)
            self.do_log("finished running checkCMS3")

            # out = """
            # ERROR!                Inconsistent scale1fb!
            # =============== RESULTS =========================
            # Total problems found: 1
            # """

            lines = out.split("\n")
            problems = []
            tot_problems = -1
            for line in lines:
                if "ERROR!" in line: problems.append(line.replace("ERROR!","").strip())
                elif "Total problems found:" in line: tot_problems = int(line.split(":")[1].strip())

        self.do_log("found %i total problems:" % tot_problems)
        for prob in problems:
            self.do_log("-- %s" % prob)

        # if skipping tail, of course we will have problem with event mismatch, so subtract it out
        if self.do_skip_tail and tot_problems > 0:
            tot_problems -= 1

        self.sample["checks"]["nproblems"] = tot_problems
        self.sample["checks"]["problems"] = problems
        self.sample['nevents_merged'] = self.sample['nevents_unmerged'] if tot_problems == 0 else 0

        self.handle_sample_problems()

        return tot_problems == 0

    def handle_sample_problems(self):
        problems = self.sample["checks"]["problems"]
        merged_dir = self.sample["crab"]["outputdir"]+"/merged/"

        for problem in problems:
            if "Wrong event count" in problem:
                # delete this imerged
                if not self.do_skip_tail:
                    imerged = int(problem.split(".root")[0].split("_")[-1])
                    u.cmd("rm %s/merged_ntuple_%i.root" % (merged_dir, imerged))
                    self.submit_merge_jobs()
                else:
                    # FIXME be smart about event counts? or is there no way to get event counts
                    # until crab has finished? but that defeats purpose of do_skip_tail
                    pass
            elif "events with zeros in" in problem:
                # delete all merged and remerge
                u.cmd("rm %s/merged_ntuple_*.root" % (merged_dir))
                self.submit_merge_jobs()
            elif "DAS query failed" in problem:
                # probably transient, ignore and let check() try again later
                pass


if __name__=='__main__':


    # flowchart:
    # === status: created
    # 0) renew proxy
    # 1) copy jecs, make crab config, make pset
    #
    # === status: crab
    # 2) submit crab jobs and get status
    # 3) keep getting status until is_crab_done
    #
    # === status: postprocessing
    # 4) make miniaod map, make merging chunks
    # 5) submit merging jobs
    # 6) check merge output and re-submit outstanding jobs until all done
    # 7) checkCMS3
    # 8) make meta data
    # 9) copy to final resting place
    #
    # === status: done

    s = Sample( **{
              "dataset": "/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/MINIAODSIM",
              "gtag": "74X_mcRun2_asymptotic_v2",
              "kfact": 1.0,
              "efact": 1.0,
              "xsec": 0.0234,
              "debug": True,
              "specialdir_test": False,
              } )

    if u.proxy_hours_left() < 5:
        print "Proxy near end of lifetime, renewing."
        u.proxy_renew()
    else:
        print "Proxy looks good"

    u.copy_jecs()

    # s.handle_tsa_prechecks()

    s.crab_submit()

    s.crab_parse_status()

    if s.is_crab_done():

        s.make_miniaod_map()
        s.make_merging_chunks()
        s.submit_merge_jobs()

    if s.is_merging_done():
        s.make_metadata()
        if s.check_output():
            s.copy_files()

    s.save()

    pprint.pprint(s.get_slimmed_dict())

    # pprint.pprint( s.sample )
