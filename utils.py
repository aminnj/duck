import os
import sys
import commands
import pycurl 
import StringIO 
import ast
import datetime
import params

def get(cmd, returnStatus=False):
    status, out = commands.getstatusoutput(cmd)
    if returnStatus: return status, out
    else: return out

def cmd(cmd, returnStatus=False):
    status, out = commands.getstatusoutput(cmd)
    if returnStatus: return status, out
    else: return out

def proxy_hours_left():
    try:
        info = get("voms-proxy-info")
        hours = int(info.split("timeleft")[-1].strip().split(":")[1])
    except: hours = 0
    return hours

def copy_jecs():
    for jec in params.jecs:
        if not os.path.isfile(jec):
            os.system("cp /nfs-7/userdata/JECs/%s ." % jec)

web_dir = "%s/public_html/%s/" % (os.getenv("HOME"), params.dashboard_name)
def make_dashboard():
    if not os.path.isdir(web_dir): os.makedirs(web_dir)
    cmd("chmod 755 -R %s" % web_dir)
    cmd("chmod 755 %s/*.py" % web_dir)
    cmd("cp -rp dashboard/* %s/" % web_dir)
    cmd("sed -i s#BASEDIR_PLACEHOLDER#%s/# %s/main.js" % (os.getcwd(), web_dir))
    print "http://uaf-6.t2.ucsd.edu/~%s/%s" % (os.getenv("USER"), web_dir.split("public_html/")[1])

def copy_json():
    # cmd("cp data.json ~/public_html/autotupletest/")
    cmd("cp data.json %s/" % web_dir)

def read_samples(filename="instructions.txt"):
    samples = []
    with open(filename, "r") as fhin:
        for line in fhin.readlines():
            line = line.strip()
            if len(line) < 5: continue
            if line[0] == "#": continue
            parts = line.strip().split()
            if len(parts) < 5: continue
            dataset, gtag, xsec, kfact, efact = parts[:5]
            sample = { "dataset": dataset, "gtag": gtag, "kfact": float(kfact), "efact": float(efact), "xsec": float(xsec) }
            if len(parts) == 6: sample["sparms"] = parts[5].split(",")
            samples.append(sample)
    return samples

def proxy_renew():
    # http://www.t2.ucsd.edu/tastwiki/bin/view/CMS/LongLivedProxy
    cert_file = "/home/users/{0}/.globus/proxy_for_{0}.file".format(os.getenv("USER"))
    if os.path.exists(cert_file): cmd("voms-proxy-init -q -voms cms -hours 120 -valid=120:0 -cert=%s" % cert_file)
    else: cmd("voms-proxy-init -hours 9876543:0 -out=%s" % cert_file)

def get_proxy_file():
    # cert_file = "/home/users/{0}/.globus/proxy_for_{0}.file".format(os.getenv("USER"))
    cert_file = '/tmp/x509up_u%s' % str(os.getuid()) # TODO: check that this is the same as `voms-proxy-info -path`
    return cert_file

def get_timestamp():
    # return current time as a unix timestamp
    return int(datetime.datetime.now().strftime("%s"))

def dataset_event_count(dataset):
    # 3 hours of work to figure out how the crab dbs api works and get this to work with only `cmsenv`....
    # can't use urllib2 since x509 got supported after 2.7.6
    # can't use requests because that doesn't come with cmsenv
    # btw. api is at https://cmsweb.cern.ch/dbs/prod/global/DBSReader/
    b = StringIO.StringIO() 
    c = pycurl.Curl() 
    url = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/filesummaries?dataset=%s&validFileOnly=1" % dataset
    cert = get_proxy_file()
    c.setopt(pycurl.URL, url) 
    c.setopt(pycurl.WRITEFUNCTION, b.write) 
    c.setopt(pycurl.CAPATH, '/etc/grid-security/certificates') 
    c.unsetopt(pycurl.CAINFO)
    c.setopt(pycurl.SSLCERT, cert)
    c.perform() 
    ret = ast.literal_eval(b.getvalue())
    if len(ret) > 0:
        return { "nevents": ret[0]['num_event'], "filesize": ret[0]['file_size'], "nfiles": ret[0]['num_file'], "nlumis": ret[0]['num_lumi'] }

    return None

def dataset_event_count_2(dataset):
    # cmd(". /cvmfs/cms.cern.ch/crab3/crab-env-bootstrap.sh >& /dev/null")
    from dbs.apis.dbsClient import DbsApi
    url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
    api=DbsApi(url=url)
    output = api.listDatasets(dataset=dataset)

    if(len(output)==1):
        inp = output[0]['dataset']
        info = api.listFileSummaries(dataset=inp)[0]
        filesize = info['file_size']
        nevents = info['num_event']
        nlumis = info['num_lumi']
        files = api.listFiles(dataset=dataset, detail=1, validFileOnly=1)
        return {"nevents": nevents, "filesize": filesize, "nfiles": len(files), "nlumis": nlumis}

    return {}

def get_hadoop_name():
    # handle non-standard hadoop name mapping
    user = os.getenv("USER")
    if user == "dsklein": user = "dklein"
    elif user == "iandyckes": user = "gdyckes"
    elif user == "mderdzinski": user = "mderdzin"
    elif user == "rclsa": user = "rcoelhol"
    return user

if __name__=='__main__':

    # if proxy_hours_left() < 5:
    #     print "Proxy near end of lifetime, renewing."
    #     proxy_renew()
    # else:
    #     print "Proxy looks good"

    print get_proxy_file()

    # print dataset_event_count('/SMS-T5ttcc_mGl-1025to1200_mLSP-0to1025_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15MiniAODv2-FastAsympt25ns_74X_mcRun2_asymptotic_v2-v1/MINIAODSIM')
    # print dataset_event_count('/DYJetsToLL_M-50_Zpt-150toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/MINIAODSIM')
    # print dataset_event_count_2('/DYJetsToLL_M-50_Zpt-150toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/MINIAODSIM')

    # for samp in read_samples():
    #     print samp
    
    # make_dashboard()

