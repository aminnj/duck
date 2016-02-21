import os
import sys
import commands

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


def proxy_renew():
    # http://www.t2.ucsd.edu/tastwiki/bin/view/CMS/LongLivedProxy
    cert_file = "/home/users/{0}/.globus/proxy_for_{0}.file".format(os.getenv("USER"))
    if os.path.exists(cert_file): cmd("voms-proxy-init -q -voms cms -hours 120 -valid=120:0 -cert=%s" % cert_file)
    else: cmd("voms-proxy-init -hours 9876543:0 -out=%s" % cert_file)

def get_proxy_file():
    cert_file = "/home/users/{0}/.globus/proxy_for_{0}.file".format(os.getenv("USER"))
    return cert_file

def dataset_event_count(dataset):
    from dbs.apis.dbsClient import DbsApi
    url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
    api=DbsApi(url=url)
    output = api.listDatasets(dataset=dataset)

    if(len(output)==1):
        inp = output[0]['dataset']
        info = api.listFileSummaries(dataset=inp)[0]
        filesize = info['file_size']/1e9 # GB
        nevents = info['num_event']
        nlumis = info['num_lumi']
        files = api.listFiles(dataset=dataset, detail=1, validFileOnly=1)
        bad_nevents = sum(file["event_count"] for file in files if not file["is_file_valid"])
        return {"nevents": nevents, "filesize": filesize, "nfiles": len(files), "nlumis": nlumis, "bad_nevents": bad_nevents}

    return None

if __name__=='__main__':

    # if proxy_hours_left() < 5:
    #     print "Proxy near end of lifetime, renewing."
    #     proxy_renew()
    # else:
    #     print "Proxy looks good"

    print dataset_event_count('/DYJetsToLL_M-50_Zpt-150toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/MINIAODSIM')
    

