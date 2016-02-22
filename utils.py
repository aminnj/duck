import os
import sys
import commands
import pycurl 
import StringIO 
import ast

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
    # 3 hours of work to figure out how the crab dbs api works and get this to work with only `cmsenv`....
    # can't use urllib2 since x509 got supported after 2.7.6
    # can't use requests because that doesn't come with cmsenv
    # btw. api is at https://cmsweb.cern.ch/dbs/prod/global/DBSReader/
    b = StringIO.StringIO() 
    c = pycurl.Curl() 
    url = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/filesummaries?dataset=%s&validFileOnly=1" % dataset
    cert = '/tmp/x509up_u%s' % str(os.getuid())
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

if __name__=='__main__':

    # if proxy_hours_left() < 5:
    #     print "Proxy near end of lifetime, renewing."
    #     proxy_renew()
    # else:
    #     print "Proxy looks good"

    print dataset_event_count('/DYJetsToLL_M-50_Zpt-150toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/MINIAODSIM')
    

