#!/usr/bin/python
import cgi, cgitb 
import os, sys, commands
import json
import commands
import ast
import datetime

def inputToDict(form):
    d = {}
    for k in form.keys():
        # print k, form[k].value
        d[k] = form[k].value
    return d


form = cgi.FieldStorage()

print "Content-type:text/html\r\n"
# inp = {"name": "", "username": "", "page": "Other", "otherPage": ""}
inp = inputToDict(form)
# for some reason, can't use $USER, must do whoami
status, user = commands.getstatusoutput("whoami")

action = inp["action"]

# some pre-processing
if action in ["fetch", "update"]:
    if "username" not in inp: inp["username"] = ""
    if "name" not in inp: inp["name"] = "all"
    if inp["page"].strip().lower() == "other":
        if "otherPage" not in inp or inp["otherPage"].strip() == "":
            print "You need to specify [page] in: http://www.t2.ucsd.edu/tastwiki/bin/view/CMS/[page]"
            sys.exit()
        inp["page"] = inp["otherPage"]
    onlyUnmade = "unmade" in inp


if action == "fetch":
    import twiki
    samples = twiki.get_samples(assigned_to=inp["name"], username=inp["username"], get_unmade=onlyUnmade, page=inp["page"])

    if not samples:
        print "None"
    for isample, sample in enumerate(samples):
        if isample == 0:
            print "# samples assigned to %s from %s" % (inp["name"], inp["page"])
        print sample["dataset"], sample["gtag"], sample["xsec"], sample["kfact"], sample["efact"], sample["sparms"]

elif inp["action"] == "update":
    import twiki
    if "samples" in inp: inp["samples"] = json.loads(inp["samples"])
    if "samples" not in inp or (not inp["samples"]):
        print "How do you expect me to update something if you don't give me anything to update it with? Provide samples."
        sys.exit()

    twiki.update_samples(inp["samples"], username=inp["username"], page=inp["page"])

elif inp["action"] == "addinstructions":
    if "data" not in inp:
        sys.exit()

    data = inp["data"]
    basedir = inp["basedir"]

    samples = data.split("\n")
    samples = [sample.strip() for sample in samples if sample.strip().startswith("/")]
    samples = [sample for sample in samples if len(sample.split()) >= 5]


    # fname = basedir + "instructions_autotupletest.txt"
    fname = basedir + "instructions.txt"

    if len(samples) < 1: sys.exit()

    with open(fname, "a") as fhin:
        fhin.write("\n\n# following %i samples injected from dashboard on %s\n" \
                % (len(samples), str(datetime.datetime.now())))
        fhin.write("\n".join(samples))

    print "Wrote %i samples to %s" % (len(samples), fname)





