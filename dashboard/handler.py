#!/usr/bin/python
import cgi, cgitb 
import os, sys, commands
import json
import commands
import ast

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
# print inp
# for some reason, can't use $USER, must do whoami
status, user = commands.getstatusoutput("whoami")

if "username" not in inp: inp["username"] = ""
if "name" not in inp: inp["name"] = "all"
if inp["page"].strip().lower() == "other":
    if "otherPage" not in inp or inp["otherPage"].strip() == "":
        print "You need to specify [page] in: http://www.t2.ucsd.edu/tastwiki/bin/view/CMS/[page]"
        sys.exit()
    inp["page"] = inp["otherPage"]


onlyUnmade = "unmade" in inp

import twiki

if inp["action"] == "fetch":
    samples = twiki.get_samples(assigned_to=inp["name"], username=inp["username"], get_unmade=onlyUnmade, page=inp["page"])
    if not samples:
        print "None"
    for sample in samples:
        print sample["dataset"], sample["gtag"], sample["xsec"], sample["kfact"], sample["efact"], sample["sparms"]

elif inp["action"] == "update":
    if "samples" in inp: inp["samples"] = json.loads(inp["samples"])
    if "samples" not in inp or (not inp["samples"]):
        print "How do you expect me to update something if you don't give me anything to update it with? Provide samples."
        sys.exit()

    # print inp["samples"][0]
    twiki.update_samples(inp["samples"], username=inp["username"], page=inp["page"])


