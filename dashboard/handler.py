#!/usr/bin/python
import cgi, cgitb 
import os, sys, commands
import json
import commands

def inputToDict(form):
    d = {}
    for k in form.keys():
        d[k] = form[k].value
    return d


form = cgi.FieldStorage()

print "Content-type:text/html\r\n"
inp = inputToDict(form)
# for some reason, can't use $USER, must do whoami
status, user = commands.getstatusoutput("whoami")

if "username" not in inp: inp["username"] = ""
if "name" not in inp: inp["name"] = "all"

onlyUnmade = "unmade" in inp

import twiki
samples = twiki.get_samples(assigned_to=inp["name"], username=inp["username"], get_unmade=onlyUnmade, page=inp["page"])
if not samples:
    print "None"
for sample in samples:
    print sample["dataset"], sample["gtag"], sample["xsec"], sample["kfact"], sample["efact"], sample["sparms"]

