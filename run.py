import Samples, time
import utils as u
import pprint
import datetime
import json
import sys
import os

DO_TEST = True # if True, put the final samples in a  /snt/test/ dir so we don't screw anything

instructions = "instructions.txt"
if len(sys.argv) > 1:
    instructions = sys.argv[1]
    if not os.path.isfile(instructions):
        print ">>> %s does not exist" % instructions
        sys.exit()

if u.proxy_hours_left() < 60:
    print ">>> Proxy near end of lifetime, renewing."
    u.proxy_renew()

u.copy_jecs()


all_samples = []
for i in range(5000):

    data = { "samples": [], "last_updated": None }

    # read instructions file. if new sample found, add it to list
    # for existing samples, try to update params (xsec, kfact, etc.)
    for samp in u.read_samples(instructions):
        if samp not in all_samples:
            if DO_TEST: 
                samp["specialdir_test"] = True
                print ">>> You have specified DO_TEST, so final samples will end up in snt/test/!"
            s = Samples.Sample(**samp) 
            all_samples.append(s)
        else:
            all_samples[all_samples.index(samp)].update_params(samp)

    # for isample, s in enumerate(all_samples):
    #     s.nuke()
    # sys.exit()

    for isample, s in enumerate(all_samples):
        stat = s.get_status()

        if not s.pass_tsa_prechecks(): continue

        if stat == "new":
            s.crab_submit()
        elif stat == "crab":
            s.crab_parse_status()
            if s.is_crab_done():
                s.make_miniaod_map()
                s.make_merging_chunks()
                s.submit_merge_jobs()
        elif stat == "postprocessing":
            if s.is_merging_done():
                s.make_metadata()
                if s.check_output():
                    s.copy_files()
            else:
                s.submit_merge_jobs()
        elif stat == "done":
            pass

        s.save()
        data["samples"].append( s.get_slimmed_dict() )

    data["last_updated"] = u.get_timestamp()
    with open("data.json", "w") as fhout:
        json.dump(data, fhout, sort_keys = True, indent = 4)
    u.copy_json()

    time.sleep(5 if i < 3 else 600)

