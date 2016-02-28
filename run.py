import Samples, time
import utils as u
import pprint
import datetime
import json
import sys
import os

instructions = "instructions_test.txt"
if len(sys.argv) > 1:
    instructions = sys.argv[1]
    if not os.isfile(instructions):
        print ">>> %s does not exist" % instructions
        sys.exit()

all_samples = []

for samp in u.read_samples(instructions):
    samp["debug"] = False
    s = Samples.Sample(**samp) 
    all_samples.append(s)

if u.proxy_hours_left() < 20:
    print "Proxy near end of lifetime, renewing."
    u.proxy_renew()

u.copy_jecs()
u.make_dashboard()

data = { 
"samples": [{} for _ in range(len(all_samples))]
}

for i in range(5000):
    for isample, s in enumerate(all_samples):
        stat = s.get_status()

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
        data["samples"][isample] = s.get_slimmed_dict()

    data["last_updated"] = u.get_timestamp()
    with open("data.json", "w") as fhout:
        json.dump(data, fhout, sort_keys = True, indent = 4)
    u.copy_json()

    time.sleep(15 if i < 3 else 600)

