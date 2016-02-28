import Samples, time
import utils as u
import pprint
import datetime
import json
import sys


all_samples = []


def logger_callback(date, pfx, text):
    pass
    # print "HERE: ", date, pfx, text
    # # fname = "/home/users/namin/public_html/autotupletest/test.txt"
    # line = "<span class='tickerItem'><span class='tickerItemDate'>[%s]</span> <span class='tickerItemPfx'>[%s]</span> <span class='tickerItemText'>%s</span></span>" % (date, pfx, text)
    # u.cmd('echo "%s" >> %s' % (line, fname))

for samp in u.read_samples("instructions.txt"):
    samp["debug"] = False
    s = Samples.Sample(**samp) 
    all_samples.append(s)

# for samp in u.read_samples("instructions_test.txt"):
#     samp["debug"] = False
#     s = Samples.Sample(**samp) 
#     all_samples.append(s)

if u.proxy_hours_left() < 20:
    print "Proxy near end of lifetime, renewing."
    u.proxy_renew()

u.copy_jecs()

data = { 
"samples": [{} for _ in range(len(all_samples))]
}

# for isample, s in enumerate(all_samples):
#     # if "ZZZ" in s["dataset"]: continue
#     s.crab_kill()
#     s.crab_delete_dir()
# sys.exit()

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

    # after we loop through all samples, update the json and copy it over
    data["last_updated"] = u.get_timestamp()
    with open("data.json", "w") as fhout:
        json.dump(data, fhout, sort_keys = True, indent = 4)
    u.copy_json()

    # sleep for 5 mins unless it's the first couple of iterations
    print "sleeping for 5 mins"
    time.sleep(15 if i < 3 else 300)

