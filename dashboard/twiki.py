import mechanize
import os
import sys
from collections import OrderedDict


def get_browser(page, username, password_file = "%s/.twikipw" % os.getenv("HOME")):
    BASE_URL = "http://www.t2.ucsd.edu/tastwiki/bin/view/CMS/"

    with open(password_file, "r") as fhin: password = fhin.read().strip()

    br = mechanize.Browser()
    br.addheaders = [('User-agent', 'Firefox')]
    br.set_handle_robots( False )
    br.open(BASE_URL+page)
    br.select_form(nr=0)
    br.form['username'] = username
    br.form['password'] = password
    br.submit()
    return br

def update_samples(samples):
    br = get_browser("Autotupletest", "namin")
    for link in br.links():
        if link.text.strip() == 'Raw Edit':
            br.follow_link(link)
            break
    br.select_form('main')
    raw = br.get_value('text')

    columns = ["dataset", "filter_type", "nevents_in", "nevents_out", "xsec", "kfact", "efact", "gtag", "cms3tag", "location", "assigned", "comments"] 
    lines_out = []
    for iline,line in enumerate(raw.split("\n")):
        if line.count("|") is 13 and "*Dataset*" not in line:
            line = line.strip()
            parts = map(lambda x: x.strip(), line.split("|")[1:-1])
            sample_twiki  = OrderedDict( zip(columns, parts) )

            for sample in samples:
                # find matching sample (dataset, gtag, cms3tag must match), then fill in events and location
                if sample_twiki["dataset"] == sample["dataset"] and sample_twiki["cms3tag"] == sample["cms3tag"] \
                  and sample_twiki["gtag"] == sample["gtag"] and sample_twiki["location"] == "":
                    sample_twiki["location"] = sample["finaldir"]
                    sample_twiki["nevents_in"] = sample["nevents_DAS"]
                    sample_twiki["nevents_out"] = sample["nevents_merged"]
                    line = "| %s |" % " | ".join(map(str,sample_twiki.values()))
                break

        lines_out.append(line)

    tosubmit = "\n".join(lines_out)
    # SANITY CHECK: if replacement text is less than 95% of original text, maybe we screwed up
    # if we didn't take out whitespace, adding entries would increase the size and then we could use 100%
    if len(tosubmit) > 0.95*len(raw):
        br.form['text'] = "\n".join(lines_out)
        br.submit()

if __name__=='__main__':

    samples = [{"dataset": "/DYToEE_13TeV-amcatnloFXFX-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/MINIAODSIM",
              "cms3tag": "CMS3_V07-04-08",
              "gtag": "MCRUN2_74_V9",
              "finaldir": "final/dir/test/",
              "nevents_DAS": 312261,
              "nevents_merged": 312261,
            }]
    update_samples(samples)
