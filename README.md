# duck
## Instructions
* `source setup.sh` will set up your environment and make the dashboard
* Use the dashboard to create an `instructions.txt` file
* At this point, I prefer to start a screen and then make sure to `source setup.sh` (maybe a couple of times until it doesn't complain).
* `python run.py instructions.txt`
* Sit back and relax

## TODO:
- [ ] Check that nothing happened to the files after copying (don't need to do full blown CMS3, just check event counts or something)
- [ ] Parse checkCMS3 output and remake stuff appropriately
- [ ] Be able to change xsec, kfact, efact before post-processing (either through web interface or through an updated instructions.txt)
- [ ] Copy metadata to backup directory
- [ ] If merged files are already in the final directory, either warn users or mark job as done
- [ ] Be able to nuke and resubmit job from dashboard
- [ ] Resubmit crab task if been bootstrapped or some other thing for longer than x minutes

