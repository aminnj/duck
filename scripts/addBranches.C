#include <iostream>
#include <fstream>
#include <cstdlib>
#include "TFile.h"
#include "TTree.h"
#include "TBranch.h"
#include "TString.h"

using namespace std;

int addBranches(TString inFile, TString outFile, ULong64_t nevents, ULong64_t nevents_effective,
        Float_t xsec, Float_t kfactor, Float_t filt_eff) {

    cout << "[addBranches] Processing File " << inFile << endl;

    TFile *f = TFile::Open(inFile, "READ");
    if (! f || f->IsZombie()) {
        cout << "[addBranches] File does not exist!" << endl;
        return 1;
    }

    TTree* t = (TTree*)f->Get("Events");
    if (! t || t->IsZombie()) {
        cout << "[addBranches] Tree does not exist!" << endl;
        return 2;
    }

    // check for CMS3 first then CMS2
    bool isData = false;
    if (t->GetBranch("int_eventMaker_evtisRealData_CMS3.obj") != 0) {
        isData = (Int_t)t->GetMaximum("int_eventMaker_evtisRealData_CMS3.obj");
    }
    else if (t->GetBranch("int_eventMaker_evtisRealData_CMS2.obj") != 0) {
        isData = (Int_t)t->GetMaximum("int_eventMaker_evtisRealData_CMS2.obj");
    }

    // Removes all non *_CMS*.* branches
    t->SetBranchStatus("*", 0);
    t->SetBranchStatus("*_CMS*.*", 1);

    // Removes the branches (if they exist) that we want to replace
    TString bName = t->GetAlias("evt_xsec_excl");
    if(bName != "") { bName.ReplaceAll(".obj", "*"); t->SetBranchStatus(bName.Data(), 0); }

    bName = t->GetAlias("evt_xsec_incl");
    if(bName != "") { bName.ReplaceAll(".obj", "*"); t->SetBranchStatus(bName.Data(), 0); }

    bName = t->GetAlias("evt_kfactor");
    if(bName != "") { bName.ReplaceAll(".obj", "*"); t->SetBranchStatus(bName.Data(), 0); }

    bName = t->GetAlias("evt_nEvts");
    if(bName != "") { bName.ReplaceAll(".obj", "*"); t->SetBranchStatus(bName.Data(), 0); }

    bName = t->GetAlias("evt_nEvts_effective");
    if(bName != "") { bName.ReplaceAll(".obj", "*"); t->SetBranchStatus(bName.Data(), 0); }

    bName = t->GetAlias("evt_filt_eff");
    if(bName != "") { bName.ReplaceAll(".obj", "*"); t->SetBranchStatus(bName.Data(), 0); }

    bName = t->GetAlias("evt_scale1fb");
    if(bName != "") { bName.ReplaceAll(".obj", "*"); t->SetBranchStatus(bName.Data(), 0); }

    TFile *out = TFile::Open(outFile, "RECREATE");
    TTree *clone = t->CloneTree(-1, "fast");

    //Calculate scaling factor and put variables into tree 
    Float_t scale1fb = 1000.0*xsec*kfactor*filt_eff/(Double_t)nevents_effective;

    if(isData) {
        scale1fb = 1.0;
        cout<< "[addBranches] Data file. scale1fb: " << scale1fb << endl;
    } else {
        cout << "[addBranches] scale1fb: " << scale1fb << endl; 
    }

    TBranch* b1 = clone->Branch("evtscale1fb", &scale1fb, "evt_scale1fb/F");
    TBranch* b2 = clone->Branch("evtxsecexcl", &xsec, "evt_xsec_excl/F");
    TBranch* b3 = clone->Branch("evtxsecincl", &xsec, "evt_xsec_incl/F");
    TBranch* b4 = clone->Branch("evtkfactor", &kfactor, "evt_kfactor/F");
    TBranch* b5 = clone->Branch("evtnEvts", &nevents, "evt_nEvts/l");
    TBranch* b6 = clone->Branch("evtnEvtseffective", &nevents_effective, "evt_nEvts_effective/l");
    TBranch* b7 = clone->Branch("evtfilteff", &filt_eff, "evt_filt_eff/F");

    clone->SetAlias("evt_scale1fb",  "evtscale1fb");
    clone->SetAlias("evt_xsec_excl", "evtxsecexcl");
    clone->SetAlias("evt_xsec_incl",  "evtxsecincl");
    clone->SetAlias("evt_kfactor",   "evtkfactor");
    clone->SetAlias("evt_nEvts",     "evtnEvts");
    clone->SetAlias("evt_nEvts_effective", "evtnEvtseffective");
    clone->SetAlias("evt_filt_eff",     "evtfilteff");

    t->SetMakeClass(1);
    t->SetBranchStatus("*", 0);
    t->SetBranchStatus("float_genMaker_genpsweight_CMS3.obj", 1);

    float genWeight = 999;
    if(!isData) t->SetBranchAddress("float_genMaker_genpsweight_CMS3.obj", &genWeight);

    Int_t nentries = t->GetEntries();
    for(Int_t i = 0; i < nentries; i++) {
        if(!isData){
            t->GetEntry(i);
            if(genWeight < 0) scale1fb = -1*fabs(scale1fb);
            else scale1fb = fabs(scale1fb);
        } 
        b1->Fill();
        b2->Fill();
        b3->Fill();
        b4->Fill();
        b5->Fill();
        b6->Fill();
        b7->Fill();
    }

    clone->Write(); 
    out->Close();
    f->Close();
    return 0;

}

