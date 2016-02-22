#include <iostream>
#include <fstream>
#include "TChain.h"
#include "TString.h"
#include "TFile.h"
#include "TTree.h"
#include <string>

using namespace std;

int mergeScript(TString unmergedDir, TString unmergedIndices, TString outFile) {
    if (unmergedIndices == "" || unmergedDir == "") {
        cout<<"[merge] File list not supplied. Please supply a list of files to be merged. Exiting..."<<endl;
        return 1;
    }
    if (outFile == "") {
        cout<<"[merge] outFile name not supplied. Please supply a name for merged file. Exiting..."<<endl;
        return 2;
    }

    TChain ch1("Events");

    TObjArray *tx = unmergedIndices.Tokenize(",");
    tx->Print();
    for (Int_t i = 0; i < tx->GetEntries(); i++) {
        TString fname = unmergedDir + "/" + "ntuple_" + ((TObjString *)(tx->At(i)))->String() + ".root";
        cout << "[merge] Adding to chain: " << fname << endl;
        ch1.Add(fname);
    }

    unsigned int nEntries = ch1.GetEntries();
    cout << "[merge] Merging sample with " << nEntries << " entries." << endl;
    ch1.Merge(outFile, "fast");

    // check for correct number of events
    TFile* mergedFile = new TFile(outFile);
    TTree *mergedTree = (TTree*)mergedFile->Get("Events");
    const int mergedCount = mergedTree->GetEntries();
    const int unmergedCount = ch1.GetEntries();
    cout << "[merge] Merged Entries: " << mergedCount << endl;
    cout << "[merge] Unmerged Entries: " << unmergedCount << endl;
    if (mergedCount != unmergedCount){
        cout << "[merge] Merged count not equal to unmerged count. Exiting..." << endl;
        return 4;
    }

    return 0;

}
