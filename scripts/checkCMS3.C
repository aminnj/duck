#include <iostream>
#include <fstream>

#include "TChain.h"
#include "TFile.h"
#include "TTree.h"
#include "TH1.h"
#include "TRegexp.h"
#include "TSystem.h"

using namespace std;


void printColor(const char* message, const int color, bool human) {
  if( human ) printf("\033[%dm%s\033[0m\n", color, message);
  else cout << message << endl;
}


int checkCMS3( TString samplePath = "", TString unmerged_path = "", bool useFilter = false, bool humanUser = true, string sampleName = "") {

  if( samplePath == "" ) {
    cout << "Please provide a path to a CMS3 sample!" << endl;
    return 1;
  }

  int nProblems = 0;
  bool isMerged = false;
  bool isSUSY   = false;
  bool isScan   = false;

  char message[40];

  TChain* chain = new TChain("Events");

  cout << endl;


  /////////////////////////////////////////////////////////////////////////////////
  // File counting
  /////////////////////////////////////////////////////////////////////////////////

  // Make a chain, and count the number of files in the directory
  const unsigned int nMergedFiles = chain->Add( samplePath + "/merged_ntuple*.root");
  const unsigned int nUnmergedFiles = chain->Add( samplePath + "/ntuple*.root");
  const unsigned int nFilesHere = nMergedFiles + nUnmergedFiles;

  // Check to see if these are merged or unmerged ntuples
  if(      nMergedFiles>0  && nUnmergedFiles==0 ) {
    isMerged = true;
    cout << "I found " << nMergedFiles << " merged ntuple files in this directory." << endl;
  }
  else if( nMergedFiles==0 && nUnmergedFiles>0  ) {
    isMerged = false;
    cout << "I found " << nUnmergedFiles << " unmerged ntuple files in this directory." << endl;
  }
  else if( nMergedFiles==0 && nUnmergedFiles==0 ) {
    cout << "Sorry, I couldn't find any CMS3 ntuples in this directory!" << endl;
    return 1;
  }
  else if( nMergedFiles>0  && nUnmergedFiles>0  ) {
    cout << "This doesn't make sense -- it looks like there are both merged and unmerged ntuples in this directory!" << endl;
    cout << "That shouldn't happen, so I'm exiting..." << endl;
    return 1;
  }
  else {
    cout << "Something went terribly wrong when I tried to count the files in this directory. Exiting!" << endl;
    return 1;
  }


  /////////////////////////////////////////////////////////////////////////////////
  // Set up a few branches, and read in some key values
  /////////////////////////////////////////////////////////////////////////////////

  chain->SetMakeClass(1);
  
  vector<TString> cms3tag;
  vector<TString> dataset;
  vector<TString> sparm_names;
  vector<float>   sparm_values;
  
  TBranch* branch_CMS3tag;
  TBranch* branch_dataset = chain->GetBranch(chain->GetAlias("evt_dataset"));
  TBranch* branch_sparmnames;
  TBranch* branch_sparmvals;

  if( chain->GetAlias("evt_CMS3tag") != 0 ) branch_CMS3tag = chain->GetBranch(chain->GetAlias("evt_CMS3tag"));
  else branch_CMS3tag = chain->GetBranch(chain->GetAlias("evt_CMS2tag"));

  branch_CMS3tag->SetAddress(    &cms3tag      );
  branch_dataset->SetAddress(    &dataset      );

  branch_dataset->GetEntry(1);
  TString dataset_name = dataset.at(0);

  TRegexp massScan("[mM][a-zA-Z]+-[0-9]+to[0-9]+");
  if( dataset_name.Contains(massScan)) isScan = true;
  if( dataset_name.Contains("SMS") )   isSUSY = true;

  if( isSUSY && isMerged ) {
    branch_sparmnames = chain->GetBranch(chain->GetAlias("sparm_names"));
    branch_sparmvals  = chain->GetBranch(chain->GetAlias("sparm_values"));
    branch_sparmnames->SetAddress( &sparm_names  );
    branch_sparmvals->SetAddress(  &sparm_values );
    branch_sparmnames->GetEntry(1);
    branch_sparmvals->GetEntry(1);
  }
  
  /////////////////////////////////////////////////////////////////////////////////
  // Check CMS3 tag
  /////////////////////////////////////////////////////////////////////////////////

  branch_CMS3tag->GetEntry(0);
  TString tagtext = cms3tag.at(0);

  cout << "\nCMS3 tag: ";
  printColor( tagtext.Data(), 96, humanUser );
  cout << endl;

  TRegexp cms3Version("V[0-9][0-9]-[0-9][0-9]-[a-zA-Z0-9-_]*");

  // If the sample to be checked is stored in the SNT hadoop area,
  //  then check to make sure the directory has the correct tag name

  if( samplePath.Contains("/hadoop/cms/store/group/snt") ) {
	TString tagStored = tagtext(cms3Version);
	TString tagDir    = samplePath(cms3Version);
	if( tagStored != tagDir ) {
	  if( tagDir == "" ) {
		printColor("Warning: I couldn't detect a CMS3 version number in the name of the sample directory:", 93, humanUser);
		cout << samplePath << endl;
	  }
	  else {
		printColor("ERROR! CMS3 tags don't match!", 91, humanUser);
		cout << "This ntuple was made using " << tagStored << ", but it's stored in a directory named " << tagDir << "." << endl;
		nProblems++;
	  }

	}
  }

  
  /////////////////////////////////////////////////////////////////////////////////
  // Event counting
  /////////////////////////////////////////////////////////////////////////////////

  cout << "\n================ Event counts ================================\n" << endl;

  // Read from nEvts branch
  Long64_t nEvts_branch = 0;
  if( isMerged ) {
    nEvts_branch = (Long64_t)chain->GetMaximum("evt_nEvts");
    cout << "Number in \"nEvts\" branch: " << nEvts_branch << endl;
  }

  // Count using chain->GetEntries()
  cout << "Total events in files:    " << flush;
  const Long64_t nEvts_chain = chain->GetEntries();
  cout << nEvts_chain << endl;

  // Get event count from DAS
  cout << "Event count from DAS:     " << flush;
  bool das_failed = true;
  int loop_count = 0;
  Long64_t nEvts_das = -9999;

  while( loop_count<5 && das_failed ) {
      std::cout << "python das_client.py --query=\"dataset= "+dataset_name+" | grep dataset.nevents\" " << std::endl;
    TString Evts_das = gSystem->GetFromPipe( "python das_client.py --query=\"dataset= "+dataset_name+" | grep dataset.nevents\" | tail -1" );
    nEvts_das = Evts_das.Atoll();
    if( nEvts_das > 0 ) das_failed = false;
    loop_count++;
  }

  if( das_failed ) {
    printColor("ERROR! DAS query failed!", 91, humanUser);
    nProblems++;
  }
  else cout << nEvts_das << endl;

  ////////////////////////////////////////////////
  //    CHECK EVENT COUNTS                      //
  ////////////////////////////////////////////////
 
  bool countsMatch = false;

  //1. Merged files
  if( isMerged ) {

    //(a) Check das vs. branch. A problem here normally indicates a problem with the unmerged files
    if( nEvts_das == nEvts_branch ) countsMatch = true;

    //(b) Check unmerged vs. merged. A problem here normally indicates a problem with merging
    int nEvts_unmerged = 0;
    if( unmerged_path == "" ) cout << "Warning!  No unmerged path provided, will not check nMerged == nUnmerged..." << endl;
    else {
      TChain* chain_unmerged = new TChain("Events");
      int nFiles = chain_unmerged->Add( unmerged_path + "/ntuple_*.root");
      if (nFiles == 0) {
        cout << "ERROR! Unmerged files not found. Aborting..." << endl;
        return 99; 
      }
      nEvts_unmerged = chain_unmerged->GetEntries();
      if( nEvts_unmerged != nEvts_chain ) {
        countsMatch = false;
        printColor("ERROR! Too few merged events! ", 91, humanUser);
      }
      cout << "Evts in unmerged sample:  " << nEvts_unmerged << endl;
    }

    //(c) Check branch == merged if there is no filter
    if (!useFilter && nEvts_branch != nEvts_chain) countsMatch = false;
  }

  //2. Unmerged files
  else if(!isMerged && nEvts_chain==nEvts_das) countsMatch = true;

  ////////////////////////////////////////////////
  //    REPORT EVENT COUNTS                     //
  ////////////////////////////////////////////////

  if( countsMatch ) printColor("            Matched", 92, humanUser);
  else {
    printColor("ERROR!            Counts mismatch!", 91, humanUser);
    nProblems++;
  }

  // Breakdown by filename
  if( nFilesHere > 1 && !countsMatch
      && ((nEvts_chain!=nEvts_das && !das_failed) || nEvts_chain!=nEvts_branch) ) {

    float nEvtsPerFile = nEvts_chain / float(nFilesHere);
    bool isHigh = false;
    bool isLow  = false;

    cout << "\nNumber of events by file:" << endl;
    TObjArray *fileList = chain->GetListOfFiles();
    TIter fileIter(fileList);
    TFile *currentFile = 0;
    TRegexp shortname("[mergd_]*ntuple_[0-9]+.root");

    printf( "%28s:  %10.1f\n", "Average", nEvtsPerFile );

    while(( currentFile = (TFile*)fileIter.Next() )) {
      TFile *file = new TFile( currentFile->GetTitle() );
      TTree *tree = (TTree*)file->Get("Events");
      TString filename = file->GetName();
      Long64_t nEvts_file = tree->GetEntries();

      isHigh = false;
      isLow = false;
      if( nEvts_file < (0.5*nEvtsPerFile) ) isLow = true;
      else if( nEvts_file > (1.7*nEvtsPerFile) ) isHigh = true;

      if( isHigh ) printf( "ERROR! Wrong event count for %28s:  %8lld  <-- count is high\n", filename(shortname).Data(), nEvts_file );
      else if( isLow) printf( "ERROR! Wrong event count for %28s:  %8lld  <-- count is low\n", filename(shortname).Data(), nEvts_file );
      else if( !isHigh && !isLow && nFilesHere<10 ) printf( "ERROR! Wrong event count for %28s:  %8lld\n", filename(shortname).Data(), nEvts_file );

	  delete tree;
	  file->Close();
	  delete file;
    }
  }



  /////////////////////////////////////////////////////////////////////////////////
  // Check CMS3 post-processing variables (if this is a merged sample)
  /////////////////////////////////////////////////////////////////////////////////

  cout << "\n\n============ Post-processing variables ============================" << endl;

  if( isMerged ) {
    // Check for branches with values set to zero
    cout << "\nChecking for events with important values set to zero:" << endl;
    Long64_t nZeros_xsec     = chain->GetEntries("evt_xsec_incl==0");
    cout << "Cross-section: ";
    if( nZeros_xsec == 0 ) printColor("No zeros found", 92, humanUser);
    else {
      sprintf(message, "ERROR! %lld events with zeros in evt_xsec_incl!", nZeros_xsec);
      printColor(message, 91, humanUser);
      nProblems++;
    }
    Long64_t nZeros_kfact    = chain->GetEntries("evt_kfactor==0");
    cout << "k factor:      ";
    if( nZeros_kfact == 0 ) printColor("No zeros found", 92, humanUser);
    else {
      sprintf(message, "ERROR! %lld events with zeros in evt_kfactor!", nZeros_kfact);
      printColor(message, 91, humanUser);
      nProblems++;
    }
    Long64_t nZeros_filteff  = chain->GetEntries("evt_filt_eff==0");
    cout << "Filter eff:    ";
    if( nZeros_filteff == 0 ) printColor("No zeros found", 92, humanUser);
    else {
      sprintf(message, "ERROR! %lld events with zeros in evt_filt_eff!", nZeros_filteff);
      printColor(message, 91, humanUser);
      nProblems++;
    }
    Long64_t nZeros_scale1fb = chain->GetEntries("evt_scale1fb==0");
    cout << "scale1fb:      ";
    if( nZeros_scale1fb == 0 ) printColor("No zeros found", 92, humanUser);
    else {
      sprintf(message, "ERROR! %lld events with zeros in evt_scale1fb!", nZeros_scale1fb);
      printColor(message, 91, humanUser);
      nProblems++;
    }

    // Make sure the value of scale1fb is consistent with the other numbers
    cout << "\nChecking values for consistency:" << endl;
    cout << " Number of events = " << nEvts_branch << endl;
    double xsec = chain->GetMaximum("evt_xsec_incl");
    cout << "    Cross section = " << xsec << endl;
    double kfact = chain->GetMaximum("evt_kfactor");
    cout << "         k factor = " << kfact << endl;
    double filteff = chain->GetMaximum("evt_filt_eff");
    cout << "Filter efficiency = " << filteff << endl;
    double scale1fb = chain->GetMaximum("evt_scale1fb");
    cout << "         Scale1fb = " << scale1fb << endl;

    double test_scale1fb = 1000.*xsec*filteff*kfact / double(nEvts_branch);
    // cout << "test_scale1fb: " << test_scale1fb << ". Consistency: " << (test_scale1fb - scale1fb) / scale1fb << endl;
    if( ((test_scale1fb - scale1fb) / scale1fb) < 0.000001 ) printColor("                 CONSISTENT ", 92, humanUser);
    else {
      printColor("ERROR!                Inconsistent scale1fb! ", 91, humanUser);
      nProblems++;
    }

  } // end "if( isMerged )"
  else cout << "\nThis is an unmerged sample. Skipping checks on postprocessing branches..." << endl;


  /////////////////////////////////////////////////////////////////////////////////
  // Sparm checks
  /////////////////////////////////////////////////////////////////////////////////

  cout << "\n\n============ Sparm branches ============================" << endl;   

  if( isSUSY && isMerged ) {

    if( isScan ) {
      cout << "\nThis appears to be a SUSY scan, with a range of sparm values." << endl;
	  cout << "If you want to verify all of those sparm values, you'll need to do it manually." << endl;
    }
    else {
      cout << "\nFound these sparm values:" << endl;
      int nSparmVals = sparm_values.size();

      for( int i=0; i<nSparmVals; i++ ) {
        sprintf(message, "%10s = %7.1f", sparm_names.at(i).Data(), sparm_values.at(i) );

        if( sparm_values.at(i) > 0. ) printColor(message, 92, humanUser);
        else {
          printColor(message, 91, humanUser);
          nProblems++;
        }
      } //end loop over sparm variables
    }
  } //end if(isSUSY && isMerged)
  else if( !isSUSY) cout << "\nThis doesn't appear to be a SUSY sample. Skipping checks on sparm branches..." << endl;
  else if( !isMerged ) cout << "\nThis SUSY sample isn't merged. Skipping checks on sparm branches..." << endl;



  /////////////////////////////////////////////////////////////////////////////////
  // Summary
  /////////////////////////////////////////////////////////////////////////////////
  
  cout << "\n\n=============== RESULTS =========================" << endl;  
  cout << "\nTotal problems found: ";
  if (nProblems==0) printColor("0", 92, humanUser);
  else {
    sprintf(message, "%d", nProblems);
    printColor(message, 91, humanUser);
  }
  cout << endl;

  return nProblems;
}
