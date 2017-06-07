//
// Created by Zhuyun Dai on 8/25/15.
//

#include "indri/Repository.hpp"
#include "indri/CompressedCollection.hpp"
#include "indri/LocalQueryServer.hpp"
#include "indri/QueryEnvironment.hpp"
#include <iostream>
#include <sstream>
#include <math.h>
#include <cmath>
#include <time.h>
#include <stdlib.h>     /* atoi */

#include "indri/Repository.hpp"
#include "indri/CompressedCollection.hpp"
#include "indri/LocalQueryServer.hpp"
#include <tr1/unordered_map>
#include <tr1/unordered_set>

using namespace std;
using namespace indri::api;
using std::tr1::unordered_set;
using std::tr1::unordered_map;

void readQueriesToTerms(unordered_set<string> &queryTerms, const char *queryFile){
    queryTerms.clear();
    ifstream queryStream;
    queryStream.open(queryFile);

    string line;
    while (! queryStream.eof()) {
        getline(queryStream, line);
        if(line.empty()) {
            if(! queryStream.eof()) {
                cout << "Error: Empty line found in query file." <<endl;
                exit(-1);
            }
            else {
                break;
            }
        }

        string term;
        stringstream ss;
        ss.str(line);
        while(!ss.eof()){
            ss>>term;
            queryTerms.insert(term);
        }
    }
}

void mapQueryTermsToId(unordered_set<string> &queryTerms,
                       unordered_set<int> &queryTermIds,
                       unordered_map<int, string> &id2stem,
                       indri::index::Index * index,
                       indri::collection::Repository & repo){
	queryTermIds.clear();

	int termID;
	unordered_set<string>::iterator it;
    for (it = queryTerms.begin(); it != queryTerms.end(); it++){
		string term = *it;
        string stem = repo.processTerm(term);
        termID = index->term(stem);
        if(termID <= 0)
            continue;
        queryTermIds.insert(termID);
        id2stem[termID] = stem;
    }
}

struct FeatVec{
    int df;
    double sum_prob;
    unsigned sum_tf;
    FeatVec(){
        df = 0;
        sum_prob = 0;
        sum_tf = 0;
    }
    FeatVec(int _df, double _sum_prob, unsigned _sum_tf){
        df = _df;
        sum_prob = _sum_prob;
        sum_tf = _sum_tf;
    }
    void updateFeature(int freq, int docLen){
        df += 1;
        sum_prob += freq/double(docLen);
        sum_tf += freq;
    }
};

void get_document_vector(indri::index::Index *index,
                         const int docid,
                         const unordered_set<int> &queryTerms,
                         const unordered_map<int, string> &id2stem,
                         unordered_map<string, FeatVec> &features) {

    unordered_map<int, int> docVec;
    unordered_map<int, int>::iterator docVecIt;

    const indri::index::TermList *list = index->termList(docid);
    indri::utility::greedy_vector <int> &terms = (indri::utility::greedy_vector <int> &) list->terms();

    // the whole documents
    int docLen = 0;
    docVec.clear();
    for (int t = 0; t < terms.size(); t++)
    {

        if (terms[t] <= 0) // [OOV]
            continue;

        docLen++;

        if (queryTerms.find(terms[t]) == queryTerms.end())  // not query term
            continue;

        if (docVec.find(terms[t]) != docVec.end()) {
            docVec[terms[t]]++;
        }
        else {
            docVec[terms[t]] = 1;
        }

    }

	if(docLen <= 0){
		cout<<"docLen = 0: "<< docid<<endl;
		return;
	}
    // update feature
    unordered_map<int, int>::iterator it;
    int termID, freq;
    for(it = docVec.begin(); it != docVec.end(); it++){
        termID = it->first;
        freq = it->second;
        string stem = (id2stem.find(termID))->second;
        if(features.find(stem) == features.end())
            features[stem] = FeatVec(1, freq/double(docLen), freq);
        else
            features[stem].updateFeature(freq, docLen);
    }

	// to get shard size and total term freq in shards
	features[" "].updateFeature(docLen, docLen);

    // Finish processing this doc

    delete list;
    list = NULL;
    terms.clear();

}

void writeFeatures(const unordered_map<string, FeatVec> &features,
                   const string outFile){

    ofstream outStream;
    outStream.open(outFile.c_str());

    unordered_map<string, FeatVec>::const_iterator it;
    vector<string> key_list;
    for (it=features.begin(); it != features.end(); ++it) {
        key_list.push_back(it->first);
    }
	sort(key_list.begin(), key_list.end());
    for (vector<string>::iterator it2=key_list.begin(); it2 != key_list.end(); ++it2) {
        it = features.find(*it2);
        outStream<<it->first;
        outStream<<" ";
        outStream<<it->second.df<<" "<<it->second.sum_tf<<" "<<it->second.sum_prob<<endl;
    }
    outStream.close();
}

int main(int argc, char **argv){
    int nRepos = atoi(argv[1]);     // number of indri repos
    string repoPaths[nRepos];       // each repo path
    int i = 0;
    for(i = 0; i < nRepos; i++){
        repoPaths[i] = argv[i + 2];
        cout<<repoPaths[i]<<endl;
    }
    string extidFile = argv[nRepos + 2];    // doc extid in one shard, each line is an extid
    string outFile = argv[nRepos + 3];      // output file
    std::string queryFile = argv[nRepos + 4]; // each line is a query, seperated by space

    ifstream extidStream;
    extidStream.open(extidFile.c_str());

    // open indri indexes
    indri::index::Index *indexes[nRepos];
    indri::collection::Repository repos[nRepos];
    QueryEnvironment IndexEnvs[nRepos];
    for(i = 0; i < nRepos; i++) {
        IndexEnvs[i].addIndex (repoPaths[i]);
        indri::index::Index *index = NULL;
        indri::collection::Repository::index_state state;
        repos[i].openRead(repoPaths[i]);
        state = repos[i].indexes();
        index = (*state)[0];
        indexes[i] = index;
    }

    // read query terms
    unordered_set<string> queryTerms;
    vector<unordered_set<int> > list_queryTermIDs(nRepos);
    vector<unordered_map<int, string> > list_id2stems(nRepos);


    readQueriesToTerms(queryTerms, queryFile.c_str());
    for(i = 0; i < nRepos; i++){
        unordered_set<int> queryTermIDs;
        mapQueryTermsToId(queryTerms, list_queryTermIDs[i], list_id2stems[i], indexes[i], repos[i]);
    }

    // Features
    unordered_map<string, FeatVec> features;
	features[" "] = FeatVec();


    vector <string> extids;
    vector <int> intids;
    int intid = 0;

    string extid;
    string prevExtid = "";
    string outLine;
    int indexId = 0;
    int ndoc = 0;
    while(!extidStream.eof()){
        extidStream>>extid;
        extids.clear();
        extids.push_back(extid);

        for(i = 0; i < nRepos; i++){
            intids = IndexEnvs[i].documentIDsFromMetadata("docno", extids);
            if (intids.size() < 1) continue;
            intid = intids[0];
            if (intid > 0) break;
        }
        if (intid > 0){
            cout<<extid<<" "<<repoPaths[i]<<endl;
            get_document_vector(indexes[i], intid, list_queryTermIDs[i], list_id2stems[i], features);
        }

    }
    extidStream.close();
    for(i = 0; i < nRepos; i++){
        IndexEnvs[i].close();
        repos[i].close();
    }

    writeFeatures(features, outFile);

    return 0;
}
