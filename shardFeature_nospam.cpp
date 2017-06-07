//
// Created by Zhuyun Dai on 8/25/15.
//

#include "indri/Repository.hpp"
#include "indri/CompressedCollection.hpp"
#include "indri/LocalQueryServer.hpp"
#include "indri/QueryEnvironment.hpp"
#include <algorithm>
#include <iostream>
#include <sstream>
#include <math.h>
#include <cmath>
#include <time.h>
#include "indri/Repository.hpp"
#include "indri/CompressedCollection.hpp"
#include "indri/LocalQueryServer.hpp"
#include <tr1/unordered_map>

using namespace std;
using namespace indri::api;
using std::tr1::unordered_map;


void readExtids(const string extidFile, unordered_map<string, int> &extids){
    extids.clear();
    ifstream extidStream;
    extidStream.open(extidFile.c_str());

    string line;
    while(! extidStream.eof()){
        getline(extidStream, line);
        if(line.empty())
            break;
        extids[line] = 1;
    }

    extidStream.close();
}

void filterSpams(const string spamFile, unordered_map<string, int> &extids){
    ifstream spamStream;
    spamStream.open(spamFile.c_str());

    string line;
    while(! spamStream.eof()){
        getline(spamStream, line);
        if(line.empty())
            break;
        unordered_map<string, int>::iterator found = extids.find(line);
        if(found != extids.end())
            found->second = 0;
    }
    spamStream.close();
}

void readQueryTerms(set<int> &queryTerms, const char *queryTermFile, indri::index::Index * index, indri::collection::Repository & repo){
	queryTerms.clear();
	ifstream queryStream;
	queryStream.open(queryTermFile);
	
	int termID;
	string line;
	while(! queryStream.eof()) {
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
		
		string stem = repo.processTerm(line);
		if(stem.length() <= 0) 
			continue;

		termID = index->term(stem);
		if(termID > 0){
			queryTerms.insert(termID);
			cout<<line<<" "<<termID<<endl;
		}	
	}
	queryStream.close();
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
                         const set<int> &queryTerms,
                         unordered_map<int, FeatVec> &features) {

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
        if(features.find(termID) == features.end())
            features[termID] = FeatVec(1, freq/double(docLen), freq);
        else
            features[termID].updateFeature(freq, docLen);
    }

	// to get shard size and total term freq in shards
	features[-1].updateFeature(docLen, docLen);

    // Finish processing this doc

    delete list;
    list = NULL;
    terms.clear();

}

void writeFeatures(const unordered_map<int, FeatVec> &features,
                   const string outFile){

    ofstream outStream;
    outStream.open(outFile.c_str());

    unordered_map<int, FeatVec>::const_iterator it;
    vector<int> key_list;
    for (it=features.begin(); it != features.end(); ++it) {
        key_list.push_back(it->first);
    }
	sort(key_list.begin(), key_list.end());
    for (vector<int>::iterator it2=key_list.begin(); it2 != key_list.end(); ++it2) {
        it = features.find(*it2);
        outStream<<it->first;
        outStream<<" ";
        outStream<<it->second.df<<" "<<it->second.sum_tf<<" "<<it->second.sum_prob<<endl;
    }
    outStream.close();
}

int main(int argc, char **argv){
    string repoPath = argv[1];
    string extidFile = argv[2];
    string outFile = argv[3];
    std::string queryTermFile = argv[4];
    string spamFile = argv[5];

    // open indri index
    QueryEnvironment IndexEnv;
    IndexEnv.addIndex (repoPath);
    indri::collection::Repository r;
    indri::index::Index *index = NULL;
    indri::collection::Repository::index_state state;
    r.openRead(repoPath);
    state = r.indexes();
    index = (*state)[0];

    // read query terms
    set<int> queryTerms;
    readQueryTerms(queryTerms, queryTermFile.c_str(), index, r);

    // read  extids and filter spams
    unordered_map<string, int> extids;
    readExtids(extidFile, extids);
    filterSpams(spamFile, extids);

    // Features
    unordered_map<int, FeatVec> features;
	features[-1] = FeatVec();


    vector <string> tmpextids;
    vector <int> intids;
    int intid = 0;

    string extid;
    string outLine;
    int indexId = 0;
    int ndoc = 0;

    unordered_map<string, int>::iterator iter;
    for(iter = extids.begin(); iter != extids.end(); iter++)
    {
        // filter out spam
        if (iter->second == 0)
            continue;

         extid = iter->first;

        tmpextids.push_back(extid);
    }
    intids = IndexEnv.documentIDsFromMetadata("docno", tmpextids);

    for(int i = 0; i < intids.size(); i++)
    {
        intid = intids[i];
        get_document_vector(index, intid, queryTerms, features);
    }

    IndexEnv.close();
    r.close();

    writeFeatures(features, outFile);

    return 0;
}
