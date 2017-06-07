//
// Created by Zhuyun Dai on 3/21/16.
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
#include "indri/Repository.hpp"
#include "indri/CompressedCollection.hpp"
#include "indri/LocalQueryServer.hpp"
#include <tr1/unordered_map>
#include <tr1/unordered_set>

using namespace std;
using namespace indri::api;
using std::tr1::unordered_set;
using std::tr1::unordered_map;

void readQueryTerms(unordered_set<int> &queryTerms, const char *queryTermFile, indri::index::Index * index, indri::collection::Repository & repo){
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
                         const unordered_set<int> &queryTerms,
                         unordered_map<int, FeatVec> featureLists[]) {

    int nFields = 2;
    string fields[2] = {"title", "inlink"};
    int fieldIDs[nFields + 1];
    for (int i = 0; i < nFields; i++) {
        fieldIDs[i] = index->field(fields[i]);
    }

    unordered_map<int, int> docVecs[2];
    unordered_map<int, int>::iterator docVecIt;

    const indri::index::TermList *list = index->termList(docid);
    indri::utility::greedy_vector <int> &terms = (indri::utility::greedy_vector <int> &) list->terms();
    indri::utility::greedy_vector <indri::index::FieldExtent> fieldVec = list->fields();
    indri::utility::greedy_vector<indri::index::FieldExtent>::iterator fIter = fieldVec.begin();

    // the whole documents
    int docLens[2] = {0, 0};
    int fdx;
    while(fIter != fieldVec.end())
    {
        // find the field
        for(fdx = 0; fdx < nFields; fdx++){
            if ((*fIter).id == fieldIDs[fdx]){
               break;
            }
        }
        if(fdx >= nFields){
            fIter++;
            continue;
        }

        // processing the fdx field

        int beginTerm = (*fIter).begin;
        int endTerm = (*fIter).end;

        // note that the text is inclusive of the beginning
        // but exclusive of the ending
        for(int t = beginTerm; t < endTerm; t++){
            if (terms[t] <= 0) // [OOV]
                continue;

            docLens[fdx]++;

            if (queryTerms.find(terms[t]) == queryTerms.end())  // not query term
                continue;

            if (docVecs[fdx].find(terms[t]) != docVecs[fdx].end()) {
                docVecs[fdx][terms[t]]++;
            }
            else {
                docVecs[fdx][terms[t]] = 1;
            }
        }
        fIter++;
    }

    // update feature
    for(fdx = 0; fdx < nFields; fdx++){
        if(docLens[fdx] <= 0)
            continue;
        unordered_map<int, int>::iterator it;
        int termID, freq;
        for(it = docVecs[fdx].begin(); it != docVecs[fdx].end(); it++){
            termID = it->first;
            freq = it->second;
            if(featureLists[fdx].find(termID) == featureLists[fdx].end())
                featureLists[fdx][termID] = FeatVec(1, freq/double(docLens[fdx]), freq);
            else
                featureLists[fdx][termID].updateFeature(freq, docLens[fdx]);
        }
        // to get shard size and total term freq in shards
        featureLists[fdx][-1].updateFeature(docLens[fdx], docLens[fdx]);
    }

    // Finish processing this doc
    delete list;
    list = NULL;
    terms.clear();
}

void writeFeatures(const unordered_map<int, FeatVec> featureLists[],
                   const string outFile){

    ofstream outStream;
    outStream.open(outFile.c_str());
    int nFields = 2;
    for(int fdx = 0; fdx < nFields; fdx++){
        unordered_map<int, FeatVec>::const_iterator it;
        vector<int> key_list;
        for (it=featureLists[fdx].begin(); it != featureLists[fdx].end(); ++it) {
            key_list.push_back(it->first);
        }
	    sort(key_list.begin(), key_list.end());
        for (vector<int>::iterator it2=key_list.begin(); it2 != key_list.end(); ++it2) {
            it = featureLists[fdx].find(*it2);
            outStream<<it->first;
            outStream<<" ";
            outStream<<it->second.df<<" "<<it->second.sum_tf<<" "<<it->second.sum_prob<<endl;
        }
        outStream<<endl;
    }
    outStream.close();
}

int main(int argc, char **argv){
    string repoPath = argv[1];
    string extidFile = argv[2];
    string outFile = argv[3];
    std::string queryTermFile = argv[4];

    ifstream extidStream;
    extidStream.open(extidFile.c_str());

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
    unordered_set<int> queryTerms;
    readQueryTerms(queryTerms, queryTermFile.c_str(), index, r);

    // Features of different field
    int nFields = 2;
    unordered_map<int, FeatVec> featureLists[nFields];

    for(int i = 0; i < nFields; i++){
        featureLists[i][-1] = FeatVec();
    }

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
        extids.push_back(extid);
    }
    extidStream.close();

    intids = IndexEnv.documentIDsFromMetadata("docno", extids);
    for(int i = 0; i < intids.size(); i++){
        intid = intids[i];
        get_document_vector(index, intid, queryTerms, featureLists);
    }

    IndexEnv.close();
    r.close();

    writeFeatures(featureLists, outFile);

    return 0;
}
