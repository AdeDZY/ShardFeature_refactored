//
// Created by Zhuyun Dai on 8/17/16.
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
    double sum_logprob;
    double sum_sqr_logprob;
    double min_logprob;
    FeatVec(){
        df = 0;
        sum_prob = 0;
        sum_tf = 0;
        sum_logprob = 0;
        sum_sqr_logprob = 0;
        min_logprob = 333333333;
    }
    FeatVec(int _df, double _sum_prob, unsigned _sum_tf, double _sum_logprob, double _sum_sqr_logprob, double _min_logprob){
        df = _df;
        sum_prob = _sum_prob;
        sum_tf = _sum_tf;
        sum_logprob = _sum_logprob;
        sum_sqr_logprob = _sum_sqr_logprob;
        min_logprob = _min_logprob;
    }
    void updateFeature(int freq, int docLen){
        df += 1;
        double p = freq/double(docLen);
        sum_prob += p;
        sum_tf += freq;
        sum_logprob += log(p);
        sum_sqr_logprob += log(p) * log(p);
        if(min_logprob > log(p))
			min_logprob = log(p);
    }
};

void get_document_vector(indri::index::Index *index,
                         const int docid,
                         const unordered_set<int> &queryTerms,
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
            features[termID] = FeatVec(1, freq/double(docLen), freq, log(freq/double(docLen)), log(freq/double(docLen)) * log(freq/double(docLen)), log(freq/double(docLen)));
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
        outStream<<it->second.df<<" "<<it->second.sum_tf<<" "<<it->second.sum_prob<<" "<<it->second.sum_logprob<<" "<<" "<<it->second.sum_sqr_logprob<<" "<<it->second.min_logprob<<endl;
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

    // Features
    unordered_map<int, FeatVec> features;
    features[-1] = FeatVec();


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

        intids = IndexEnv.documentIDsFromMetadata("docno", extids);
        intid = intids[0];
        get_document_vector(index, intid, queryTerms, features);

    }
    extidStream.close();

    IndexEnv.close();
    r.close();

    writeFeatures(features, outFile);

    return 0;
}
