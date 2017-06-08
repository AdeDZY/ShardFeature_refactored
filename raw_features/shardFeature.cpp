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


void readQueryBigrams(set<pair<int, int> > &queryBigrams, const char *queryFile, indri::index::Index * index, indri::collection::Repository & repo){
    queryBigrams.clear();
    ifstream queryStream;
    queryStream.open(queryFile);

    int termID;
    vector<int> termIds;
    string line;
    while(! queryStream.eof()) {
        termIds.clear();
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
            string stem = repo.processTerm(term);
            termID = index->term(stem);
            termIds.push_back(termID);
        }

        for(int i = 0; i < termIds.size() - 1; i++){
            if(termIds[i] >= 0 && termIds[i + 1] >= 0){
                queryBigrams.insert(std::make_pair<int, int>(termIds[i], termIds[i + 1])) ;
                cout<<termIds[i]<<"_"<<termIds[i + 1]<<endl;
            }
        }
    }
    queryStream.close();
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
                         const set<pair<int, int> > &queryBigrams,
                         const unordered_map<int, string> &id2stem,
                         unordered_map<string, FeatVec> featureList[],
                         map<pair<string, string>, FeatVec> &bigramFeatures) {

    int nFields = 3;
    string fields[nFields] = {"body", "title", "inlink"};
	int fieldIDs[nFields + 1];
    for (int i = 0; i < nFields; i++) {
        fieldIDs[i] = index->field(fields[i]);
    }
    
	unordered_map<int, int> docVecs[nFields];
    unordered_map<int, int>::iterator docVecIt;

    const indri::index::TermList *list = index->termList(docid);
    indri::utility::greedy_vector <int> &terms = (indri::utility::greedy_vector <int> &) list->terms();
    indri::utility::greedy_vector <indri::index::FieldExtent> fieldVec = list->fields();
    indri::utility::greedy_vector<indri::index::FieldExtent>::iterator fIter = fieldVec.begin();

    int docLens[nFields] = {0, 0, 0, 0};
    int fdx;
    while(fIter != fieldVec.end())
    {
        // find which field the current fIter is
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

        // the text is inclusive of the beginning
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
            string stem = (id2stem.find(termID))->second;
            if(featureList[fdx].find(stem) == featureList[fdx].end())
                featureList[fdx][stem] = FeatVec(1, freq/double(docLens[fdx]), freq);
            else
                featureList[fdx][stem].updateFeature(freq, docLens[fdx]);
        }
        // to get shard size and total term freq in shards
        featureList[fdx]["-1"].updateFeature(docLens[fdx], docLens[fdx]);
    }

    // bigram features
    // the whole documents
    int docLen = 0;
    map<pair<int, int>, int> docVec;
    docVec.clear();
    for (int t = 0; t < terms.size() - 1; t++)
    {

        docLen++;

        if (terms[t] <= 0) // [OOV]
            continue;
        if (terms[t + 1] <= 0)
            continue;

        std::pair<int, int> p = std::make_pair(terms[t], terms[t + 1]);
        if (queryBigrams.find(p) == queryBigrams.end())  // not query term
            continue;

        if (docVec.find(p) != docVec.end()) {
            docVec[p]++;
        }
        else {
            docVec[p] = 1;
        }

    }

    if(docLen > 0){
        // update feature
        map<pair<int, int>, int>::iterator it;
        int freq;
        pair<int, int> bigram;
        for(it = docVec.begin(); it != docVec.end(); it++){
            bigram = it->first;
            freq = it->second;
            int termID_0 = bigram.first;
            int termID_1 = bigram.second;
            string stem_0 = (id2stem.find(termID_0))->second;
            string stem_1 = (id2stem.find(termID_1))->second;
            pair<string, string> bigram_stem = std::make_pair(stem_0, stem_1);
            if(bigramFeatures.find(bigram_stem) == bigramFeatures.end())
                bigramFeatures[bigram_stem] = FeatVec(1, freq/double(docLen), freq);
            else
                bigramFeatures[bigram_stem].updateFeature(freq, docLen);
        }
    }

    // to get shard size and total term freq in shards
    bigramFeatures[std::make_pair("-1", "-1")].updateFeature(docLen, docLen);

    // Finish processing this doc

    delete list;
    list = NULL;
    terms.clear();

}

void writeFeatures(const unordered_map<string, FeatVec> featureList[],
                   const string outFile){
    ofstream outStream;
    outStream.open(outFile.c_str());
    int nFields = 3;
    for(int fdx = 0; fdx < nFields; fdx++){
        unordered_map<string, FeatVec>::const_iterator it;
        vector<string> key_list;
        for (it=featureList[fdx].begin(); it != featureList[fdx].end(); ++it) {
            key_list.push_back(it->first);
        }
        sort(key_list.begin(), key_list.end());
        for (vector<string>::iterator it2=key_list.begin(); it2 != key_list.end(); ++it2) {
            it = featureList[fdx].find(*it2);
            outStream<<it->first;
            outStream<<" ";
            outStream<<it->second.df<<" "<<it->second.sum_tf<<" "<<it->second.sum_prob<<endl;
        }
        outStream<<endl;
    }
    outStream.close();
}

void writeBigramFeatures(const map<pair<string, string>, FeatVec> &features,
                         const string outFile){

    ofstream outStream;
    outStream.open(outFile.c_str());

    map<pair<string, string>, FeatVec>::const_iterator it;
    for(it = features.begin(); it != features.end(); it++){
        outStream<<(it->first).first<<"_"<<(it->first).second;
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
    string outUnigramFile = argv[nRepos + 3];      // output file
    string outBigramFile = argv[nRepos + 4];      // output file
    std::string queryFile = argv[nRepos + 5]; // each line is a query, seperated by space

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
    vector<set<pair<int, int> > > list_queryBigrams(nRepos);


    readQueriesToTerms(queryTerms, queryFile.c_str());
    for(i = 0; i < nRepos; i++){
        unordered_set<int> queryTermIDs;
        mapQueryTermsToId(queryTerms, list_queryTermIDs[i], list_id2stems[i], indexes[i], repos[i]);
		readQueryBigrams(list_queryBigrams[i], queryFile.c_str(), indexes[i], repos[i]);
    }

    // Features of different field
    int nFields = 3;
    unordered_map<string, FeatVec> featureList[nFields];
    for(int i = 0; i < nFields; i++){
        featureList[i]["-1"] = FeatVec();
    }
    map<pair<string, string>, FeatVec> bigramFeatures;
    bigramFeatures[std::make_pair("-1", "-1")] = FeatVec();

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
            get_document_vector(indexes[i], intid, list_queryTermIDs[i], list_queryBigrams[i], list_id2stems[i], featureList, bigramFeatures);
        }

    }
    extidStream.close();
    for(i = 0; i < nRepos; i++){
        IndexEnvs[i].close();
        repos[i].close();
    }

    writeFeatures(featureList, outUnigramFile);
    writeBigramFeatures(bigramFeatures, outBigramFile);

    return 0;
}
