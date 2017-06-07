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
#include "indri/Repository.hpp"
#include <vector>
#include "indri/CompressedCollection.hpp"
#include "indri/LocalQueryServer.hpp"

using namespace std;
using namespace indri::api;



void readQueryTerms(set<pair<int, int> > &queryTerms, const char *queryFile, indri::index::Index * index, indri::collection::Repository & repo){
	queryTerms.clear();
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
				queryTerms.insert(std::make_pair<int, int>(termIds[i], termIds[i + 1])) ;
				cout<<termIds[i]<<"_"<<termIds[i + 1]<<endl;
			}
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
                         const set<pair<int, int> > &queryTerms,
                         map<pair<int, int>, FeatVec> &features) {

    map<pair<int, int>, int> docVec;

    const indri::index::TermList *list = index->termList(docid);
    indri::utility::greedy_vector <int> &terms = (indri::utility::greedy_vector <int> &) list->terms();

    // the whole documents
    int docLen = 0;
    docVec.clear();
    for (int t = 0; t < terms.size() - 1; t++)
    {

        docLen++;

        if (terms[t] <= 0) // [OOV]
            continue;
		if (terms[t + 1] <= 0)
			continue;

		std::pair<int, int> p = std::make_pair(terms[t], terms[t + 1]);
        if (queryTerms.find(p) == queryTerms.end())  // not query term
            continue;

        if (docVec.find(p) != docVec.end()) {
            docVec[p]++;
        }
        else {
            docVec[p] = 1;
        }

    }

	if(docLen <= 0){
		cout<<"docLen = 0: "<< docid<<endl;
		return;
	}
    // update feature
    map<pair<int, int>, int>::iterator it;
    int  freq;
	pair<int, int> bigram;
    for(it = docVec.begin(); it != docVec.end(); it++){
        bigram = it->first;
        freq = it->second;
        if(features.find(bigram) == features.end())
            features[bigram] = FeatVec(1, freq/double(docLen), freq);
        else
            features[bigram].updateFeature(freq, docLen);
    }

	// to get shard size and total term freq in shards
	features[std::make_pair(-1, -1)].updateFeature(docLen, docLen);

    // Finish processing this doc

    delete list;
    list = NULL;
    terms.clear();

}

void writeFeatures(const map<pair<int, int>, FeatVec> &features,
                   const string outFile){

    ofstream outStream;
    outStream.open(outFile.c_str());

    map<pair<int, int>, FeatVec>::const_iterator it;
    for(it = features.begin(); it != features.end(); it++){
        outStream<<(it->first).first<<"_"<<(it->first).second;
        outStream<<" ";
        outStream<<it->second.df<<" "<<it->second.sum_tf<<" "<<it->second.sum_prob<<endl;
    }
    outStream.close();
}

int main(int argc, char **argv){
    string repoPath = argv[1];
    string extidFile = argv[2];
    string outFile = argv[3];
    std::string queryFile = argv[4];

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
    set<pair<int, int> > queryTerms;
    readQueryTerms(queryTerms, queryFile.c_str(), index, r);

    // Features
    map<pair<int, int>, FeatVec> features;
	features[std::make_pair(-1, -1)] = FeatVec();


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
