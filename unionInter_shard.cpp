//
// Created by Zhuyun Dai on 4/11/16.
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

void readQueries(unordered_set<int> &queryTerms,
                 vector<vector<int> > &queries,
                 const char *queryFile,
                 indri::index::Index * index,
                 indri::collection::Repository & repo){

    queryTerms.clear();
    queries.clear();
	ifstream queryStream;
	queryStream.open(queryFile);

	int termID;
	string line;
	int qid = 0;
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

		string term;
		stringstream ss;
		ss.str(line);
		queries.push_back(vector<int>());
		while(!ss.eof()){
			ss>>term;
			string stem = repo.processTerm(term);
			termID = index->term(stem);
			if (termID > 0){
			    queryTerms.insert(termID);
			    queries[qid].push_back(termID);
			}
		}
		qid += 1;
	}
	queryStream.close();
}


void get_document_vector(indri::index::Index *index,
                         const int docid,
                         unordered_set<int> &queryTerms,
                         vector<vector<int> > &queries,
                         vector<vector<int> > &res_inter,
                         vector<vector<int> > &res_union,
                         const int shardid) {

    unordered_map<int, int> docVec;
    unordered_map<int, int>::iterator docVecIt;

    const indri::index::TermList *list = index->termList(docid);
    indri::utility::greedy_vector <int> &terms = (indri::utility::greedy_vector <int> &) list->terms();

    // the whole documents
    int termID;
    bool has_all, has_one;
    docVec.clear();
    for (int t = 0; t < terms.size(); t++)
    {

        if (terms[t] <= 0) // [OOV]
            continue;

        if (queryTerms.find(terms[t]) == queryTerms.end())  // not query term
            continue;

        docVec[terms[t]] = 1;
    }

	if(docVec.size() == 0){
    	delete list;
    	list = NULL;
    	terms.clear();
		return;
	}
    // update union and intersection
    for (int q = 0; q < queries.size(); q++)
    {
        has_all = true;
        has_one = false;
        for (int i = 0; i < queries[q].size(); i++)
        {
            termID = queries[q][i];
            if (docVec.find(termID) == docVec.end())
                has_all = false;
            else
                has_one = true;
        }
        if(has_all)
            res_inter[shardid][q] += 1;
        if(has_one)
            res_union[shardid][q] += 1;
    }

    // Finish processing this doc

    delete list;
    list = NULL;
    terms.clear();
}

void writeResults(const vector<vector<int> > &res_inter,
                  const vector<vector<int> > &res_union,
                  const string outFile,
                  const int nShards){

    ofstream outStream;
    outStream.open(outFile.c_str());

    for(int q = 0; q < res_union[0].size(); q++){
        for(int s = 0; s < nShards; s++){
        	outStream<<q + 1;
        	outStream<<" "<<s + 1<<" ";
            outStream<<res_inter[s][q]<<" "<<res_union[s][q]<<endl;
        }
    }
    outStream.close();
}

int main(int argc, char **argv){
    string repoPath = argv[1];
    string extidFile = argv[2];
    string outFile = argv[3];
    std::string queryFile = argv[4];
    string nShardsStr = argv[5];

    int nShards = atoi(nShardsStr.c_str());
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

    // read queries
    unordered_set<int> queryTerms;
    vector<vector<int> > queries;
    readQueries(queryTerms, queries, queryFile.c_str(), index, r);

    // results
    vector<vector<int> > res_inter(nShards);
    vector<vector<int> > res_union(nShards);
    for(int s = 0; s < nShards; s++)
    {
        res_inter[s].resize(queries.size());
        res_union[s].resize(queries.size());
    }

    vector <string> extids;
    vector <int> intids;
    vector<int> shardids;
    int intid = 0;
    string extid;
    string prevExtid = "";
    string outLine;
    string shardid;
    int indexId = 0;
    int ndoc = 0;
    while(!extidStream.eof()){
        extidStream>>extid;
        extidStream>>shardid;
        extids.push_back(extid);
        shardids.push_back(atoi(shardid.c_str()) - 1);
    }
    extidStream.close();

    intids = IndexEnv.documentIDsFromMetadata("docno", extids);
    for(int i = 0; i < intids.size(); i++){
        intid = intids[i];
        get_document_vector(index, intid, queryTerms, queries, res_inter, res_union, shardids[i]);
        if(i%1000 == 0)
            cout<<i<<endl;
    }

    IndexEnv.close();
    r.close();

    writeResults(res_inter, res_union, outFile, nShards);

    return 0;
}
