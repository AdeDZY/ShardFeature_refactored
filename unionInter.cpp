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
                         vector<int> &res_inter,
                         vector<int> &res_union) {

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
            res_inter[q] += 1;
        if(has_one)
            res_union[q] += 1;
    }

    // Finish processing this doc

    delete list;
    list = NULL;
    terms.clear();
}

void writeResults(const vector<int> &res_inter,
                  const vector<int> &res_union,
                  const string outFile){

    ofstream outStream;
    outStream.open(outFile.c_str());


    for(int q = 0; q < res_union.size(); q++){
        outStream<<q;
        outStream<<" ";
        outStream<<res_inter[q]<<" "<<res_union[q]<<endl;
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

    // read queries
    unordered_set<int> queryTerms;
    vector<vector<int> > queries;
    readQueries(queryTerms, queries, queryFile.c_str(), index, r);

    // results
    vector<int> res_inter(queries.size());
    vector<int> res_union(queries.size());

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
        get_document_vector(index, intid, queryTerms, queries, res_inter, res_union);
    }

    IndexEnv.close();
    r.close();

    writeResults(res_inter, res_union, outFile);

    return 0;
}
