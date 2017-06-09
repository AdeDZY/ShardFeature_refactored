//
// Created by Zhuyun Dai on 6/8/17.
//

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

void readQueriesToStems(const char *queryFile, indri::index::Index * index, indri::collection::Repository & repo){
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
            string stem = repo.processTerm(term);
            cout<<stem<<" ";
        }
	cout<<endl;
    }
}


int main(int argc, char **argv){
    string repoPath = argv[1];
    std::string queryFile = argv[2]; // each line is a query, seperated by space

    // open indri indexes
    indri::index::Index *index;
    indri::collection::Repository repo;
    QueryEnvironment IndexEnv;
    IndexEnv.addIndex (repoPath);
    indri::collection::Repository::index_state state;
    repo.openRead(repoPath);
    state = repo.indexes();
    index = (*state)[0];

    // read query terms

    readQueriesToStems(queryFile.c_str(), index, repo);

    return 0;
}
