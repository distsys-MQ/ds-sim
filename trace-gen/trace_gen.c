#include <assert.h>
#include <math.h>
#include <libxml/parser.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "log_normal.h" // https://people.sc.fsu.edu/~jburkardt/c_src/log_normal/log_normal.html

typedef struct timeP {
    double mean;
    double stdev;
} timeP;

typedef struct nodeP {
    double mean;
    double stdev;
    double failureRatio;
} nodeP;

typedef struct serverFailure {
    const char* sType;
    int nID;
    int sID;
    int startTime;
    int endTime;
} serverFailure;


static const timeP TERAGRID_TIME = {0.688, 0.531};
static const nodeP TERAGRID_NODE = {-1.408, 1.907, 0.418};

static const timeP ND07CPU_TIME = {2.442, 0.661};
static const nodeP ND07CPU_NODE = {1.433, 2.022, 0.982};

static const timeP WEBSITES02_TIME = {2.43, 0.30};
static const nodeP WEBSITES02_NODE = {1.65, 1.22, 1.0};


void distGen(timeP time, nodeP node, char* config, char* trace);
int getMaxIndex(double arr[], int n);
void lognormalTimeDist(double arr[], int n, double mu, double sigma);
void freeArr(int** arr, int n);
int cmpInt(const void* a, const void* b);
int cmpServerFailure(const void* structA, const void* structB);
void usage();
void printArr(double arr[], int n);


int main (int argc, char* argv[]) {
    if (argc != 3) {
        usage();
    }
    
    timeP timeStat;
    nodeP nodeStat;
    
    if (!strcmp(argv[1], "teragrid")) {
        timeStat = TERAGRID_TIME;
        nodeStat = TERAGRID_NODE;
    } else if (!strcmp(argv[1], "nd07cpu")) {
        timeStat = ND07CPU_TIME;
        nodeStat = ND07CPU_NODE;
    } else if (!strcmp(argv[1], "websites02")) {
        timeStat = WEBSITES02_TIME;
        nodeStat = WEBSITES02_NODE;
    } else {
        usage();
    }
    
    if (access(argv[2], R_OK) == -1) {
        fprintf(stderr, "Cannot read '%s'!\n", argv[2]);
        exit(1);
    }
    
    srand(1234);
    
    time_t startTime;
    time(&startTime);
    
    distGen(timeStat, nodeStat, argv[1], argv[2]);
    
    time_t endTime;
    time(&endTime);
    double diff = difftime(endTime, startTime);
    printf("Execution time: %.2f seconds\n", diff);
    
    return EXIT_SUCCESS;
}

// make_and_save_distribution
void distGen(timeP time, nodeP node, char* trace, char* config) {
    printf("Start making failure trace file based on %s and %s distribution.\n", config, trace);
    
    const char* sTypes[] = {"tiny", "small", "medium", "large", "xlarge", "2xlarge", "3xlarge", "4xlarge", "5xlarge", "6xlarge"};
    int sTypeNum = 10;
    
    int sLimit = 10;
    int rawTotalServers = sTypeNum * sLimit;
    int totalTime = 86400;
    
    int sIDs[rawTotalServers];
    
    for (int i = 0; i < sLimit; i++) {
        sIDs[i] = i;
    }
    
    int totalServers = rawTotalServers * node.failureRatio;
    int totalTimeMin = (totalTime / 60.0) + 1;
    
    // pre_generate_lognormal_time_distribution
    int maxNum = ((totalServers/10) > 10) ? (totalServers/10) : 10;
    double** preGenLognorm = calloc(maxNum, sizeof(*preGenLognorm));
    
    for (int i = 0; i < maxNum; i++) {
        preGenLognorm[i] = calloc(totalTimeMin, sizeof(*preGenLognorm[i]));
        lognormalTimeDist(preGenLognorm[i], totalTimeMin, time.mean, time.stdev);
    }
    
    // get_top_n
    int** maxIndexes = calloc(maxNum, sizeof(*maxIndexes));
    
    for (int i = 0; i < maxNum; i++) {
        maxIndexes[i] = calloc(totalTimeMin, sizeof(*maxIndexes[i]));
        
        for (int k = 0; k < totalTimeMin; k++) {
            int max = getMaxIndex(preGenLognorm[i], totalTimeMin);
            maxIndexes[i][k] = max;
            preGenLognorm[i][max] = -1;
        }
        free(preGenLognorm[i]);
    }
    free(preGenLognorm);
    
    // get_lognormal_time_distribution
    double nodeD[totalServers];
    double nodeMean = 0.0;
    double nodeStdev = 0.0;
    double sum = 0.0;
    
    lognormalTimeDist(nodeD, totalServers, node.mean, node.stdev);
    
    for (int i = 0; i < totalServers; i++) {
        sum += nodeD[i];
        //~ printf("%f\n", nodeD[i]);
    }
    
    nodeMean = sum / (totalServers * 1.0);
    double variance = 0.0;
    
    for (int i = 0; i < totalServers; i++) {
        variance += pow(nodeD[i] - nodeMean, 2);
    }
    nodeStdev = sqrt(variance / (totalServers * 1.0));
    
    //~ printf("node mean: %f\n", nodeMean);
    //~ printf("node stdev: %f\n", nodeStdev);
    
    long totalFailedTime = 0;
    int** nodeDArr = calloc(totalServers, sizeof(*nodeDArr));
    int ftCount[totalServers];
    
    for (int i = 0; i < totalServers; i++) {
        int failedTime = ((totalTimeMin * nodeD[i]) / 100);
        
        if (failedTime > totalTimeMin) {
            failedTime = totalTimeMin;
        }
        totalFailedTime += failedTime;
        ftCount[i] = failedTime;
        
        nodeDArr[i] = calloc(failedTime, sizeof(*nodeDArr[i]));
        int randIndex = rand() % maxNum;
        
        for (int k = 0; k < failedTime; k++) {
            nodeDArr[i][k] = maxIndexes[randIndex][k];
        }
        qsort(nodeDArr[i], failedTime, sizeof(*nodeDArr[i]), cmpInt);
    }
    freeArr(maxIndexes, maxNum);
    
    // Seems to be slightly higher than the actual number of generated failures
    int totalFailures = 0;
    for (int i = 0; i < totalServers; i++) {
        totalFailures += ftCount[i];
    }
    
    serverFailure** serverFailures = calloc(totalFailures, sizeof(**serverFailures));
    int sFailIndex = 0;
    
    // make_failure_trace_like_fta_format
    for (int i = 0; i < totalServers; i++) {
        int ftNum = ftCount[i];
        int nodeDIndex = 0;
        int startSecondIndex = 0;
        
        while (ftNum > 0) {
            int ftDurNum = 0;
            int* ftDur = calloc(totalFailures, sizeof(*ftDur));
            int ft = nodeDArr[i][nodeDIndex++];
            ftNum--;
            startSecondIndex = ftDurNum;
            ftDur[ftDurNum++] = ft;
            
            while (ftNum > 0) {
                if (ft + 1 == (int) nodeDArr[i][nodeDIndex]) {
                    ft = nodeDArr[i][nodeDIndex++];
                    ftNum--;
                    ftDur[ftDurNum++] = ft;
                } else {
                    break;
                }
            }
            
            if (ftDurNum > 0) {
                serverFailure* sf = calloc(1, sizeof(*sf));
                
                sf->sType = sTypes[i / sLimit];
                sf->nID = i;
                sf->sID = sIDs[i % sLimit];
                sf->startTime = ftDur[startSecondIndex] * 60;
                sf->endTime = ftDur[ftDurNum - 1] * 60 + 59;
                
                serverFailures[sFailIndex++] = sf;
                
                //~ int diff = sf->endTime - sf->startTime;
                //~ if (diff > 1000) {
                    //~ printf("%d: %d\n", sFailIndex, diff);
                //~ }
                //~ printf("%d %d %s %d %d\n", sf->startTime, sf->endTime, sf->sType, sf->nID, sf->sID);
            }
            free(ftDur);
        }
    }
    freeArr(nodeDArr, totalServers);
    
    printf("totalFailures: %d\n", totalFailures);
    printf("sFailIndex: %d\n", sFailIndex);
    
    totalFailures = sFailIndex;
    serverFailure** serverFailuresNoNull = calloc(totalFailures, sizeof(**serverFailures));
    memcpy(serverFailuresNoNull, serverFailures, sizeof(*serverFailuresNoNull) * totalFailures);
    qsort(serverFailuresNoNull, totalFailures, sizeof(*serverFailures), cmpServerFailure);
    
    for (int i = 0; i < totalFailures; i++) {
        serverFailure* sf = serverFailuresNoNull[i];
        
        if (sf) {
            printf("%d %d %s %d %d\n", sf->startTime, sf->endTime, sf->sType, sf->nID, sf->sID);
            free(sf);
        } else {
            printf("test\n");
        }
    }
    free(serverFailures);
    free(serverFailuresNoNull);
}

int getMaxIndex(double arr[], int n) {
    int max = 0;
    
    for (int i = 0; i < n; i++) {
        if (arr[i] > arr[max]) {
            max = i;
        }
    }
    return max;
}

void lognormalTimeDist(double arr[], int n, double mu, double sigma) {
    int* s = calloc(1, sizeof(*s));
    *s = rand();
    
    for (int i = 0; i < n; i++) {
        arr[i] = log_normal_sample(mu, sigma, s);
    }
    free(s);
}

void freeArr(int** arr, int n) {
    for (int i = 0; i < n; i++) {
        free(arr[i]);
    }
    free(arr);
}

int cmpInt(const void* a, const void* b) {
    return (*(int*) a - *(int*) b);
}

int cmpServerFailure(const void* structA, const void* structB) {
    serverFailure* a = *(serverFailure**) structA;
    serverFailure* b = *(serverFailure**) structB;
    
    if (a->startTime > b->startTime) {
        return 1;
    } else if (a->startTime < b->startTime) {
        return -1;
    } else {
        return 0;
    }
}

void usage() {
    fprintf(stderr, "usage: ./trace-gen BASE_TRACE (teragrid | nd07cpu | websites02) CONFIGS_XML\n");
    exit(1);
}

void printArr(double arr[], int n) {
    for (int i = 0; i < n; i++) {
        printf("%.2f ", arr[i]);
    }
    printf("\n\n");
}
