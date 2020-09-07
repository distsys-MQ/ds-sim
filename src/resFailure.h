#ifndef DS_SIM_RES_FAILURE_H
#define DS_SIM_RES_FAILURE_H
//********************** RESOURCE FAILURE HANDLING (written by Young Ki (YK) and ported by Jayden King (JK)): Start ********************
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

typedef struct {
	timeP tParam;
	nodeP nParam;
	char *model;
} ResFailureModel;

typedef struct {
		int inx;
		double pglognorm;
} PreGenLogNormal;
	
ResFailureModel resFailureModels[] = {{{0.688, 0.531}, {-1.408, 1.907, 0.418}, "teragrid"},
										{{2.442, 0.661}, {1.433, 2.022, 0.982}, "nd07cpu"},
										{{2.43, 0.30}, {1.65, 1.22, 1.0}, "websites02"},
										// following three added on 6th Aug 2019
										{{1.98, 0.76}, {0.27, 1.78, 0.519}, "ldns04"},
										{{2.59, 0.45}, {1.8, 1.75, 0.952}, "pl05"},
										{{2.45, 0.64}, {2.14, 1.18, 1.0}, "g5k06"}};

//********************** RESOURCE FAILURE HANDLING (YK & JK): End ********************

//********************** RESOURCE FAILURE HANDLING (YK & JK) ********************
//void distGen(timeP time, nodeP node, char* config, char* trace);
int getMaxIndex(double arr[], int n);
void lognormalTimeDist(double arr[], int n, double mu, double sigma);
void freeArr(int** arr, int n);
int cmpInt(const void* a, const void* b);
int cmpServerFailure(const void* structA, const void* structB);
void usage();
void printArr(double arr[], int n);

//********************** RESOURCE FAILURE HANDLING (Young) ********************
int CmpPreGenLN(const void *p1, const void *p2);
inline int GetFailureModel(char *fModelName);
//int GetFailureTrace(char *fTraceName);
inline int GetServProp(int no, unsigned short *sID);
int *GetRandomServIndices(int rawTotalServers, int totalServers);
int GenerateResFailures(int fModel);
void SkipComments(FILE *f);
int ReadResFailure(FILE *file, ResFailure *resFail);
void AddResRecovery(ResFailure *resFail, ResFailure *resRecover);
int CmpResFailures(const void *p1, const void *p2);
int LoadResFailures(char *failFileName);
//********************** RESOURCE FAILURE HANDLING: End ********************

#endif
