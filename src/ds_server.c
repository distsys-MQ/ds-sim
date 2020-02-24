/*********************************************************************
	Server of ds-sim
	Department of Computing, Macquarie University, Australia
	Initiated by Young Choon Lee, 2019 - 2020
	
	TODO:
		- log file (ds-sim.log) containing scheduling info per server and stats (DONE)
		- xml error checking (e.g., checking required attributes, such as type in server, job and workload) (partially CHECKED)
		- consider to increase max job count from SHRT_MAX (32767) to INX_MAX (DONE)
		- consider resource failures to occur until the end of simulation; (DONE by generating failures for 50% longer time
		  as some jobs have to re-run, the actual duration of simulation (that failures are generated based on) is longer than the end time of failure generation
		- consider not to send RESF & RESR messages; (DONE by adding -a option)
		- investigate the frequency of server failures (currently, it looks like there are a bit too many failures) (DONE by adding -s option)
		- implement TERM (DONE; need more testing) and KILJ (DONE) for server and job, respectively; TERM kills all jobs (running&waiting jobs) 
		- Add 'RESC Capable #cores mem disk' that returns all resources that have sufficient capacities to accommodate a job with specified capacities regardless of their availability. With this command, an Active server with insufficient resources will return its available time being -1 and that with sufficient resources will return its actual available time (i.e., the current time); this will avoid to use 'RESC All' that returns availabile time of -1 when a server is Active regardless of its current resource availability
		- handle unsorted list of server types for RESC capable (this is currently for speeding up the simulation purposes only)
		- handle the situation where the job can't be scheduled because all capable servers have been failed 
		(may implement 'NXTJ' to get the next job and put the current job after that job) (DONE; needs thorough testing)
		
		- !! job generation: 
			> more "randomise" (avoid strong relationship) between runtime and resource requirements
			> consider the total number of cores in the system and load paramters

		- consider the update of server state takes place in one coherent place (MoveSchedJobState?)
		- tidy up the main function w.r.t. different commands
		- relative path for failure trace file and job file (mayby use of an env variable like CLASSPATH in Java)
		- implement MIGJ (job migration, probably with some migration overhead)
		
*********************************************************************/
#include <stdio.h>
#include <ctype.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <string.h>
#include <netinet/in.h> //For the AF_INET (Address Family)
#include <sys/types.h>
#include <unistd.h> 	// For fork
#include <assert.h>
#include <limits.h>
#include <errno.h>
#include <signal.h>
#include <libxml/parser.h>
#include <math.h>
#include <time.h>

#include "log_normal.h" // https://people.sc.fsu.edu/~jburkardt/c_src/log_normal/log_normal.html

//#define DEBUG

#define VERSION						"14-Feb, 2020 @ MQ"
#define DEVELOPERS					"Young Choon Lee, Jayden King and Young Ki Kim"
#define UNDEFINED					-1
#define UNKNOWN						UNDEFINED
#define TRUE						1
#define FALSE						0
#define FORWARD						1
#define BACKWARD					-1
#define INCREASE					1
#define DECREASE					-1
#define NUM_RES_FAILURE_FIELDS		4
#define DEFAULT_BUF_SIZE			256
#define MAX_BUF						1024
#define MAX_NAME_LENGTH				256
#define DEFAULT_LOG_CNT				1000
#define DEFAULT_RES_FAILURES		DEFAULT_LOG_CNT
#define DEFAULT_PORT				50000			// Ports 49152-65535
#define MAX_CONNECTIONS				10
#define DEFAULT_JOB_CNT				1000
#define DEFAULT_MAX_JOB_RUNTIME		(60*60*24*7)	// 7 days 
#define MAX_JOB_RUNTIME				(60*60*24*30)	// 30 days in seconds
#define MAX_SIM_END_TIME			MAX_JOB_RUNTIME
#define DEFAULT_SIM_END_TIME		DEFAULT_MAX_JOB_RUNTIME
#define MIN_IN_SECONDS				60
#define HOUR_IN_SECONDS 			(60*60)			// in seconds
#define TWELVE_HOURS				(60*60*12)
#define ONE_DAY_IN_SEC				(60*60*24)
#define DEFAULT_RES_FAIL_T_GRAIN	(MIN_IN_SECONDS * 2)	// two mins
#define DEFAULT_RND_SEED			1024
#define DEFAULT_RES_LIMIT			20
#define MIN_MEM_PER_CORE			1000			// 1GB
#define MIN_DISK_PER_CORE			2000			// 2GB
#define MIN_MEM_PER_JOB				100				// 100MB
#define MIN_MEM_PER_JOB_CORE		MIN_MEM_PER_JOB
#define MIN_DISK_PER_JOB_CORE		MIN_MEM_PER_JOB
#define DEFAULT_BASE				10
#define ENDTIME_STR					"endtime"
#define JCNT_STR					"jobcount"
#define JOB_RIGID					1
#define JOB_MALLEABLE				2
//#define MAX_RUN_TIME			DEFAULT_SIM_END_TIME // max run time should be bounded by simulation end time; 
													 // may be specified in three categories: 
													 // short, medium and long (ratio, min and max), possibly DAG jobs
#define MAX_RUN_TIME_EST_ERR		100				// in percentage
#define MAX_REL_SPEED				100				// used to generate relative CPU speed between 0.1 and 10.0; may be used for extension
#define DEFAULT_BOOTUP_TIME			60				// 60 seconds
#define SYS_INFO_FILENAME			"system.xml"
#define JOB_FILENAME				"ds-jobs.xml"
#define RES_FAILURE_FILENAME		"res-failures.txt"
#define END_DATA					"."

#define BOUND_INT_MAX(x)			((x) < 0 ? INT_MAX : (x))	// when positive overflow, limit the value (x) to INT_MAX
#define ROUND(x)					((int)((x) + 0.5))
#define MIN(x,y)					((x) < (y) ? (x) : (y))
#define MAX(x,y)					((x) > (y) ? (x) : (y))

enum Verbose {VERBOSE_STATS = 1, VERBOSE_BRIEF, VERBOSE_ALL};

enum LimitName {Port_Limit, JCnt_Limit, JRunTime_Limit, JPop_Limit, SEnd_Limit, 
				SType_Limit, Res_Limit, SBootTime_Limit, RSeed_Limit, T_Grain_Limit, F_Scale_Limit, 
				CCnt_Limit, Mem_Limit, Disk_Limit, WMinLoad_Limit, WMaxLoad_Limit, JType_Limit, WorkloadTime_Limit};

enum HelpOption {H_All, H_Usage, H_Limits, H_States};

enum JobState {Submitted, Waiting, Running, Suspended, Completed, Failed, Killed, END_JOB_STATE};

enum JobType {Instant_Job, Short_Job, Medium_Job, Long_Job, END_JOB_TYPE};

enum LoadState {Load_Low, Load_Transit_Low, Load_Transit_Med, Load_Transit_High, Load_High, END_LOAD_STATE}; // go back and forth

enum ServerType {Tiny, Small, Medium, Large, XLarge, XXLarge, XXXLarge, XXXXLarge, END_SERVER_TYPE};

enum ResState {Inactive, Booting, Idle, Active, Unavailable};

enum ResFailureEvent {RES_Failure, RES_Recovery};

enum ResFailureModelName {Teragrid, ND07CPU, Websites02, LDNS04, PL05, G5K06, Invalid_Res_Failure_Model}; 

enum ResSearch {All, Type, Avail};

enum SchdStatus {ValidSchd, InvalidJob, InvalidServType, InvalidServID, ServUnavailable, ServIncapable};

typedef struct {
	char *name;
	long int min;
	long int max;
	long int def;
} Limit;
		
typedef struct {
	char *desc;
	char optChar;
	int useArg;	// TRUE/FALSE
	char *optArg;
} CmdLineOption;

typedef struct {
	unsigned int simEndTime;
	unsigned int maxJobCnt;
} SimTermCondition;

typedef struct {
	unsigned short port;		// TCP/IP port number
	unsigned char configFile;	// flag to check if an xml configuration file has been specified
	char configFilename[MAX_NAME_LENGTH];
	unsigned char jobFile;		// flag if a job list file (.xml) is used
	char jobFileName[MAX_NAME_LENGTH];
	unsigned int rseed;
	unsigned short resLimit;	// only used for uniform resource limit set by the "-r" option
	SimTermCondition termination;
} SimConfig;

typedef struct {
	int cores;
	int mem;
	int disk;
	float relSpeed;
} ResCapacity;

typedef struct {
	unsigned int id;
	int type;
	int submitTime;
	int estRunTime;
	int actRunTime;
	ResCapacity	resReq;
} Job;

typedef struct {
	int type;
	char *name;
	int min;
	int max;
	int rate;
} JobTypeProp;

typedef struct {
	int state;
	char *stateStr;
} JobState;

typedef struct WaitingJob {
	int submitTime;				// resubmit time after failure
	Job *job;
	struct WaitingJob *next;
} WaitingJob;

typedef struct {
	enum SchdStatus statusID;
	char *status;
} SchduleStatus;

typedef struct SchedJob {
	int startTime;
	int endTime;
	enum JobState state;
	Job *job;
	struct SchedJob *next;
} SchedJob;

typedef struct {
	unsigned short type;		// e.g., Amazon EC2 m3.xlarge, c4.2xlarge, etc.
	char *name;
	unsigned short limit;		// the max number of servers
	unsigned short bootupTime;
	ResCapacity capacity;
	float rate;					// hourly rate in cents; however, the actual charge is based on #seconds
} ServerTypeProp;

typedef struct {
	int numFails;
	int totalFailTime;
	int mttf;	// mean time to failure
	int mttr;	// mean time to recovery
	int lastOpTime;	// last time of operation whether it's the start time of server or the time after recovery (i.e., resume)
} ServerFailInfo;

typedef struct {
	unsigned short type;
	unsigned short id;
	int availCores;
	int availMem;		// in MB
	int availDisk;		// in MB
	int startTime;
	int usedTime;		// in seconds; can be used for cost calculation and utilisation check
	ServerFailInfo failInfo;
	SchedJob *waiting;
	SchedJob *running;
	SchedJob *failed;
	SchedJob *suspended;
	SchedJob *completed;
	SchedJob *killed;
	enum ResState state;
} Server;

typedef struct {
	int state;
	char *stateStr;
} ServState;

typedef struct {
	int startTime;
	int endTime;
	unsigned short servType;
	unsigned short servID;
	enum ResFailureEvent eventType;		// currently, only two: FAILURE or RECOVERY 
} ResFailure;

typedef struct {
	int fModel;
	int timeGrain;
	int scaleFactor;
	char *ftFilename;
	int numFailues;
	int nextFailureInx;
	ResFailure *resFailures;
} ResFailureTrace;

typedef struct {
	unsigned char numServTypes;	// the number of server types
	unsigned char maxServType;	// largest server type used to generate job res requirements
	int totalNumServers;
	ServerTypeProp *sTypes;		// server properties per type
	Server **servers;
	ResFailureTrace *rFailT;	// resource failure traces
} System;

// load is estimated by CPU utilisation (i.e., #cores used/total #cores)
typedef struct {
	char type;					// not used in version 1; it may be used to indicate either rigid or malleable job
	char *name;					// workload type name ("type" attribute)
	unsigned char minLoad;		// in percentage
	unsigned char maxLoad;		// in percentage
	unsigned char loadOffset;	// load offset between min and max loads (i.e., transition period)
	int avgLowTime;				// in seconds
	int avgTransitTime;			// in seconds
	int avgHighTime;			// in seconds
	int numJobTypes;
	JobTypeProp *jobTypes;
	unsigned int numJobs;
	Job *jobs;
} Workload;

typedef struct {
	unsigned int time;
	char msg[DEFAULT_BUF_SIZE];
} Log;

typedef struct {
	int numServUsed;
	float *usage;
	float *rentalCost;
} Stats;

// global variables

Limit limits[] = {
					{"TCP/IP port range", 49152, 65535, DEFAULT_PORT},
					{"Max Job count", 1, INT_MAX, DEFAULT_JOB_CNT},
					{"Job run time", 1, MAX_JOB_RUNTIME, DEFAULT_MAX_JOB_RUNTIME},	
					{"Job population rate", 1, 100, 10},
					{"Simulation end time", 1, MAX_SIM_END_TIME, DEFAULT_SIM_END_TIME},
					{"The number of server types", 1, 100, END_SERVER_TYPE},
					{"Resource limit (#servers/type)", 1, 1000, 20},
					{"Server bootup time", 0, 600, DEFAULT_BOOTUP_TIME},	// instant - 10 mins
					{"Random seed limit", 1, UINT_MAX, DEFAULT_RND_SEED},
					{"Server failure time granularity", MIN_IN_SECONDS, HOUR_IN_SECONDS, DEFAULT_RES_FAIL_T_GRAIN},
					{"Server failure scale factor", 1, 100, 100},	// in percentage with respect to the original failure distribution
					{"Core count limit", 1, 256, 4},
					{"Memory limit", 1000, 8192000, 32000},
					{"Disk limit", 1000, 8192000, 64000},
					{"Min load limit", 1, 99, 10},
					{"Max load limit", 1, 100, 90},
					{"Job type limit", 1, CHAR_MAX, END_JOB_TYPE},
					{"Time limit of load state", HOUR_IN_SECONDS, TWELVE_HOURS, HOUR_IN_SECONDS},
					{NULL, 0, 0, 0}
				};
		
CmdLineOption cmdOpts[] = {
					{"a(ll): send all messages including (resource) failure/recovery notification", 'a', FALSE, NULL},
					{"c(onfiguration file): use of configuration file", 'c', TRUE, "configuration file name (.xml)"},
					{"d(uration): simulation duration/end time", 'd', TRUE, "n (in seconds)"},
					{"f(ailure): failure distribution model (e.g., teragrid, nd07cpu and websites02)", 'f', TRUE, "teragrid|nd07cpu|websits02|g5k06|pl05|ldns04"},
					{"g(ranularity): time granularity for resource failures", 'g', TRUE, "n (in second)"},
					{"h(elp): usage", 'h', TRUE, "all|usage|limits|states"},
					{"j(ob): set job count", 'j', TRUE, "n (max #jobs to generate)"},
					{"l(imit of #servers): the number of servers (uniform to all server types)", 'l', TRUE, "n"},
					{"n(ewline): newline character (\\n) at the end of each message", 'n', FALSE, NULL},
					{"p(ort number): TCP/IP port number", 'p', TRUE, "n"},
					{"r(andom seed): random seed", 'r', TRUE, "n (some integer)"},
					{"s(cale factor for resource failures): between 1 and 100; \n\t 1 for nearly no failures and 100 for original", 's', TRUE, "n (in percentage)"},
					{"v(erbose): verbose", 'v', TRUE, "all|brief|stats"},
					{NULL, ' ', FALSE, NULL}
				};
				
JobTypeProp defaultJobTypes[] = {
							{Instant_Job, "instant", 1, 60, 58},
							{Short_Job, "short", 61, 600, 30},
							{Medium_Job, "medium", 601, 3600, 10},
							{Long_Job, "long", 3601, MAX_JOB_RUNTIME, 2}
						};

					// memory size is dynamically set considering core count and disk size
					// with a range of [1000, 32000], between 1GB and 32GB 
					// per core with an inteval of 1GB
ServerTypeProp defaultServerTypes[] = {
							{Tiny, "tiny", UNKNOWN, DEFAULT_BOOTUP_TIME, {1, 1000, 4000, 1.0}, 0.1},
							{Small, "small", UNKNOWN, DEFAULT_BOOTUP_TIME, {2, 1000, 16000, 1.0}, 0.2},
							{Medium, "medium", UNKNOWN, DEFAULT_BOOTUP_TIME, {4, 1000, 64000, 1.0}, 0.4},
							{Large, "large", UNKNOWN, DEFAULT_BOOTUP_TIME, {8, 1000, 256000, 1.0}, 0.8},
							{XLarge, "xlarge", UNKNOWN, DEFAULT_BOOTUP_TIME, {16, 1000, 512000, 1.0}, 1.6},
							{XXLarge, "2xlarge", UNKNOWN, DEFAULT_BOOTUP_TIME, {32, 1000, 1024000, 1.0},3.2},
							{XXXLarge, "3xlarge", UNKNOWN, DEFAULT_BOOTUP_TIME, {64, 1000, 2048000, 1.0}, 6.4},
							{XXXXLarge, "4xlarge", UNKNOWN, DEFAULT_BOOTUP_TIME, {128, 1000, 4096000, 1.0}, 12.8}
						};

SchduleStatus SchdStatusList[] = {{ValidSchd, "OK"}, 
								{InvalidJob, "ERR: No such waiting job exists"},
								{InvalidServType, "ERR: No such server type exists"}, 
								{InvalidServID, "ERR: No such server exists (server id: out of bound)"},
								{ServUnavailable, "ERR: The specified server unavailable"},
								{ServIncapable, "ERR: Server incapable of running such a job"}};

ServState servStates[] = {{Inactive, "inactive"},
						{Booting, "booting"},
						{Idle, "idle"},
						{Active, "active"},
						{Unavailable, "unavailable"},
						{UNKNOWN, NULL}};


JobState jobStates[] = {{Submitted, "submitted"},
							{Waiting, "waiting"},
							{Running, "running"},
							{Suspended, "suspended"},
							{Completed, "completed"},
							{Failed, "failed"},
							{Killed, "killed"},
							{UNKNOWN, NULL}};

//********************** RESOURCE FAILURE HANDLING (Jayden): Start ********************
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

//********************** RESOURCE FAILURE HANDLING (Jayden): End ********************


SimConfig sConfig;
System systemInfo;
Workload workloadInfo;
WaitingJob *wJobs;
unsigned int curJobID;
unsigned int curSimTime;
unsigned int actSimEndTime;
int numJobsCompleted = 0;
int fd;
char *version = VERSION;
char *developers = DEVELOPERS;
int allMsg = FALSE;
int newline = FALSE;
int verbose = FALSE;
Log *schedOps;
int logCnt;

// function prototypes
void PrintWelcomeMsg(int argc, char **argv);
void InitSim();
void CreateResFailTrace();
inline void GracefulExit();
void CompleteRecvMsg(char *msg, int length);
void CompleteSendMsg(char *msg, int length);
void SendMsg(int conn, char *msg);
void RecvMsg(int conn, char *msg);
int GetIntValue(char *str, char *desc);
float GetFloatValue(char *str, char *desc);
inline int IsOutOfBound(int value, long int min, long int max, char *message);
int ConfigSim(int argc, char **argv);
void Help(int opt);
void ShowUsage();
void ShowLimits();
void ShowStates();
void CompleteConfig();
void CreateServerTypes();
int StoreServerType(xmlNode *node);
void ResizeServerTypes();
void InitServers(Server *servers, ServerTypeProp *sType);
void CreateServers();
void ResizeJobTypes();
void CreateJobTypes(int njTypes);
int StoreJobType(xmlNode *node);
int StoreWorkloadInfo(xmlNode *node);

//********************** RESOURCE FAILURE HANDLING (Jayden) ********************
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
void ReadResFailure(FILE *file, ResFailure *resFail);
void AddResRecovery(ResFailure *resFail, ResFailure *resRecover);
int CmpResFailures(const void *p1, const void *p2);
int LoadResFailures(char *failFileName);
//********************** RESOURCE FAILURE HANDLING: End ********************

int LoadSimConfig(xmlNode *node);
int ValidateJobRates();
int ReadSimConfig(char *filename);
void GenerateSystemInfo();
inline long int CalcTotalCoreCount();
inline long int CalcCoresInUse(int submitTime, Job *jobs, int jID);
inline int GetNextLoadDuration(enum LoadState lState);
inline int GetJobType();
int ReadJobs(char *jobFileName);
int LoadJobs(xmlNode *node);
int GetJobTypeByName(char *);
Job *GenerateJobs();
inline int GetNumLoadTransitions();
void GenerateWorkload();
int ValidateSystemInfo();
int ValidateWorkloadInfo();
void WriteSystemInfo();
void WriteJobs();
void WriteResFailures();
int HandleKilledJobs(SchedJob *sJob, Server *server, int killedTime);
int HandleFailedJobs(SchedJob *sJob, Server *server, int failedTime);
int HandlePreemptedJobs(SchedJob *sJob, Server *server, int eventType, int eventTime);
void HandlePreemptedJob(SchedJob *sJob, int eventType, int eventTime);
void FailRes(ResFailure *resFail);
void RecoverRes(ResFailure *resRecover);
int CheckForFailures(int nextFET);
int FreeWaitingJob(int jID);
WaitingJob *RemoveWaitingJob(int jID);
WaitingJob *GetWaitingJob(int jID);
WaitingJob *GetFirstWaitingJob();
void InsertWaitingJob(WaitingJob *wJob);
void AddToWaitingJobList(Job *job, int submitTime);
Job *GetNextJobToSched(int *submitTime);
SchedJob *RemoveSchedJob(SchedJob *sJob, Server *server);
void AddSchedJobToEnd(SchedJob *sJob, SchedJob **sJobList);
void MoveSchedJobState(SchedJob *sJob, int jStateNew, Server *server);
void UpdateServerCapacity(Server *server, ResCapacity *resReq, int offset);
SchedJob **GetCJobList(SchedJob *firstCJob);
int CmpSchedJobs(const void *p1, const void *p2);
int GetEST(Server *server, ResCapacity *resReq, SchedJob *completedSJob);
SchedJob *GetNextJobToRun(Server *server, int prevJobET);
int CmpLog(const void *p1, const void *p2);
int PrintLog();
int CompareEndTime(const void *p1, const void *p2);
void MergeSort(SchedJob **head, int (*lessThan)(const void *item1, const void *item2));
SchedJob *Divide(SchedJob *head);
SchedJob *Merge(SchedJob *head1, SchedJob *head2, int (*lessThan)(const void *item1, const void *item2));
void UpdateRunningJobs(Server *server);
unsigned int UpdateResStates();
int FindResTypeByName(char *name);
inline char *FindResTypeNameByType(int type);
void SendDataHeader(int conn);
inline int GetServerBootupTime(int type);
int SendResInfoAll(int conn);
int SendResInfoByType(int conn, int type, ResCapacity *resReq);
int SendResInfoByServer(int conn, int type, int id, ResCapacity *resReq);
int SendResInfoByAvail(int conn, ResCapacity *resReq);
int SendResInfoByCapacity(int conn, ResCapacity *resReq);
int SendResInfoByCapacityMin(int conn, ResCapacity *resReq);
int SendResInfo(int conn, char *buffer);
int SendJobsPerStateOnServer(int conn, SchedJob *sJob, int numMsgSent);
int SendJobsOnServer(int conn, char *buffer);
inline int CountJobs(SchedJob *sJob);
int SendJobCountOfServer(int conn, char *buffer);
inline int GetTotalEstRT(SchedJob *sJob);
int SendEstWTOfServer(int conn, char *buffer);
int BackFillJob(Job *job);
int TerminateServer(char *cmd);
SchedJob *FindSchedJobByState(int jID, enum JobState state, Server *server);
SchedJob *FindSchedJob(int jID, Server *server);
int KillJob(char *cmd);
int IsSchdValid(int jID, char *stName, int sID);
SchedJob **GetSchedJobList(int jState, Server *server);
int AssignToServer(SchedJob *sJob, Server *server);
int IsSufficientRes(ResCapacity *resReq, Server *server);
int IsServerCapable(ResCapacity *resReq, Server *server);
int ScheduleJob(int jId, int sType, int sID);
int CountWJobs();
void PrintUnscheduledJobs();
void PrintStats();
void FreeSystemInfo();
void FreeWorkloadInfo();
void FreeSchedJobs();
void FreeAll();

void SigHandler(int signo)
{
  if (signo == SIGINT)
	GracefulExit();
}

int main(int argc, char **argv)
{
	struct sockaddr_in serv;
	int conn;
	char buffer[MAX_BUF] = ""; 
	char username[MAX_BUF] = "";
	int submitTime;
	Job *lastJobSent = NULL;

	InitSim();

	if (!ConfigSim(argc, argv)) {
		printf("Usage: %s -h all\n", argv[0]);
		FreeAll();
		return 1;
	}

	CompleteConfig();

	if (!sConfig.configFile) {	// configuration file not specified
		GenerateSystemInfo();
		if (!sConfig.jobFile)
			GenerateWorkload();
	}
	else
	if (!ValidateSystemInfo() || (!sConfig.jobFile && !ValidateWorkloadInfo())) {
		FreeAll();
		return 1;
	}

	serv.sin_family = AF_INET;
	serv.sin_port = htons(sConfig.port); // set the port
	serv.sin_addr.s_addr = INADDR_ANY;
	assert((fd = socket(AF_INET, SOCK_STREAM, 0)) >= 0); // create a TCP socket

	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &(int){ 1 }, sizeof(int)) < 0)
		perror("setsockopt(SO_REUSEADDR | SO_REUSEPORT) failed");

	bind(fd, (struct sockaddr *)&serv, sizeof(serv)); //assigns the address specified by serv to the socket
	//listen(fd, MAX_CONNECTIONS); //Listen for client connections. Maximum 10 connections will be permitted.
	listen(fd, 1); //Listen for client connections. 
	PrintWelcomeMsg(argc, argv);
	//Now we start handling the connections.
	if ((conn = accept(fd, (struct sockaddr *)NULL, NULL)) < 0) {
        perror("accept"); 
		GracefulExit();
    }
	if (signal(SIGINT, SigHandler) == SIG_ERR)
		fprintf(stderr, "\ncan't catch SIGINT\n");
	
	// read "HELO"
	while (TRUE) {
		RecvMsg(conn, buffer);
		if (!strcmp(buffer, "HELO")) {
			strcpy(buffer, "OK");
			break;
		}
		fprintf(stderr, "ERR: invalid message (%s)!\n", buffer);
		strcpy(buffer, "ERR");
		
		SendMsg(conn, buffer);
	}
	SendMsg(conn, buffer);

	while (TRUE) {
		RecvMsg(conn, buffer);
		if (strlen(buffer) > 4 && !strncmp(buffer, "AUTH", 4)) {
			strncpy(username, &buffer[4], strlen(buffer) - 4);
			strcpy(buffer, "OK");
			printf("# Welcome %s!\n", username);
			WriteSystemInfo();
			printf("# The system information can be read from '%s'\n", SYS_INFO_FILENAME);
			break;
		}
		fprintf(stderr, "ERR: invalid message (%s)!\n", buffer);
		strcpy(buffer, "ERR");
		SendMsg(conn, buffer);
	}
	SendMsg(conn, buffer);

	while (TRUE) {
		RecvMsg(conn, buffer);
		if (!strcmp(buffer, "REDY"))
			break;
			
		fprintf(stderr, "ERR: invalid message (%s)!\n", buffer);
		strcpy(buffer, "ERR");
		SendMsg(conn, buffer);
	}
	// scheduling loop
	while (strcmp(buffer, "QUIT")) {
		Job *job;
		char cmd[MAX_BUF] = "";
		int failure = UNDEFINED;
		int nextFET = UNKNOWN;	// next failure event time
		
		if (!strncmp(buffer, "NXTJ", 4) && !lastJobSent)
			strcpy(buffer, "REDY");
			
		if (!strcmp(buffer, "REDY")) {
			while (!strcmp(buffer, "REDY")) {
				if (systemInfo.rFailT)
					failure = CheckForFailures(nextFET);
				if (failure != UNDEFINED) {	// there is a failure
					// if (allMsg) needs to be here to handle multiple resource failures with the same failure start time
					if (allMsg) {	// send failure/recovery message only -a is used
						ResFailure *resFail = &systemInfo.rFailT->resFailures[failure];
						
						if (resFail->eventType == RES_Failure)
							strcpy(cmd, "RESF");
						else
						if (resFail->eventType == RES_Recovery) 
							strcpy(cmd, "RESR");
						sprintf(buffer, "%s %s %hu %d", cmd, FindResTypeNameByType(resFail->servType), resFail->servID, resFail->startTime);
					}
					else
						failure = UNDEFINED;
				}
				else {
					job = GetNextJobToSched(&submitTime);
					if (!job) {	// no more jobs to schedule
						if (numJobsCompleted == workloadInfo.numJobs || !systemInfo.rFailT ||
							(systemInfo.rFailT && systemInfo.rFailT->nextFailureInx >= systemInfo.rFailT->numFailues))
							strcpy(buffer, "NONE");
						else 	// get the next simulation event (RESF/RESR) time
						if (systemInfo.rFailT)
							nextFET = systemInfo.rFailT->resFailures[systemInfo.rFailT->nextFailureInx].startTime;
					}
					else {
						if (job->submitTime == submitTime)	// normal job submission
							strcpy(cmd, "JOBN");
						else
						if (job->submitTime < submitTime)	// preempted job (failed/killed)
							strcpy(cmd, "JOBP");
						// cmd: 4 chars submit_time: int job_id: int estimated_runtime: int #cores_requested: int memory: int disk: int
						sprintf(buffer, "%s %d %d %d %d %d %d", cmd, submitTime, job->id, 
								job->estRunTime, job->resReq.cores, job->resReq.mem, job->resReq.disk);
						
						lastJobSent = job;

						UpdateResStates();
					}
				}
			}
		}
		else
		if (!strncmp(buffer, "SCHD", 4)) {
			int jID, sID, status;
			char stName[MAX_NAME_LENGTH];
			
			sscanf(buffer, "SCHD %d %s %d", &jID, stName, &sID);
			if ((status = IsSchdValid(jID, stName, sID)))
				sprintf(buffer, "%s", SchdStatusList[status].status);
			else {
				int sType = FindResTypeByName(stName);
				status = ScheduleJob(jID, sType, sID);
				sprintf(buffer, "%s", SchdStatusList[status].status);
				if (status == ValidSchd)	// job got scheduled successfully
					lastJobSent = NULL;
			}
		}
		else
		if (!strncmp(buffer, "RESC", 4)) {
			int ret;

			ret	= SendResInfo(conn, buffer);
			
			if (ret == UNDEFINED) {
				char temp[MAX_BUF];
				
				strcpy(temp, buffer);
				sprintf(buffer, "ERR: invalid resource infomation query (%s)!", buffer);
			}
			else
				strcpy(buffer, END_DATA);
		}
		else 
		if (!strncmp(buffer, "LSTJ", 4)) {
			int ret;
			
			ret = SendJobsOnServer(conn, buffer);
			
			if (ret == UNDEFINED) {
				char temp[MAX_BUF];
				
				strcpy(temp, buffer);
				sprintf(buffer, "ERR: invalid job listing query (%s)!", buffer);
			}
			else
				strcpy(buffer, END_DATA);
		}
		else 
		if (!strncmp(buffer, "CNTJ", 4)) {
			int ret;
			
			ret = SendJobCountOfServer(conn, buffer);
			
			if (ret == UNDEFINED) {
				char temp[MAX_BUF];
				
				strcpy(temp, buffer);
				sprintf(buffer, "ERR: invalid job count query (%s)!", buffer);
			}
			else
				sprintf(buffer, "%d", ret);
		}
		else 
		if (!strncmp(buffer, "EJWT", 4)) {	//	estimated job waiting time
			int ret;
			
			ret = SendEstWTOfServer(conn, buffer);
			
			if (ret == UNDEFINED) {
				char temp[MAX_BUF];
				
				strcpy(temp, buffer);
				sprintf(buffer, "ERR: invalid estimated waiting time query (%s)!", buffer);
			}
			else
				sprintf(buffer, "%d", ret);
		}
		else
		if (!strncmp(buffer, "NXTJ", 4)) {
			int ret;
			
			ret = BackFillJob(lastJobSent);
			if (ret == UNDEFINED) {
				fprintf(stderr, "Error: simulation can't continue at Job %d at %d\n", lastJobSent->id, curSimTime);
				break;
			}
			else {
				lastJobSent = NULL;
				continue;
			}
		}
		else
		if (!strncmp(buffer, "TERM", 4)) {
			int ret = TerminateServer(buffer);
			
			if (ret == UNDEFINED) // either incorrect format or unavailable/failed server specified
				sprintf(buffer, "ERR: invalid server termination command (%s)!", buffer);
			else
				sprintf(buffer, "%d jobs killed", ret);
		}
		else
		if (!strncmp(buffer, "KILJ", 4)) {
			int ret = KillJob(buffer);
			
			if (ret == UNDEFINED)
				sprintf(buffer, "ERR: invalid job termination command (%s)!", buffer);
			else
				sprintf(buffer, "job %d killed", ret);
		}
		else {
			char temp[MAX_BUF];
			
			strcpy(temp, buffer);
			sprintf(buffer, "ERR: invalid command (%s)", temp);
		}
		SendMsg(conn, buffer);
		memset(buffer, 0, sizeof(buffer));
		// message should be one of the following: SCHD, request resource information (RESC), REDY or NXTJ
		RecvMsg(conn, buffer);
	}

	// complete the execution of all scheduled jobs	
	curSimTime = UINT_MAX;
	curSimTime = UpdateResStates();
	
	strcpy(buffer, "QUIT");
	SendMsg(conn, buffer);
	
	if (wJobs || curJobID < workloadInfo.numJobs) { // scheduling incomplete
		fprintf(stderr, "%d jobs not scheduled!\n", workloadInfo.numJobs - curJobID + CountWJobs());
		if (verbose)
			PrintUnscheduledJobs();
	}
	close(fd);

	if (!sConfig.jobFile)
		WriteJobs();
	
	if (systemInfo.rFailT)
	/*if (systemInfo.rFailT->fModel > UNKNOWN && 
		systemInfo.rFailT->fModel < Invalid_Res_Failure_Model)*/
		WriteResFailures();
	
	PrintStats();
	
	FreeAll();
	
	return 0;
}

void PrintWelcomeMsg(int argc, char **argv)
{
	int i;
	char port[MAX_BUF];
	
	printf("# ds-sim server %s\n", version);
	printf("# Server-side simulator started with '");
	for (i = 0; i < argc; i++) {
		printf("%s", argv[i]);
		if (i + 1 < argc)
			printf(" ");
	}
	printf("'\n");
	sprintf(port, "%d", sConfig.port);
	printf("# Waiting for connection to port %s of IP address 127.0.0.1\n", port);
}

void InitSim()
{
	sConfig.port = limits[Port_Limit].def;
	sConfig.configFile = FALSE;
	sConfig.configFilename[0] = '\0';
	sConfig.jobFile = FALSE;
	sConfig.jobFileName[0] = '\0';
	sConfig.rseed = 0;
	sConfig.resLimit = 0;
	sConfig.termination.simEndTime = 
	sConfig.termination.maxJobCnt = 0;
	
	systemInfo.numServTypes = limits[SType_Limit].def;
	systemInfo.maxServType = UNKNOWN;
	systemInfo.totalNumServers = 0;
	systemInfo.sTypes = NULL;
	systemInfo.servers = NULL;
	systemInfo.rFailT = NULL;
	
	workloadInfo.type = UNDEFINED;
	workloadInfo.name = NULL;
	workloadInfo.minLoad = 
	workloadInfo.maxLoad = 
	workloadInfo.loadOffset = 0;
	workloadInfo.avgLowTime =
	workloadInfo.avgTransitTime = 
	workloadInfo.avgHighTime = 0;
	workloadInfo.numJobTypes = 0;
	workloadInfo.jobTypes = NULL;
	workloadInfo.numJobs = 0;
	workloadInfo.jobs = NULL;
	
	wJobs = NULL;
	
	curJobID = 0;
	curSimTime = 0;
	actSimEndTime = 0;
}


void CreateResFailTrace()
{
	systemInfo.rFailT = (ResFailureTrace *)malloc(sizeof(ResFailureTrace));
	ResFailureTrace *rFT = systemInfo.rFailT;
	
	assert(systemInfo.rFailT);
	
	rFT->fModel = UNDEFINED;
	rFT->timeGrain = limits[T_Grain_Limit].def;
	rFT->scaleFactor = limits[F_Scale_Limit].def;
	rFT->ftFilename = NULL;
	rFT->numFailues = 0;
	rFT->nextFailureInx = 0;
	rFT->resFailures = NULL;
}


inline void GracefulExit()
{
	close(fd);
	exit(EXIT_FAILURE);
}

void CompleteRecvMsg(char *msg, int length)
{
	if (newline) {
		if (msg && msg[length-1] != '\n') {
			msg[length] = '\0';
			fprintf(stderr, "No new line character at the end of %s!\n", msg);
			GracefulExit();
		}
		length--;
	}
	msg[length] = '\0';
}

void CompleteSendMsg(char *msg, int length)
{
	if (newline) {
		msg[length] = '\n';
		msg[length+1] = '\0';
	}
}

void SendMsg(int conn, char *msg)
{
	char *orgMsg = strdup(msg);
	
	CompleteSendMsg(msg, strlen(msg));
	if ((send(conn, msg, strlen(msg), 0)) < 0) {
		fprintf(stderr, "%s wasn't not sent successfully!\n", msg);
		GracefulExit();
	}
	if (verbose == VERBOSE_ALL ||
		(verbose == VERBOSE_BRIEF && 
			(!strncmp(orgMsg, "JOB", 3) || 
			!strcmp(orgMsg, "OK") || 
			!strcmp(orgMsg, "NONE") ||
			!strcmp(orgMsg, "QUIT")))) 
			printf("SENT %s\n", orgMsg);
		
	free(orgMsg);
}

void RecvMsg(int conn, char *msg)
{
	int recvCnt;
	
	recvCnt = read(conn , msg, MAX_BUF - 1);
	CompleteRecvMsg(msg, recvCnt);
	if (verbose == VERBOSE_ALL ||
		(verbose == VERBOSE_BRIEF && 
			(!strcmp(msg, "HELO") ||
			!strncmp(msg, "AUTH", 4) || 
			!strcmp(msg, "REDY") || 
			!strncmp(msg, "SCHD", 4) || 
			!strcmp(msg, "QUIT")))) 
			printf("RCVD %s\n", msg); 
}

int GetIntValue(char *str, char *desc)
{
	int value;
	char *endptr;
	
	if (!str) {
		fprintf(stderr, "Error: %s\n", desc);
		return UNDEFINED;
	}
	
	value = strtol(str, &endptr, DEFAULT_BASE);
	if (errno != 0) {
		perror("strtol");
		return UNDEFINED;
	}
	if (endptr == str) {
		fprintf(stderr, "No digits were found!\n");
		return UNDEFINED;
	}
	
	return value;
}

float GetFloatValue(char *str, char *desc)
{
	float value;
	char *endptr;
	
	if (!str) {
		fprintf(stderr, "Error: %s\n", desc);
		return FALSE;
	}
	
	value = strtof(str, &endptr);

	if (errno != 0) {
		perror("strtof");
		return UNDEFINED;
	}
	if (endptr == str) {
		fprintf(stderr, "No digits were found!\n");
		return UNDEFINED;
	}

	return value;
}

inline int IsOutOfBound(int value, long int min, long int max, char *message)
{
	int outOfBound = value < min || value > max;
	if (outOfBound)
		fprintf(stderr, "%d: Out of bound! (%ld - %ld); %s\n", value, min, max, message);
	
	return outOfBound;
}

int ConfigSim(int argc, char **argv)
{
	int c, i;
	int retValue;
	char optStr[MAX_BUF] = "";
	
	opterr = 0;
	errno = 0;

	// construct option string
	for (i = 0; cmdOpts[i].desc; i++) {
		sprintf(optStr, "%s%c", optStr, cmdOpts[i].optChar);
		if (cmdOpts[i].useArg)
			strcat(optStr, ":");
	}
	
	while ((c = getopt(argc, argv, optStr)) != -1)
		switch (c)
		{
			case 'h':	// usage
				if (!strcmp(optarg, "usage"))
					Help(H_Usage);
				else
				if (!strcmp(optarg, "limits"))
					Help(H_Limits);
				else
				if (!strcmp(optarg, "states"))
					Help(H_States);
				else
					Help(H_All);
				
				return FALSE;
			case 'v': 
				if (!strcmp(optarg, "all"))
					verbose = VERBOSE_ALL;
				else
				if (!strcmp(optarg, "brief"))
					verbose = VERBOSE_BRIEF;
				else
				if (!strcmp(optarg, "stats"))
					verbose = VERBOSE_STATS;
				else
					verbose = FALSE;
				break;
			case 'j':	// job count
				if ((retValue = GetIntValue(optarg, limits[JCnt_Limit].name)) == UNDEFINED)
					return FALSE;
				if (IsOutOfBound(retValue, limits[JCnt_Limit].min, limits[JCnt_Limit].max, limits[JCnt_Limit].name))
					return FALSE;
				sConfig.termination.maxJobCnt = retValue;
				break;
			case 'd':	// end time
				if ((retValue = GetIntValue(optarg, limits[SEnd_Limit].name)) == UNDEFINED)
					return FALSE;
				if (IsOutOfBound(retValue, limits[SEnd_Limit].min, limits[SEnd_Limit].max, limits[SEnd_Limit].name))
					return FALSE;
				sConfig.termination.simEndTime = retValue;
				break;
			case 'r':	// random seed
				if ((retValue = GetIntValue(optarg, limits[RSeed_Limit].name)) == UNDEFINED)
					return FALSE;
				if (IsOutOfBound(retValue, limits[RSeed_Limit].min, limits[RSeed_Limit].max, limits[RSeed_Limit].name))
					return FALSE;
				if (!sConfig.rseed) {
					sConfig.rseed = retValue;
					srand(sConfig.rseed);
				}
				break;
			case 'l':	// resource limit
				if ((retValue = GetIntValue(optarg, limits[Res_Limit].name)) == UNDEFINED)
					return FALSE;
				if (IsOutOfBound(retValue, limits[Res_Limit].min, limits[Res_Limit].max, limits[Res_Limit].name))
					return FALSE;
				sConfig.resLimit = retValue;
				break;
			case 'c':	// configuration file
				if (access(optarg, R_OK) == -1) {
					fprintf(stderr, "No such file (%s) exist!\n", optarg);
					return FALSE;
				}
				sConfig.configFile = TRUE;
				strcpy(sConfig.configFilename, optarg);
				break;
			case 'f':	// resource failure
					if (!systemInfo.rFailT)
						CreateResFailTrace();
					
					systemInfo.rFailT->fModel = GetFailureModel(optarg);
					
					if (systemInfo.rFailT->fModel == Invalid_Res_Failure_Model) {
						fprintf(stderr, "Invalid resource failure model!\n");
						return FALSE;
					}
				break;
			case 'p':	// port number
				if ((retValue = GetIntValue(optarg, limits[Port_Limit].name)) == UNDEFINED)
					return FALSE;
				if (IsOutOfBound(retValue, limits[Port_Limit].min, limits[Port_Limit].max, limits[Port_Limit].name))
					return FALSE;
				sConfig.port = retValue;
				break;
			case 'n':	// new line ('\n') at the end of each message
				newline = TRUE;
				break;
			case 'a':	// all messages including resource failure/recovery
				allMsg = TRUE;
				break;
			case 'g':
				if ((retValue = GetIntValue(optarg, limits[T_Grain_Limit].name)) == UNDEFINED)
					return FALSE;
				if (IsOutOfBound(retValue, limits[T_Grain_Limit].min, limits[T_Grain_Limit].max, limits[T_Grain_Limit].name))
					return FALSE;
				systemInfo.rFailT->timeGrain = retValue;
				break;
			case 's': // failure distribution scale factor
				if ((retValue = GetIntValue(optarg, limits[F_Scale_Limit].name)) == UNDEFINED)
					return FALSE;
				if (IsOutOfBound(retValue, limits[F_Scale_Limit].min, limits[F_Scale_Limit].max, limits[F_Scale_Limit].name))
					return FALSE;
				systemInfo.rFailT->scaleFactor = retValue;
				break;
			case '?':
				if (optopt == 'j' || optopt == 'd' || optopt == 's' || optopt == 'r' ||
					optopt == 'c' || optopt == 'p' || optopt == 'h' || optopt == 'g')
					fprintf(stderr, "Option -%c requires an argument.\n", optopt);
				else if (isprint(optopt))
					fprintf (stderr, "Unknown option `-%c'.\n", optopt);
				else
					fprintf (stderr, "Unknown option character `\\x%x'.\n", optopt);
				return FALSE;
			default:
				break;
				
		}

	if (sConfig.configFile) {
		if (!(sConfig.configFile = ReadSimConfig(sConfig.configFilename))) {
			fprintf(stderr, "Configuration file (%s) invalid!\n", sConfig.configFilename);
			return FALSE;
		}
	}

	return TRUE;
}

void Help(int opt)
{
	printf("ds-sim (ds-server: %s)\n\t by %s\n\n", version, developers);
	switch (opt) {
		case H_All:
			ShowUsage();
			ShowLimits();
			ShowStates();
			break;
		case H_Usage:
			ShowUsage();
			break;
		case H_Limits:
			ShowLimits();
			break;
		case H_States:
			ShowStates();
			break;
		default:
			break;
	}
	
}

void ShowUsage()
{
	printf("----------------------------------- [ Usage ] ----------------------------------\n");
	printf("For the (ds-sim) server side,\n");
	printf("ds-server [OPTION]...\n\n");
	printf("For the (ds-sim) client side,\n");
	printf("ds-client [OPTION]...\n\n");
	
	printf("---------------------- [ Server-side command line options ] --------------------\n");
	for (int i = 0; cmdOpts[i].desc; i++) {
		if (cmdOpts[i].useArg)
			printf("-%c %s\n", cmdOpts[i].optChar, cmdOpts[i].optArg);
		else
			printf("-%c\n", cmdOpts[i].optChar);
		printf("\t%s\n\n", cmdOpts[i].desc);
	}
	printf("---------------------- [ Client-side command line options ] --------------------\n");
	printf("-a scheduling algorithm name\n\n");
}

void ShowLimits()
{
	printf("----------------------------------- [ Limits ] ---------------------------------\n");
	for (int i = 0; limits[i].name; i++) 
		printf("%s\n\tmin: %ld, max: %ld and default: %ld\n\n", limits[i].name, limits[i].min, limits[i].max, limits[i].def);

}

void ShowStates()
{
	int i;
	
	printf("-------------------------------- [ Server states ] -----------------------------\n");
	for (i = 0; servStates[i].state != UNKNOWN; i++)
		printf("%d: %s\n", servStates[i].state, servStates[i].stateStr);
	
	printf("--------------------------------- [ Job states ] -------------------------------\n");
	for (i = 0; jobStates[i].state != UNKNOWN; i++)
		printf("%d: %s\n", jobStates[i].state, jobStates[i].stateStr);

}


void CompleteConfig()
{	
	// set max to one that has not been specified since simulation ends whichever condition meets first
	if (!sConfig.termination.simEndTime && !sConfig.termination.maxJobCnt)
		sConfig.termination.maxJobCnt = limits[JCnt_Limit].def;
	if (!sConfig.termination.simEndTime)
		sConfig.termination.simEndTime = limits[SEnd_Limit].max;
	if (!sConfig.termination.maxJobCnt)
		sConfig.termination.maxJobCnt = limits[JCnt_Limit].max;
}

void CreateServerTypes(int nsTypes) 
{
	systemInfo.servers = (Server **)calloc(nsTypes, sizeof(Server *));	// allocate mem and initialise
	assert(systemInfo.servers);
	systemInfo.sTypes = (ServerTypeProp *)calloc(nsTypes, sizeof(ServerTypeProp));
	assert(systemInfo.sTypes);
}

// store server properties per type
int StoreServerType(xmlNode *node)
{
	ServerTypeProp *sTypes = systemInfo.sTypes;
	int i, retValue, nsTypes = systemInfo.numServTypes;
	static int maxCoreCnt = 0;
	ResCapacity *capacity;
	
	for (i = 0; i < nsTypes && sTypes[i].name; i++); // find the next available server type
	if (i >= limits[SType_Limit].max) {	// too many server types specified
		fprintf(stderr, "Too many server types (max: %ld)\n", limits[SType_Limit].max);
		return FALSE;
	}
	
	sTypes[i].type = i;
	
	if (!(char *)xmlGetProp(node, (xmlChar *)"type")) {	// NULL; no 'type' attribute specified
		fprintf(stderr, "no \"type\" attribute specified in \"server\" element %d!\n", i+1);
		return FALSE;
	}
	sTypes[i].name = strdup((char *)xmlGetProp(node, (xmlChar *)"type"));
	
	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"limit"), "Resource limit")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[Res_Limit].min, limits[Res_Limit].max, limits[Res_Limit].name))
		return FALSE;
	sTypes[i].limit = retValue;
	
	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"bootupTime"), "Bootup time")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[SBootTime_Limit].min, limits[SBootTime_Limit].max, limits[SBootTime_Limit].name))
		return FALSE;
	sTypes[i].bootupTime = retValue;
	
	if ((sTypes[i].rate = GetFloatValue((char *)xmlGetProp(node, (xmlChar *)"hourlyRate"), "Server rental rate")) == UNDEFINED)
		return FALSE;
	
	capacity = &sTypes[i].capacity;
	
	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"coreCount"), "Core count")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[CCnt_Limit].min, limits[CCnt_Limit].max, limits[CCnt_Limit].name))
		return FALSE;
	capacity->cores = retValue;
	
	// max server type is determined by the number of cores; 
	// memory and disk are supposed to be "proportionally" set based on core count
	if (capacity->cores > maxCoreCnt) {
		maxCoreCnt = capacity->cores;
		systemInfo.maxServType = i;
	}
	
	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"memory"), "The amount of memory")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[Mem_Limit].min, limits[Mem_Limit].max, limits[Mem_Limit].name))
		return FALSE;
	capacity->mem = retValue;
	
	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"disk"), "The amount of storage")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[Disk_Limit].min, limits[Disk_Limit].max, limits[Disk_Limit].name))
		return FALSE;
	capacity->disk = retValue;
	
	capacity->relSpeed = 1.0f;
	
	systemInfo.numServTypes++;
	
	return TRUE;
}

void CreateJobTypes(int njTypes) 
{
	workloadInfo.jobTypes = (JobTypeProp *)calloc(njTypes, sizeof(JobTypeProp));
	assert(workloadInfo.jobTypes);
}

// store job properties per type
int StoreJobType(xmlNode *node)
{
	JobTypeProp *jTypes = workloadInfo.jobTypes;
	int i, retValue, njTypes = workloadInfo.numJobTypes;
	
	for (i = 0; i < njTypes && jTypes[i].name; i++); // find the next available job type
	if (i >= limits[JType_Limit].max) {	// too many job types specified
		fprintf(stderr, "Too many job types (max: %ld)\n", limits[JType_Limit].max);
		return FALSE;
	}
	
	jTypes[i].type = i;
	
	if (!(char *)xmlGetProp(node, (xmlChar *)"type")) {	// NULL; no 'type' attribute specified
		fprintf(stderr, "no \"type\" attribute specified in \"job\" element %d!\n", i+1);
		return FALSE;
	}
	jTypes[i].name = strdup((char *)xmlGetProp(node, (xmlChar *)"type"));
	
	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"minRunTime"), "Job min run time")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[JRunTime_Limit].min, limits[JRunTime_Limit].max, limits[JRunTime_Limit].name))
		return FALSE;
	jTypes[i].min = retValue;
	
	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"maxRunTime"), "Job max run time")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[JRunTime_Limit].min, limits[JRunTime_Limit].max, limits[JRunTime_Limit].name))
		return FALSE;
	jTypes[i].max = retValue;
	
	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"populationRate"), "Job population rate")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[JPop_Limit].min, limits[JPop_Limit].max, limits[JPop_Limit].name))
		return FALSE;
	jTypes[i].rate = retValue;
	
	workloadInfo.numJobTypes++;
	
	return TRUE;
}

// store workload pattern information
int StoreWorkloadInfo(xmlNode *node)
{
	int retValue;
	
	if (!(char *)xmlGetProp(node, (xmlChar *)"type")) {	// NULL; no 'type' attribute specified
		fprintf(stderr, "no \"type\" attribute specified in \"workload\" element!\n");
		return FALSE;
	}
	workloadInfo.name = strdup((char *)xmlGetProp(node, (xmlChar *)"type"));

	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"minLoad"), "Min load")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[WMinLoad_Limit].min, limits[WMinLoad_Limit].max, limits[WMinLoad_Limit].name))
		return FALSE;
	workloadInfo.minLoad = retValue;

	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"maxLoad"), "Max load")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[WMaxLoad_Limit].min, limits[WMaxLoad_Limit].max, limits[WMaxLoad_Limit].name))
		return FALSE;
	workloadInfo.maxLoad = retValue;
	
	workloadInfo.loadOffset = (workloadInfo.maxLoad - workloadInfo.minLoad) / GetNumLoadTransitions();
	
	if ((char *)xmlGetProp(node, (xmlChar *)"avgLowTime") &&
		(retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"avgLowTime"), "avgLowTime")) != UNDEFINED && 
		IsOutOfBound(retValue, limits[WorkloadTime_Limit].min, limits[WorkloadTime_Limit].max, limits[WorkloadTime_Limit].name))
		return FALSE;
	workloadInfo.avgLowTime = retValue;

	if ((char *)xmlGetProp(node, (xmlChar *)"avgHighTime") &&
		(retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"avgHighTime"), "avgHighTime")) != UNDEFINED && 
		IsOutOfBound(retValue, limits[WorkloadTime_Limit].min, limits[WorkloadTime_Limit].max, limits[WorkloadTime_Limit].name))
		return FALSE;
	workloadInfo.avgHighTime = retValue;

	if ((char *)xmlGetProp(node, (xmlChar *)"avgTransitTime") &&
		(retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"avgTransitTime"), "avgTransitTime")) != UNDEFINED && 
		IsOutOfBound(retValue, limits[WorkloadTime_Limit].min, limits[WorkloadTime_Limit].max, limits[WorkloadTime_Limit].name))
		return FALSE;
	workloadInfo.avgTransitTime = retValue;

	return TRUE;
}


inline int GetFailureModel(char *fModelName)
{
	int i;
	
	for (i = 0; i < Invalid_Res_Failure_Model && strcmp(fModelName, resFailureModels[i].model); i++);
		
	return (i < Invalid_Res_Failure_Model ? i : Invalid_Res_Failure_Model);
}

/*int GetFailureTrace(char *fTraceName)
{
	int fTrace = UNKNOWN;
	
	return fTrace;
}*/


inline int GetServProp(int no, unsigned short *sID)
{
	ServerTypeProp *sTypes = systemInfo.sTypes;
	int i, sTypeID, tServCnt;
	int numServTypes = systemInfo.numServTypes;
	
	for (i = tServCnt = sTypeID = 0; i < numServTypes && no >= tServCnt; i++) {
		tServCnt += sTypes[i].limit;
		sTypeID = i;
	}
	
	*sID = sTypeID != 0 ? no % (tServCnt - sTypes[sTypeID].limit) : no;
	
	return sTypeID;
}

int *GetRandomServIndices(int rawTotalServers, int totalServers)
{
	int *sIndices = (int *)malloc(sizeof(int) * totalServers);
	int in, im;

	im = 0;

	for (in = 0; in < rawTotalServers && im < totalServers; ++in) {
	  int rn = rawTotalServers - in;
	  int rm = totalServers - im;
	  if (rand() % rn < rm)    
		/* Take it */
		sIndices[im++] = in;
	}

	assert(im == totalServers);
	
	return sIndices;
}

//********************** RESOURCE FAILURE GENERATION (Jayden) ********************
// make_and_save_distribution
//void distGen(timeP time, nodeP node, char* trace, char* config) {
int GenerateResFailures(int fModel)
{
	int rawTotalServers = systemInfo.totalNumServers;
	int totalTime = sConfig.termination.simEndTime * 1.5;	// 50% longer time for failures as some jobs run after simEndTime
	nodeP *nParam = &resFailureModels[fModel].nParam;
	timeP *tParam = &resFailureModels[fModel].tParam;
    int totalServers = rawTotalServers * nParam->failureRatio * (systemInfo.rFailT->scaleFactor / 100.0f);
	int timeGrain = systemInfo.rFailT->timeGrain;
	int totalTimeMin = totalTime / (float)timeGrain + 1;
	int *sIndices = GetRandomServIndices(rawTotalServers, totalServers);
	PreGenLogNormal **preGenLognorm;
    
    // pre_generate_lognormal_time_distribution
    int maxNum = ((totalServers/10) > 10) ? (totalServers/10) : 10;	// use only 10% of servers if more than 100 servers
	preGenLognorm = (PreGenLogNormal **)malloc(sizeof(PreGenLogNormal *) * maxNum);
    
	if (verbose == VERBOSE_ALL)
		printf("# Start generating resource failures based on %s distribution.\n", resFailureModels[fModel].model);
    
	for (int i = 0; i < maxNum; i++) {
		double mu = tParam->mean;
		double	sigma = tParam->stdev;
		int seed = rand();
		
		preGenLognorm[i] = (PreGenLogNormal *)malloc(sizeof(PreGenLogNormal) * totalTimeMin);
		for (int k = 0; k < totalTimeMin; k++) {
			preGenLognorm[i][k].inx = k;
			preGenLognorm[i][k].pglognorm = log_normal_sample(mu, sigma, &seed);
		}
	}
	
    // get_top_n
    int** maxIndexes = (int **)malloc(sizeof(int *) * maxNum);
	for (int i = 0; i < maxNum; i++)
        maxIndexes[i] = (int *) malloc(sizeof(int) * totalTimeMin);
    
	for (int i = 0; i < maxNum; i++) {
		qsort(preGenLognorm[i], totalTimeMin, sizeof(PreGenLogNormal), CmpPreGenLN);
		for (int k = 0; k < totalTimeMin; k++)
			maxIndexes[i][k] = preGenLognorm[i][k].inx;
    }

	for (int i = 0; i < maxNum; i++)
        free (preGenLognorm[i]);
    free(preGenLognorm);
 
    // get_lognormal_time_distribution
	double *nodeD = (double *)malloc(sizeof(double) * totalServers);
    double nodeMean = 0.0;
    double sum = 0.0;
    
    lognormalTimeDist(nodeD, totalServers, nParam->mean, nParam->stdev);
    
    for (int i = 0; i < totalServers; i++) {
        sum += nodeD[i];
    }
    
    nodeMean = sum / (totalServers * 1.0);
    double variance = 0.0;
    
    for (int i = 0; i < totalServers; i++) {
        variance += pow(nodeD[i] - nodeMean, 2);
    }
    
    long totalFailedTime = 0;
	int *ftCount = (int *)malloc(sizeof(int) * totalServers);
	int** nodeDArr = (int **)malloc(sizeof(int *) * totalServers);
    
    
    for (int i = 0; i < totalServers; i++) {
        int failedTime = ((totalTimeMin * nodeD[i]) / 100);
        
        if (failedTime > totalTimeMin) {
            failedTime = totalTimeMin;
        }
		failedTime *= systemInfo.rFailT->scaleFactor / 100.0f;
		
        totalFailedTime += failedTime;
        ftCount[i] = failedTime;
        
		nodeDArr[i] = (int *)malloc(sizeof(int) * failedTime);
        int randIndex = rand() % maxNum;
        
        for (int k = 0; k < failedTime; k++) {
            nodeDArr[i][k] = maxIndexes[randIndex][k];
        }
        qsort(nodeDArr[i], failedTime, sizeof(int), cmpInt);
    }
    freeArr(maxIndexes, maxNum);
	free(nodeD);
    
    // Seems to be slightly higher than the actual number of generated failures
    int totalFailures = 0;
    for (int i = 0; i < totalServers; i++) {
        totalFailures += ftCount[i];
    }
	
	systemInfo.rFailT->numFailues = totalFailures * 2;	// for every failure, its corresponding recovery is added; hence, * 2.
	systemInfo.rFailT->resFailures = (ResFailure *)malloc(sizeof(ResFailure) * totalFailures * 2);
	ResFailure *resFailures = systemInfo.rFailT->resFailures;
	
    int sFailIndex = 0;
	int* ftDur = (int *)malloc(sizeof(int) * totalFailures);
	
    // make_failure_trace_like_fta_format
    for (int i = 0; i < totalServers; i++) {
        int ftNum = ftCount[i];
        int nodeDIndex = 0;
        int startSecondIndex = 0;
		
        while (ftNum > 0) {
            int ftDurNum = 0;
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
				// min 30 seconds interval from the previous failure; i.e., timeGrain - 31
				resFailures[sFailIndex].startTime = ftDur[startSecondIndex] * timeGrain - rand() % (timeGrain - 31);	
				if (resFailures[sFailIndex].startTime < 0) 
					resFailures[sFailIndex].startTime = 0;
				resFailures[sFailIndex].endTime = ftDur[ftDurNum - 1] * timeGrain + (timeGrain - 1) - rand() % (timeGrain - 31);
				resFailures[sFailIndex].servType = GetServProp(sIndices[i], &resFailures[sFailIndex].servID);
				resFailures[sFailIndex].eventType = RES_Failure;
				sFailIndex++;
                AddResRecovery(&resFailures[sFailIndex - 1], &resFailures[sFailIndex]);
				sFailIndex++;
            }
			
        }
    }
	
	free(ftCount);
	free(ftDur);
    freeArr(nodeDArr, totalServers);

    if (verbose == VERBOSE_ALL)
		printf("Total #failures: %d\n", totalFailures);
	
    resFailures = (ResFailure *)realloc(resFailures, sizeof(ResFailure) * sFailIndex);
	systemInfo.rFailT->numFailues = sFailIndex;
	
	qsort(resFailures, sFailIndex, sizeof(ResFailure), CmpResFailures);
	
#ifdef DEBUG	
	for (int i = 0; i < sFailIndex; i++)
	fprintf(stderr, ">>> %d %d %hu %hu <<<\n", resFailures[i].startTime, resFailures[i].endTime, 
					resFailures[i].servType, resFailures[i].servID);
#endif

	free(sIndices);
	
	return TRUE;
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

// sort by pglognorm in descending order
int CmpPreGenLN(const void *p1, const void *p2)
{
	PreGenLogNormal *left = (PreGenLogNormal *)p1;
	PreGenLogNormal *right = (PreGenLogNormal *)p2;
	
	return (right->pglognorm - left->pglognorm);
}

void SkipComments(FILE *f)
{
	char ch;

	/* skip comments */
	while (isspace(ch = (char)fgetc(f)));
	ungetc(ch, f);
	while ((ch = (char)fgetc(f)) == '#')
		while (fgetc(f) != '\n');
    ungetc(ch, f);
}


void ReadResFailure(FILE *file, ResFailure *resFail)
{
	int retValue; //, temp;
	char servType[MAX_BUF];
	
	//retValue = fscanf(file, "%d %d %s %d %hu", &resFail->startTime, &resFail->endTime, servType, &temp, &resFail->servID); for initial Python failure generator
	retValue = fscanf(file, "%d %d %s %hu", &resFail->startTime, &resFail->endTime, servType, &resFail->servID);
	assert(retValue == NUM_RES_FAILURE_FIELDS);	
	resFail->servType = FindResTypeByName(servType);
	resFail->eventType = RES_Failure;
}


void AddResRecovery(ResFailure *resFail, ResFailure *resRecover)
{
	resRecover->eventType = RES_Recovery;
	// currently resource/server gets recovered instantly; hence, resRecover->startTime == resFail->endTime
	resRecover->startTime = resFail->endTime + 1;
	resRecover->endTime = resFail->startTime;	// used when finding out the failure duration for this recovery
	resRecover->servType = resFail->servType;
	resRecover->servID = resFail->servID;
}


int CmpResFailures(const void *p1, const void *p2)
{
	ResFailure *left = (ResFailure *)p1;
	ResFailure *right = (ResFailure *)p2;

	return (left->startTime - right->startTime);
}


int LoadResFailures(char *failFileName)
{
	FILE *fFile;
	int maxFailures = DEFAULT_RES_FAILURES, fCnt = 0;
	ResFailure *resFailures;
	
	fFile = fopen(failFileName, "r");
	
	if (!fFile) {
		fprintf(stderr, "%s: no such file found!\n", failFileName);
		return FALSE;
	}
	
	SkipComments(fFile);
	
	systemInfo.rFailT->numFailues = 0;
	systemInfo.rFailT->resFailures = (ResFailure *)calloc(maxFailures, sizeof(ResFailure));
	resFailures = systemInfo.rFailT->resFailures;

	// for every resource failure event, there is an extra event (resource recovery) created 
	// with the end time of failure (endTime)
	ReadResFailure(fFile, &resFailures[fCnt]);
	fCnt++;
	AddResRecovery(&resFailures[fCnt - 1], &resFailures[fCnt]);
	fCnt++;
		
	while (!feof(fFile)) {
		ReadResFailure(fFile, &resFailures[fCnt]);
		fCnt++;
		AddResRecovery(&resFailures[fCnt - 1], &resFailures[fCnt]);
		fCnt++;
		// not enough space to record failures
		// fCnt + 1 as we add two records (failure and recovery) in batch
		if (fCnt + 1 >= maxFailures) {	
			maxFailures += DEFAULT_RES_FAILURES;	// increase by DEFAULT_RES_FAILURES (i.e., 1000)
			systemInfo.rFailT->resFailures = (ResFailure *)realloc(resFailures, sizeof(ResFailure) * maxFailures);
			resFailures = systemInfo.rFailT->resFailures;
		}
	}
	
	systemInfo.rFailT->resFailures = (ResFailure *)realloc(resFailures, sizeof(ResFailure) * fCnt);
	systemInfo.rFailT->numFailues = fCnt;
	
	fclose(fFile);
	
	// sort resource failure events by startTime, 
	// so that the resource recovery events in particular can be handled properly
	qsort(systemInfo.rFailT->resFailures, fCnt, sizeof(ResFailure), CmpResFailures);
#ifdef DEBUG
	for (int i = 0; i < fCnt; i++)
		fprintf(stderr, "### %d %d %hu %hu (%d)\n", 
			systemInfo.rFailT->resFailures[i].startTime, 
			systemInfo.rFailT->resFailures[i].endTime,
			systemInfo.rFailT->resFailures[i].servType,
			systemInfo.rFailT->resFailures[i].servID,
			systemInfo.rFailT->resFailures[i].eventType);
#endif

	return TRUE;
}


// failureDist and failureTrace attributes added to the servers element, 
// e.g., <servers failureDist="distribution" failureTrace="trace">
int LoadSimConfig(xmlNode *node)
{
	xmlNode *curNode = node;
	char *failFileName = NULL;
	int retValue, ret = TRUE;

	for (curNode = node; ret && curNode; curNode = curNode->next) {
		if (curNode->type == XML_ELEMENT_NODE) {
			if (!xmlStrcmp(curNode->name, (xmlChar *)"config")) {
				/*if (!xmlGetProp(curNode, (xmlChar *)"newline"))
					newline = FALSE;
				else*/
				if (!xmlStrcmp(xmlGetProp(curNode, (xmlChar *)"newline"), (xmlChar *)"true"))
					newline = TRUE;
				
				if (!xmlGetProp(curNode, (xmlChar *)"randomSeed"))
					sConfig.rseed = DEFAULT_RND_SEED;
				else {
					if ((retValue = GetIntValue((char *)xmlGetProp(curNode, (xmlChar *)"randomSeed"), "Random seed")) == UNDEFINED || 
						IsOutOfBound(retValue, limits[RSeed_Limit].min, limits[RSeed_Limit].max, limits[RSeed_Limit].name))
						return FALSE;
					sConfig.rseed = retValue;
				}
				srand(sConfig.rseed);
			}
			if (!xmlStrcmp(curNode->name, (xmlChar *)"servers")) { // (char *)xmlNodeGetContent(curNode)
			
				if (systemInfo.sTypes) { // sTypes already created meaning too many "servers" elements
					fprintf(stderr, "Only one system (\"servers\" element) is supported!\n");
					return FALSE;
				}
				systemInfo.numServTypes = 0;
				CreateServerTypes(limits[SType_Limit].max);
				
				// resource failures need to be dealt with after obtaining all server information 
				// as failures are generated considering servers specified in the configuration, and 
				if (((char *)xmlGetProp(curNode, (xmlChar *)"failureModel") || 
					(char *)xmlGetProp(curNode, (xmlChar *)"failureTimeGrain") ||
					(char *)xmlGetProp(curNode, (xmlChar *)"failureFile")) &&
					!(systemInfo.rFailT))
					CreateResFailTrace();

				if ((char *)xmlGetProp(curNode, (xmlChar *)"failureModel") && systemInfo.rFailT->fModel == UNDEFINED) {
					systemInfo.rFailT->fModel = GetFailureModel((char *)xmlGetProp(curNode, (xmlChar *)"failureModel"));
					if (systemInfo.rFailT->fModel == Invalid_Res_Failure_Model) {
						fprintf(stderr, "Invalid resource failure model!\n");
						return FALSE;
					}
				}

				if ((char *)xmlGetProp(curNode, (xmlChar *)"failureTimeGrain")) {
					if ((retValue = GetIntValue((char *)xmlGetProp(curNode, (xmlChar *)"failureTimeGrain"), limits[T_Grain_Limit].name)) == UNDEFINED) 
						return FALSE;
					if (IsOutOfBound(retValue, limits[T_Grain_Limit].min, limits[T_Grain_Limit].max, limits[T_Grain_Limit].name))
						return FALSE;
				}
			
				if ((char *)xmlGetProp(curNode, (xmlChar *)"failureFile")) {
					failFileName = (char *)xmlGetProp(curNode, (xmlChar *)"failureFile");
					if (failFileName)
						systemInfo.rFailT->ftFilename = strdup(failFileName);
					else
						return FALSE;
				}
			}
			else
			if (!xmlStrcmp(curNode->name, (xmlChar *)"server")) {
				if (!systemInfo.sTypes) { // sTypes not yet created meaning the "servers" element is missing
					fprintf(stderr, "The \"servers\" element is missing!\n");
					return FALSE;
				}
				if (!StoreServerType(curNode))
					return FALSE;
			}
			if (!xmlStrcmp(curNode->name, (xmlChar *)"condition")) {
				char *str = (char *)xmlGetProp(curNode, (xmlChar *)"type");
				if (!str || (strcmp(str, JCNT_STR) && strcmp(str, ENDTIME_STR))) {	// not type specified
					fprintf(stderr, "Error: invalid \"type\" attribute!\n");
					return FALSE;
				}
				if ((retValue = GetIntValue((char *)xmlGetProp(curNode, (xmlChar *)"value"), "Termination condition value")) == UNDEFINED)
					return FALSE;
				if (!strcmp(str, ENDTIME_STR)) {
					if (IsOutOfBound(retValue, limits[SEnd_Limit].min, limits[SEnd_Limit].max, limits[SEnd_Limit].name))
						return FALSE;
					sConfig.termination.simEndTime = retValue;
				}
				else
				if (!strcmp(str, JCNT_STR)) {
					if (IsOutOfBound(retValue, limits[JCnt_Limit].min, limits[JCnt_Limit].max, limits[JCnt_Limit].name))
						return FALSE;
					sConfig.termination.maxJobCnt = retValue;
				}
			}
			if (!xmlStrcmp(curNode->name, (xmlChar *)"jobs")) {
				char *jobFileName;
				
				if ((jobFileName = (char *)xmlGetProp(curNode, (xmlChar *)"file"))) {	// job list file specified
					sConfig.jobFile = TRUE;
					strcpy(sConfig.jobFileName, jobFileName);
				}
				else
				{
					// if both job list file is specified and <job> elements exist, the latter will overwrite what's read from the file
					if (workloadInfo.jobTypes) { // jobTypes already created meaning too many "jobs" elements
						fprintf(stderr, "Only one set of job types (\"jobs\" element) is supported!\n");
						return FALSE;
					}
					workloadInfo.numJobTypes = 0;
					CreateJobTypes(limits[JType_Limit].max);
				}
			}
			if (!xmlStrcmp(curNode->name, (xmlChar *)"job")) {
				if (!workloadInfo.jobTypes) { // jobTypes not yet created meaning the "jobs" element is missing
					fprintf(stderr, "The \"jobs\" element is missing!\n");
					return FALSE;
				}
				if (!StoreJobType(curNode))
					return FALSE;
			}
			if (!xmlStrcmp(curNode->name, (xmlChar *)"workload")) {
				if (workloadInfo.name) { 
					fprintf(stderr, "Only one workload pattern (\"workload\" element) is supported!\n");
					return FALSE;
				}
				if (!StoreWorkloadInfo(curNode))
					return FALSE;
			}
		}
		ret = LoadSimConfig(curNode->children);
	}
	
	return ret;
}

void ResizeServerTypes()
{
	int nsTypes = systemInfo.numServTypes;
	ServerTypeProp *sTypes = systemInfo.sTypes;
	Server **servers = systemInfo.servers;
	
	servers = (Server **)realloc(servers, sizeof(Server *) * nsTypes);
	assert(servers);
	systemInfo.servers = servers;
	sTypes = (ServerTypeProp *)realloc(sTypes, sizeof(ServerTypeProp) * nsTypes);
	assert(sTypes);
	systemInfo.sTypes = sTypes;
}

//void InitServers(Server *servers, unsigned short type, int servCnt, ResCapacity *capacity)
void InitServers(Server *servers, ServerTypeProp *sType)
{
	int i, limit = sType->limit, type = sType->type;
	ResCapacity *capacity = &sType->capacity;
	
	for (i = 0; i < limit; i++) {
		servers[i].type = type;
		servers[i].id = i;
		servers[i].availCores = capacity->cores;
		servers[i].availMem = capacity->mem;
		servers[i].availDisk = capacity->disk;
		servers[i].startTime = UNDEFINED;
		servers[i].usedTime = 0;
		servers[i].failInfo.numFails = 0;
		servers[i].failInfo.totalFailTime = 0;
		servers[i].failInfo.mttf = UNKNOWN;
		servers[i].failInfo.mttr = UNKNOWN;
		servers[i].failInfo.lastOpTime = 0;
		servers[i].waiting = 
		servers[i].running = 
		servers[i].failed = 
		servers[i].suspended = 
		servers[i].completed =
		servers[i].killed = NULL;
		servers[i].state = Inactive;
	}
}

void CreateServers()
{
	int nsTypes = systemInfo.numServTypes;
	int i, totalNumServers = 0;
	ServerTypeProp *sTypes = systemInfo.sTypes;
	Server **servers = systemInfo.servers;
	
	for (i = 0; i < nsTypes; i++) {
		ServerTypeProp *sType = &sTypes[i];
		int limit = sType->limit;
		
		totalNumServers += limit;
		servers[i] = (Server *)malloc(sizeof(Server) * limit);
		//InitServers(servers[i], sType->type, limit, capacity);
		InitServers(servers[i], sType);
	}
	
	systemInfo.totalNumServers = totalNumServers;
}

void ResizeJobTypes()
{
	workloadInfo.jobTypes = (JobTypeProp *)realloc(workloadInfo.jobTypes, sizeof(JobTypeProp) * workloadInfo.numJobTypes);
	assert(workloadInfo.jobTypes);
}

int ValidateJobRates()
{
	JobTypeProp *jobTypes = workloadInfo.jobTypes;
	int i, totalJobRate = 0;
	
	for (i = 0; i < workloadInfo.numJobTypes; i++)
		totalJobRate += jobTypes[i].rate;
	
	return (totalJobRate == 100);	// the sum must be 100%
}


int ReadSimConfig(char *filename)
{
	xmlDoc *doc = NULL;
    xmlNode *rootNode = NULL;
	int ret;

	doc = xmlReadFile(filename, NULL, 0);
	assert(doc);
	rootNode = xmlDocGetRootElement(doc);

	ret = LoadSimConfig(rootNode);

	if (ret) {	// valid configuration file
		ResizeServerTypes();
		CreateServers();

		if (!sConfig.jobFile) {
			ResizeJobTypes();

			if ((ret = ValidateJobRates()))
				workloadInfo.jobs = GenerateJobs();
		}

		if (systemInfo.rFailT) {
			if (systemInfo.rFailT->fModel > UNKNOWN && 
				systemInfo.rFailT->fModel < Invalid_Res_Failure_Model) 
				ret = GenerateResFailures(systemInfo.rFailT->fModel);
			else
			if (systemInfo.rFailT->ftFilename)
				ret = LoadResFailures(systemInfo.rFailT->ftFilename);
		}
	}
	xmlFreeDoc(doc);
	if (ret && sConfig.jobFile)
		ret = ReadJobs(sConfig.jobFileName);

	return ret;
}

void GenerateSystemInfo()
{
	int stIndex = systemInfo.numServTypes - 1;
	int lastSType = END_SERVER_TYPE;
	int servType;
	int i;
	ServerTypeProp *sTypes;
	Server **servers;
	
	CreateServerTypes(systemInfo.numServTypes);
	servers = systemInfo.servers;
	sTypes = systemInfo.sTypes;

	while (stIndex >= 0) {
		int resLimit;
		servType = stIndex + rand() % (lastSType - stIndex);
		sTypes[stIndex].type = servType;

		if (!sConfig.resLimit) {
			resLimit = limits[Res_Limit].min + rand() % (limits[Res_Limit].def * 2);
			// keep if it's less than 10, otherwise make it a multiple of 10
			sTypes[stIndex].limit = (resLimit <= 10) ? resLimit : resLimit + (10 - resLimit % 10);
		}
		else
			sTypes[stIndex].limit = sConfig.resLimit;
		
		lastSType = servType;
		stIndex--;		
	}
	
	systemInfo.maxServType = sTypes[systemInfo.numServTypes - 1].type;

	for (i = 0; i < systemInfo.numServTypes; i++) {
		ServerTypeProp *sType = &sTypes[i];
		ServerTypeProp *stProp = &defaultServerTypes[sType->type];
		ResCapacity *capacity = &sType->capacity;
		int limit = sTypes[i].limit;
		int maxMem;

		sType->name = strdup(stProp->name);
		sType->bootupTime = stProp->bootupTime;
		sType->rate = stProp->rate;
		// set resource capacity
		capacity->cores = stProp->capacity.cores;
		maxMem = (stProp->capacity.disk / stProp->capacity.cores) / 1000; // make it in GB
		capacity->mem = (1 + rand() % maxMem) * 1000 * capacity->cores; // convert back to MB
		capacity->disk = stProp->capacity.disk;
		capacity->relSpeed = stProp->capacity.relSpeed;
		
		servers[i] = (Server *)malloc(sizeof(Server) * limit);
		//InitServers(servers[i], sType->type, limit, capacity);
		InitServers(servers[i], sType);
	}
}

inline long int CalcTotalCoreCount()
{
	long int totalCores = 0;
	ServerTypeProp *sTypes = systemInfo.sTypes;
	int i, nsTypes = systemInfo.numServTypes;
	
	for (i = 0; i < nsTypes; i++)
		totalCores += sTypes[i].limit * sTypes[i].capacity.cores;

	return totalCores;
}

inline long int CalcCoresInUse(int submitTime, Job *jobs, int jID)
{
	long int coresInUse = 0;
	int i;
	
	for (i = 0; i < jID; i++) {
		Job *job = &jobs[i];
		int completionTime = job->submitTime + job->actRunTime;
		
		if (completionTime <= submitTime) continue;	// skip
		coresInUse += job->resReq.cores;
	}
	
	return coresInUse;
}

inline int GetNextLoadDuration(enum LoadState lState)
{
	int lDuration = 0;
	
	switch (lState) {
		case Load_Low:
			lDuration = workloadInfo.avgLowTime;
			break;
		case Load_High:
			lDuration = workloadInfo.avgHighTime;
			break;
		default:
			lDuration = workloadInfo.avgTransitTime;
			break;
	}
	
	return lDuration;
}

inline int GetJobType()
{
	int i, r, rMin, rMax, jType;
	JobTypeProp *jobTypes = workloadInfo.jobTypes;

	r = rand() % 100;	// out of 100%
	for (i = 0, jType = UNKNOWN, rMin = 0, rMax = jobTypes[i].rate; 
		i < workloadInfo.numJobTypes && jType == UNKNOWN; 
		i++, rMin = rMax, rMax += jobTypes[i].rate) {
		if (r >= rMin && r <= rMax - 1)
			jType = i;
	}
	
	return jType;
}

int ReadJobs(char *jobFileName)
{
	xmlDoc *adoc = NULL;
    xmlNode *rootNode = NULL;
	unsigned int maxJobCnt = sConfig.termination.maxJobCnt;
	int ret;

	adoc = xmlReadFile(jobFileName, NULL, 0);
	assert(adoc);
	rootNode = xmlDocGetRootElement(adoc);

	assert(!workloadInfo.jobs);
	workloadInfo.jobs = (Job *)calloc(maxJobCnt, sizeof(Job));
	ret = LoadJobs(rootNode);

	if (ret && workloadInfo.numJobs < maxJobCnt)
		workloadInfo.jobs = (Job *)realloc(workloadInfo.jobs, sizeof(Job) * workloadInfo.numJobs);

	xmlFreeDoc(adoc);

	return ret;
}

// to be implemented!!!
int GetJobTypeByName(char *jobType)
{
	int jType = UNKNOWN;
	
	return jType;
}

// actRuntime is set to estRuntime in the existence of job list file
//<job id="0" type="long" submitTime="83" estRunTime="37232" [actRunTime=""] cores="2" memory="500" disk="600" />
int LoadJobs(xmlNode *node)
{
	xmlNode *curNode;
	int retValue, ret = TRUE;
	static int jCnt = 0;
	
	for (curNode = node; ret && curNode; curNode = curNode->next) {
		if (curNode->type == XML_ELEMENT_NODE) {
			if (!xmlStrcmp(curNode->name, (xmlChar *)"job")) {
				Job *job = &workloadInfo.jobs[jCnt];
				
				if ((retValue = GetIntValue((char *)xmlGetProp(curNode, (xmlChar *)"id"), "Invalid job ID")) == UNDEFINED)
					return FALSE;
				job->id = retValue;
				if (!(char *)xmlGetProp(curNode, (xmlChar *)"type")) {
					fprintf(stderr, "no \"type\" attribute specified in \"job\" element %d!\n", jCnt);
					return FALSE;
				}
				job->type = GetJobTypeByName((char *)xmlGetProp(curNode, (xmlChar *)"type"));
				if ((retValue = GetIntValue((char *)xmlGetProp(curNode, (xmlChar *)"submitTime"), "Invalid submit time")) == UNDEFINED)
					return FALSE;
				job->submitTime = retValue;
				if ((retValue = GetIntValue((char *)xmlGetProp(curNode, (xmlChar *)"estRunTime"), "Invalid estimated run time")) == UNDEFINED)
					return FALSE;
				job->estRunTime = retValue;
				if (!(char *)xmlGetProp(curNode, (xmlChar *)"actRunTime"))	// if no actRunTime is specified, use estRunTime for actRunTime
					job->actRunTime = job->estRunTime;
				else
				{
					if ((retValue = GetIntValue((char *)xmlGetProp(curNode, (xmlChar *)"actRunTime"), "Invalid actual run time")) == UNDEFINED) 
						return FALSE;
					job->actRunTime = retValue;
				}
				if ((retValue = GetIntValue((char *)xmlGetProp(curNode, (xmlChar *)"cores"), "Invalid core count")) == UNDEFINED)
					return FALSE;
				job->resReq.cores = retValue;
				if ((retValue = GetIntValue((char *)xmlGetProp(curNode, (xmlChar *)"memory"), "Invalid memory amount")) == UNDEFINED)
					return FALSE;
				job->resReq.mem = retValue;
				if ((retValue = GetIntValue((char *)xmlGetProp(curNode, (xmlChar *)"disk"), "Invalid disk amount")) == UNDEFINED)
					return FALSE;
				job->resReq.disk = retValue;
				jCnt++;
			}
		}
		ret = LoadJobs(curNode->children);
	}	
	
	workloadInfo.numJobs = jCnt;
		
	return ret;
}

Job *GenerateJobs()
{
	unsigned int simEndTime = sConfig.termination.simEndTime;
	unsigned int maxJobCnt = sConfig.termination.maxJobCnt;
	JobTypeProp *jobTypes;
	ResCapacity *maxCapacity = &systemInfo.sTypes[systemInfo.maxServType].capacity;
	ResCapacity *resReq;
	int jID, jType;
	long int coresInUse, totalCores;
	enum LoadState lState = Load_Low;
	int curLoad = 0, targetLoad, curLoadSTime, curLoadETime;
	int submitTime, submitInterval;
	int estRunTime, actRunTime, estError;
	int lDir = FORWARD;
	int lTimeOffset = 5;	// 5 seconds by default
	
	jobTypes = workloadInfo.jobTypes;

	Job *jobs = (Job *)calloc(maxJobCnt, sizeof(Job));	// allocate sufficient for max job count
	
	submitInterval = MIN_IN_SECONDS;	// 1 min by default
	submitTime = 0;
	curLoadSTime = 0;
	curLoadETime = workloadInfo.avgLowTime;
	targetLoad = workloadInfo.minLoad;
	totalCores = CalcTotalCoreCount();
	
	for (jID = 0; jID < maxJobCnt && submitTime < simEndTime; jID++) {
		Job *job = &jobs[jID];
		
		job->id = jID;
		
		/* no jobs with the same submission time; 
		  this is required for job backfilling in the case all servers capable of 
		  running the job are unavailable (failed) in which the job has to be pushed back
		  with its submission time being one time unit later than the next job, 
		  but no later than the job after the next one
		  */
		submitTime += rand() % submitInterval + 1;	
		// transiting to the next load state
		if (submitTime > curLoadETime && ((lDir > 0 && curLoad >= targetLoad) || (lDir < 0 && curLoad <= targetLoad))) {
			curLoadSTime = curLoadETime;
			if (lState + lDir < Load_Low || lState + lDir >= END_LOAD_STATE)
				lDir *= -1;	// flip the sign
			lState += lDir;
			targetLoad = targetLoad + workloadInfo.loadOffset * lDir;
			curLoadETime = curLoadSTime + GetNextLoadDuration(lState);
		}
		// check current load based on the number of cores used
		coresInUse = CalcCoresInUse(submitTime, jobs, jID);
		curLoad = coresInUse / (float)totalCores * 100;
		//fprintf(stderr, "Current load (target: %d, loadState: %d): %d\n", targetLoad, lState, curLoad);
		submitInterval += curLoad < targetLoad ? -lTimeOffset : lTimeOffset;
		if (submitInterval < lTimeOffset)
			submitInterval = lTimeOffset;
		else
		if (submitInterval > limits[WorkloadTime_Limit].max)
			submitInterval = limits[WorkloadTime_Limit].max;
		
		job->submitTime = submitTime;

		// generate runtimes (estimate and actual) based on job type
		jType = GetJobType();
		job->type = jType;
		actRunTime = jobTypes[jType].min + rand() % (MAX(1, jobTypes[jType].max - jobTypes[jType].min));
		job->actRunTime = actRunTime;
		estError = rand() % actRunTime;
		estRunTime = rand() % 2 == 0 ? actRunTime - estError : BOUND_INT_MAX(actRunTime + estError);
		job->estRunTime = estRunTime;
		// generate resource requirements
		resReq = &job->resReq;
		// #cores is determined by primarily the target load range (minLoad and maxLoad)
		if (curLoad < targetLoad) 	// when load is below the min load, set #cores at least the half of max #cores
			//resReq->cores = (maxCapacity->cores / 2) + rand() % (maxCapacity->cores / 2) + 1;
			resReq->cores = rand() % maxCapacity->cores + 1;
		else {	// otherwise #cores is relative to job type (i.e., runtime)
			resReq->cores = MIN(MAX(1, ROUND((float)MIN(HOUR_IN_SECONDS, actRunTime) / (MIN_IN_SECONDS * 10))), maxCapacity->cores);
			estError = rand() % (resReq->cores * 2);
			resReq->cores = rand() % 2 == 0 ? MAX(1, resReq->cores - estError) : MIN(resReq->cores + estError, maxCapacity->cores);
		}
		resReq->mem = (MIN_MEM_PER_JOB_CORE + rand() % ((1 + resReq->cores / 10) * MIN_MEM_PER_CORE)) * resReq->cores;
		resReq->mem -= resReq->mem % 100; 	// truncate tens and ones to make it in hundreds
		resReq->mem = MIN(resReq->mem, maxCapacity->mem);
		resReq->disk = (MIN_DISK_PER_JOB_CORE + rand() % ((1 + resReq->cores / 10) * MIN_DISK_PER_CORE)) * resReq->cores;
		resReq->disk -= resReq->disk % 100; 	// truncate tens and ones to make it in hundreds
		resReq->disk = MIN(resReq->disk, maxCapacity->disk);
	}

	if (jID < maxJobCnt)	// simulation end time criteron met first
		jobs = (Job *)realloc(jobs, sizeof(Job) * jID);
	
	workloadInfo.numJobs = jID;

	return jobs;
}

void WriteJobs()
{
	FILE *f;
	int i;

	f = fopen(JOB_FILENAME, "w");
	assert(f);
	
	fprintf(f, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
	fprintf(f, "<!-- job list for ds-sim@MQ, %s -->\n", version);
	fprintf(f, "<jobs>\n");
	
	for (i = 0; i < workloadInfo.numJobTypes; i++) {
		JobTypeProp *jType = &workloadInfo.jobTypes[i];
		fprintf(f, "\t<type name=\"%s\" minRunTime=\"%d\" maxRunTime=\"%d\" populationRate=\"%d\" />\n", 
				jType->name, jType->min, jType->max, jType->rate);
	}
	for (i = 0; i < workloadInfo.numJobs; i++) {
		Job *job = &workloadInfo.jobs[i];
		ResCapacity *resReq = &job->resReq;
		/*fprintf(f, "\t<job id=\"%d\" type=\"%s\" submitTime=\"%d\" estRunTime=\"%d\" actRunTime=\"%d\" \
cores=\"%d\" memory=\"%d\" disk=\"%d\" />\n", 
			job->id, workloadInfo.jobTypes[job->type].name, job->submitTime, 
			job->estRunTime, job->actRunTime, resReq->cores, resReq->mem, resReq->disk);*/
		fprintf(f, "\t<job id=\"%d\" type=\"%s\" submitTime=\"%d\" estRunTime=\"%d\" \
cores=\"%d\" memory=\"%d\" disk=\"%d\" />\n", 
			job->id, workloadInfo.jobTypes[job->type].name, job->submitTime, 
			job->estRunTime, resReq->cores, resReq->mem, resReq->disk);
	}
	fprintf(f, "</jobs>\n");
	fclose(f);
}

inline int GetNumLoadTransitions()
{
	return Load_High - Load_Low;
}

void GenerateWorkload()
{
	int i;
	
	workloadInfo.name = strdup("alternating");	// by default
	workloadInfo.minLoad = limits[WMinLoad_Limit].min + rand() % limits[WMinLoad_Limit].max;
	workloadInfo.maxLoad = workloadInfo.minLoad + rand() % (limits[WMaxLoad_Limit].max - workloadInfo.minLoad + 1);
	workloadInfo.loadOffset = (workloadInfo.maxLoad - workloadInfo.minLoad) / GetNumLoadTransitions();
	workloadInfo.avgLowTime = limits[WorkloadTime_Limit].min + rand() % limits[WorkloadTime_Limit].max;
	workloadInfo.avgHighTime = limits[WorkloadTime_Limit].min + rand() % limits[WorkloadTime_Limit].max;
	workloadInfo.avgTransitTime = workloadInfo.avgLowTime < workloadInfo.avgHighTime ?
								workloadInfo.avgLowTime : workloadInfo.avgHighTime;
	workloadInfo.numJobTypes = END_JOB_TYPE;
	workloadInfo.jobTypes = (JobTypeProp *)malloc(sizeof(JobTypeProp) * workloadInfo.numJobTypes);
	for (i = 0; i < workloadInfo.numJobTypes; i++) {
		JobTypeProp *jType = &workloadInfo.jobTypes[i];
		jType->type = i;
		jType->name = strdup(defaultJobTypes[i].name);
		jType->min = defaultJobTypes[i].min;
		jType->max = defaultJobTypes[i].max;
		jType->rate = defaultJobTypes[i].rate;
	}
	workloadInfo.jobs = GenerateJobs();
}

void WriteResFailures()
{
	FILE *f;
	int i, totalFailures;
	char configFName[MAX_NAME_LENGTH];

	if (sConfig.configFile)
		strcpy(configFName, sConfig.configFilename);
	else
		sprintf(configFName, "random (internal)");

	f = fopen(RES_FAILURE_FILENAME, "w");
	assert(f);

	fprintf(f, "#base_trace: %s, config_file: %s, total_servers: %d, total_time: %d, \
time_dist_mean: %f, time_dist_stdev: %f, node_dist_mean: %f, node_dist_stdev: %f\n",
				resFailureModels[systemInfo.rFailT->fModel].model, configFName,
				systemInfo.totalNumServers, sConfig.termination.simEndTime, 
				resFailureModels[systemInfo.rFailT->fModel].tParam.mean,
				resFailureModels[systemInfo.rFailT->fModel].tParam.stdev,
				resFailureModels[systemInfo.rFailT->fModel].nParam.mean,
				resFailureModels[systemInfo.rFailT->fModel].nParam.stdev);

	for (i = 0, totalFailures = systemInfo.rFailT->numFailues; i < totalFailures; i++) {
		ResFailure *resFailure = &systemInfo.rFailT->resFailures[i];

		// print only up to the failure that starts before the actual simulation end time
		if (resFailure->startTime >= actSimEndTime) break;
		if (resFailure->eventType == RES_Recovery)
			continue;

		fprintf(f, "%d %d %s %hu\n", resFailure->startTime, resFailure->endTime, 
				FindResTypeNameByType(resFailure->servType), resFailure->servID);
	}

	fclose(f);
}

int ValidateSystemInfo()
{
	int i;
	
	// check resource limit, rate, core count, etc...
	for (i = 0; i < systemInfo.numServTypes; i++) {
		ServerTypeProp *sType = &systemInfo.sTypes[i];
		ResCapacity *capacity = &sType->capacity;
		int limit = sType->limit;
		
		if (IsOutOfBound(limit, limits[Res_Limit].min, limits[Res_Limit].max, limits[Res_Limit].name) ||
			IsOutOfBound(capacity->cores, limits[CCnt_Limit].min, limits[CCnt_Limit].max, limits[CCnt_Limit].name) ||
			IsOutOfBound(capacity->mem, limits[Mem_Limit].min, limits[Mem_Limit].max, limits[Mem_Limit].name) ||
			IsOutOfBound(capacity->disk, limits[Disk_Limit].min, limits[Disk_Limit].max, limits[Disk_Limit].name))
			return FALSE;
		if (sType->rate == 0) {
			fprintf(stderr, "The (hourly) rental rate of server %s missing!\n", sType->name);
			return FALSE;
		}
	}
	
	return TRUE;
}

int ValidateWorkloadInfo()
{
	int i, numTransitLoads, sumPopRate = 0;
	
	for (i = 0; i < workloadInfo.numJobTypes; i++) {
		JobTypeProp *jType = &workloadInfo.jobTypes[i];
		
		// check bounds of job type properties
		
		if (jType->min > jType->max) {
			fprintf(stderr, "Min run time (%d) should be shorter than max run time (%d) for job type (#%d)!\n", 
				jType->min, jType->max, jType->type);
			return FALSE;
		}
		sumPopRate += jType->rate;
	}
	
	if (sumPopRate != 100) {
		fprintf(stderr, "The sum of population rates must be 100 (%%)!\n");
		return FALSE;
	}
	
	if (workloadInfo.minLoad > workloadInfo.maxLoad) {
		fprintf(stderr, "Min load (%d) should be less than max load (%d)!\n", 
				workloadInfo.minLoad, workloadInfo.maxLoad);
		return FALSE;
	}
	
	numTransitLoads = Load_High - Load_Low - 1;	// three transit load patterns by default
	workloadInfo.loadOffset = (workloadInfo.maxLoad - workloadInfo.minLoad) / numTransitLoads;
	if (workloadInfo.avgLowTime <= 0)
		workloadInfo.avgLowTime = limits[WorkloadTime_Limit].min + rand() % limits[WorkloadTime_Limit].max;
	if (workloadInfo.avgHighTime <= 0)
		workloadInfo.avgHighTime = limits[WorkloadTime_Limit].min + rand() % limits[WorkloadTime_Limit].max;
	if (workloadInfo.avgTransitTime <= 0)
		workloadInfo.avgTransitTime = workloadInfo.avgLowTime < workloadInfo.avgHighTime ?
								workloadInfo.avgLowTime : workloadInfo.avgHighTime;
								
	return TRUE;
}

void WriteSystemInfo()
{
	FILE *f;
	int i;
		
	f = fopen(SYS_INFO_FILENAME, "w");
	assert(f);
	
	fprintf(f, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
	fprintf(f, "<!-- system information generated for ds-sim@MQ, %s -->\n", version);
	fprintf(f, "<system>\n");
	fprintf(f, "\t<servers>\n");
	for (i = 0; i < systemInfo.numServTypes; i++) {
		ServerTypeProp *sType = &systemInfo.sTypes[i];
		ResCapacity *capacity = &sType->capacity;
	
		fprintf(f, "\t\t<server type=\"%s\" limit=\"%d\" bootupTime=\"%d\" rate=\"%.2f\" \
coreCount=\"%d\" memory=\"%d\" disk=\"%d\" />\n", sType->name, sType->limit, sType->bootupTime, \
			sType->rate, capacity->cores, capacity->mem, capacity->disk);
	}
	
	fprintf(f, "\t</servers>\n");
	fprintf(f, "</system>\n");
	
	fclose(f);
}


int HandleKilledJobs(SchedJob *sJob, Server *server, int killedTime)
{
	return HandlePreemptedJobs(sJob, server, Killed, killedTime);
}


int HandleFailedJobs(SchedJob *sJob, Server *server, int failedTime)
{
	return HandlePreemptedJobs(sJob, server, Failed, failedTime);
}


int HandlePreemptedJobs(SchedJob *sJob, Server *server, int eventType, int eventTime)
{
	SchedJob **sJobs;
	int numJobsPreempted = 0;

	if (!sJob) return numJobsPreempted;
	
	sJobs = GetSchedJobList(eventType, server);
	AddSchedJobToEnd(sJob, sJobs);	// add all scheduled (running/waiting) jobs to an appropriate job list (failed/killed) of server
	for (; sJob; sJob = sJob->next) {
		HandlePreemptedJob(sJob, eventType, eventTime);
		numJobsPreempted++;
	}
	
	return numJobsPreempted;
}


void HandlePreemptedJob(SchedJob *sJob, int eventType, int eventTime)
{
	sJob->state = eventType;
	sJob->endTime = eventTime;
	AddToWaitingJobList(sJob->job, eventTime);
}


inline int GetEarliestStartJob(SchedJob *sJob)
{
	int est = INT_MAX;
	
	if (!sJob) return UNKNOWN;
	for (; sJob; sJob = sJob->next)
		if (sJob->startTime < est)
			est = sJob->startTime;

	return est;
}


//void FailRes(unsigned short servType, unsigned short servID, int failStartTime)
void FailRes(ResFailure *resFail)
{
	unsigned short servType = resFail->servType;
	unsigned short servID = resFail->servID;
	int	failStartTime = resFail->startTime;
	int totalOpTime;
	Server *server = &systemInfo.servers[servType][servID];
	
	//if (verbose == VERBOSE_ALL)
	//	printf("%s (#%d) FAILED at %d!\n", FindResTypeNameByType(servType), servID, failStartTime);
	
	HandleFailedJobs(server->running, server, failStartTime);
	server->running = NULL;
	
	HandleFailedJobs(server->waiting, server, failStartTime);
	server->waiting = NULL;
	
	// job suspension is not supported at this stage!!!
	HandleFailedJobs(server->suspended, server, failStartTime);
	server->suspended = NULL;
	
	// deduct time spent for jobs running at the time of failure to particularly exclude it for cost calculation
	// in the case of booting, no elapseed time is accounted for server usage
	// !!! MAY EXCLUDE THE ENTIRE DURATION OF THE CURRENT SERVER USAGE
	if (server->state != Booting) {
		int est = GetEarliestStartJob(server->running);
		int deduction = est == UNKNOWN ? 0 : failStartTime - est;
		server->usedTime += failStartTime - server->startTime - deduction;
	}
	
	server->state = Unavailable;
	server->availCores = 
	server->availMem = 
	server->availDisk =	0;
	
	server->failInfo.numFails++;
	totalOpTime = failStartTime - server->failInfo.totalFailTime;
	server->failInfo.mttf = totalOpTime / server->failInfo.numFails;
}


//void RecoverRes(unsigned short servType, unsigned short servID, int recoverTime)
void RecoverRes(ResFailure *resRecover)
{
	unsigned short servType = resRecover->servType;
	unsigned short servID = resRecover->servID;
	Server *server = &systemInfo.servers[servType][servID];
	ResCapacity *capacity = &systemInfo.sTypes[servType].capacity;
	
	server->state = Inactive;
	server->startTime = UNDEFINED;
	server->availCores = capacity->cores;
	server->availMem = capacity->mem;
	server->availDisk =	capacity->disk;
	server->failInfo.lastOpTime = resRecover->startTime;
	// resRecover->endTime has the start time of failure;
	// -1 for resRecover->startTime is 1 later than failure end time
	server->failInfo.totalFailTime += (resRecover->startTime - resRecover->endTime - 1);
	server->failInfo.mttr = server->failInfo.totalFailTime / (float) server->failInfo.numFails;	
}


// check any resource failures before the next job submission;
// if found, get jobs running on the failed server(s) and 
// put them in waiting job queue for rescheduling
int CheckForFailures(int nextFET)
{
	ResFailure *resFail, *resFailures = systemInfo.rFailT->resFailures;
	int fCnt = systemInfo.rFailT->numFailues;
	int nextFInx = systemInfo.rFailT->nextFailureInx;
	int nextJobST;

	if (nextFET == UNKNOWN)
		// curJobID is a global variable to keep track of the next job to be submitted
		nextJobST = wJobs ? wJobs->submitTime : workloadInfo.jobs[curJobID].submitTime;	
	else
		nextJobST = nextFET;

	if (nextFInx >= fCnt) return UNDEFINED;
	resFail = &resFailures[nextFInx];
	// there is at least one job to be submitted before the next resource failure event
	if (nextJobST < resFail->startTime) return UNDEFINED;
	
	curSimTime = resFail->startTime;
	UpdateResStates();
	
	if (resFail->eventType == RES_Failure) 
		FailRes(resFail);
	else 
	if (resFail->eventType == RES_Recovery) 
		RecoverRes(resFail);

	nextFInx++;
	systemInfo.rFailT->nextFailureInx = nextFInx;
	
	return (nextFInx - 1);
}


int FreeWaitingJob(int jID)
{
	WaitingJob *prev, *cur;
	
	for (prev = NULL, cur = wJobs; cur && cur->job->id != jID; prev = cur, cur = cur->next);
	if (!cur)	// not found
		return FALSE;
	else {
		if (!prev)
			wJobs = cur->next;
		else
			prev->next = cur->next;
		free(cur);
	}
	
	return TRUE;
}


WaitingJob *RemoveWaitingJob(int jID)
{
	WaitingJob *prev, *cur;
	
	for (prev = NULL, cur = wJobs; cur && cur->job->id != jID; prev = cur, cur = cur->next);
	if (cur) {
		if (!prev)
			wJobs = cur->next;
		else
			prev->next = cur->next;
		cur->next = NULL;
	}
	
	return cur;
}


WaitingJob *GetWaitingJob(int jID)
{
	WaitingJob *wJob;
	
	for (wJob = wJobs; wJob && wJob->job->id != jID; wJob = wJob->next);
	
	return wJob;
}

// jobs are placed in the order of their submission; hence, the first job to select
WaitingJob *GetFirstWaitingJob()
{
	WaitingJob *wJob = wJobs;
		
	return wJob;
}


void InsertWaitingJob(WaitingJob *wJob)
{
	WaitingJob *prev, *cur;
	
	if (!wJobs)
		wJobs = wJob;
	else {
		for (prev = NULL, cur = wJobs; cur && cur->submitTime < wJob->submitTime; prev = cur, cur = cur->next);
		if (!prev) {
			wJob->next = cur;
			wJobs = wJob;
		}
		else {
			wJob->next = prev->next;
			prev->next = wJob;
		}
	}
}


void AddToWaitingJobList(Job *job, int submitTime)
{
	WaitingJob *cur, *next, *wJob = (WaitingJob *)malloc(sizeof(WaitingJob));
	
	wJob->submitTime = submitTime;
	wJob->job = job;
	wJob->next = NULL;
	if (!wJobs)
		wJobs = wJob;
	else {
		for (cur = wJobs, next = cur->next; next; cur = next, next = next->next);
		cur->next = wJob;
	}
}

Job *GetNextJobToSched(int *submitTime)
{
	WaitingJob *wJob;
	Job *job;
	
	*submitTime = UNKNOWN;
	
	if (wJobs) {
		wJob = GetFirstWaitingJob();
		// if this is a failed job, the submitTime will be the time of failure
		*submitTime = 
		curSimTime = wJob->submitTime;
		job = wJob->job;
		return job;
	}

	if (curJobID >= workloadInfo.numJobs)
		return NULL;
	
	job = &workloadInfo.jobs[curJobID];
	AddToWaitingJobList(job, job->submitTime);
	*submitTime = 
	curSimTime = job->submitTime;
	curJobID++;
	
	return job;
}

SchedJob *RemoveSchedJob(SchedJob *sJob, Server *server)
{
	SchedJob *prev, *cur, **sJobList = GetSchedJobList(sJob->state, server);
	
	for (prev = NULL, cur = *sJobList;
		cur && cur->job->id != sJob->job->id;
		prev = cur, cur = cur->next);
		
	assert(cur);
	
	if (!prev)
		*sJobList = cur->next;
	else
		prev->next = cur->next;
	
	cur->next = NULL;
	
	return cur;
}

void AddSchedJobToEnd(SchedJob *sJob, SchedJob **sJobList)
{
	SchedJob *last;
	
	if (*sJobList == NULL)
		*sJobList = sJob;
	else {
		for (last = *sJobList; last->next; last = last->next);
		last->next = sJob;
	}
}

void InsertSJobByEndTime(SchedJob *sJob, SchedJob **sJobList)
{
	SchedJob *prevSJob, *curSJob;
	int endTime = sJob->endTime;
	
	if (*sJobList == NULL)
		*sJobList = sJob;
	else {
		for (prevSJob = NULL, curSJob = *sJobList; 
			curSJob; 
			prevSJob = curSJob, curSJob = curSJob->next) {
			if (curSJob->endTime > endTime) {
				if (prevSJob)
					prevSJob->next = sJob;
				else
					*sJobList = sJob;
				sJob->next = curSJob;
				break;
			}
		}
		if (!curSJob) // sJob's end time is the latest; hence, it has to be added to the end
			prevSJob->next = sJob;
	}
}

void MoveSchedJobState(SchedJob *sJob, int jStateNew, Server *server)
{
	SchedJob **sJobList;

	RemoveSchedJob(sJob, server);
	sJobList = GetSchedJobList(jStateNew, server);
	sJob->state = jStateNew;
	if (jStateNew == Running) {
		InsertSJobByEndTime(sJob, sJobList);
		sprintf(schedOps[logCnt].msg, "t: %10d job %5d on #%2d of server %s RUNNING\n", 
			sJob->startTime, sJob->job->id, server->id, systemInfo.sTypes[server->type].name);
#ifdef DEBUG
	fprintf(stdout, "JOB: %d -- Finish Time: %d\n", sJob->job->id, sJob->endTime);
#endif
			
		schedOps[logCnt].time = sJob->startTime;
	}
	if (jStateNew == Completed) {
		AddSchedJobToEnd(sJob, sJobList);
		sprintf(schedOps[logCnt].msg, "t: %10d job %5d on #%2d of server %s COMPLETED\n", 
			sJob->endTime, sJob->job->id, server->id, systemInfo.sTypes[server->type].name);
		schedOps[logCnt].time = sJob->endTime;
		if (sJob->endTime > actSimEndTime)	// keep track of the actual simulation end time
			actSimEndTime = sJob->endTime;
		numJobsCompleted++;
	}
	
	logCnt++;
	if (logCnt >= sizeof(schedOps) / sizeof(Log))
		schedOps = realloc(schedOps, sizeof(Log) * (logCnt + DEFAULT_LOG_CNT));
}

void UpdateServerCapacity(Server *server, ResCapacity *resReq, int offset)
{
	server->availCores += resReq->cores * offset;
	assert(server->availCores >= 0);
	server->availMem += resReq->mem * offset;
	assert(server->availMem >= 0);
	server->availDisk += resReq->disk * offset;
	assert(server->availDisk >= 0);
}

SchedJob **GetCJobList(SchedJob *firstCJob)
{
	SchedJob *cur, **cJobList;
	int i, numCJobs;
	
	for (numCJobs = 1, cur = firstCJob->next; cur; cur = cur->next) {
		if (cur->endTime != UNKNOWN && cur->endTime <= curSimTime)
			numCJobs++;
	}
	
	cJobList = (SchedJob **)malloc(sizeof(SchedJob *) * numCJobs);
	
	for(i = 0, cur = firstCJob; i < numCJobs; i++, cur = cur->next) {
		if (cur->endTime != UNKNOWN && cur->endTime <= curSimTime)
			cJobList[i] = cur;
	}

	return cJobList;
}

int CmpSchedJobs(const void *p1, const void *p2)
{
	SchedJob *left = *(SchedJob **)p1;
	SchedJob *right = *(SchedJob **)p2;

	return (left->endTime > right->endTime ? 1 : left->endTime < right->endTime ? -1 : 0);
}

int GetEST(Server *server, ResCapacity *resReq, SchedJob *completedSJob)
{
	SchedJob *cur, **cJobList;
	ResCapacity availRes;
	int i, numCJobs, est;
	
	assert(completedSJob);
	
	cJobList = GetCJobList(completedSJob);
	numCJobs = sizeof(cJobList) / sizeof(SchedJob *);
	qsort(cJobList, numCJobs, sizeof(SchedJob *), CmpSchedJobs);

	availRes.cores = server->availCores;
	availRes.mem = server->availMem;
	availRes.disk = server->availDisk;
	
	for (i = numCJobs - 1; i < 0; i--) {
		ResCapacity *resUsed = &cJobList[i]->job->resReq;
		// check backward
		availRes.cores -= resUsed->cores;
		availRes.mem -= resUsed->mem;
		availRes.disk -= resUsed->disk;
		
		if (!(availRes.cores >= resReq->cores &&
			availRes.mem >= resReq->mem &&
			availRes.disk >= resReq->disk))
			break;
	}
	assert(i >= 0);
	cur = cJobList[i];
	est = cur->endTime;

	free(cJobList);
	
	return est;
}

// currently, only the first job in the queue is checked; i.e., no backfilling is supported
SchedJob *GetNextJobToRun(Server *server, int prevJobET)
{
	SchedJob *wSJob;
	ResCapacity *resReq ;
	
	if (!(wSJob = server->waiting)) return NULL;

	resReq = &wSJob->job->resReq;
	if (IsSufficientRes(resReq, server)) { 
		wSJob->startTime = prevJobET;
		wSJob->endTime = wSJob->startTime + wSJob->job->actRunTime;
		return wSJob;
	}
	else
		return NULL;
}


int CmpLog(const void *p1, const void *p2)
{
	Log *left = (Log *)p1;
	Log *right = (Log *)p2;

	return (left->time > right->time ? 1 : left->time < right->time ? -1 : 0);
}

int PrintLog()
{
	int i;
	
	if (logCnt <= 0) return 0;
	qsort(schedOps, logCnt, sizeof(Log), CmpLog);
	for (i = 0; i < logCnt; i++)
		if (verbose != VERBOSE_STATS)
			printf("%s", schedOps[i].msg);
	
	return schedOps[logCnt - 1].time;
}


int CompareEndTime(const void *p1, const void *p2) 
{
	SchedJob *left = (SchedJob *)p1;
	SchedJob *right = (SchedJob *)p2;
	
	return (left->endTime > right->endTime ? 1 : left->endTime < right->endTime ? -1 : 0);
}


void MergeSort(SchedJob **head, int (*lessThan)(const void *item1, const void *item2)) 
{
	SchedJob *merged = *head, *second;

	if (*head && (*head)->next)   {
		second = Divide(*head);
		MergeSort(head,lessThan);
		MergeSort(&second,lessThan);
		merged = Merge(*head, second, lessThan);
	}

	*head = merged;
}


SchedJob *Divide(SchedJob *head)
{
	SchedJob *front = head; 
	SchedJob *middle = head->next->next;

	while(middle)   {
		middle = middle->next;
		front = front->next;
		if (middle) middle = middle->next;
	}
	middle = front->next;
	front->next = NULL;

	return middle;
}

SchedJob *Merge(SchedJob *head1, SchedJob *head2, int (*lessThan)(const void *item1, const void *item2)) 
{
	SchedJob *last, *newHead;
	
	if (lessThan(head1, head2))	{ 
		newHead = head1; 
		head1 = head1->next; 
	}
	else {
		newHead = head2; 
		head2 = head2->next; 
	}
	
	last = newHead;
	while (head1 && head2) {
		if(lessThan(head1, head2))
			{ last->next = head1;	
			   last = head1;
			   head1  = head1->next; }
		else	{ last->next  = head2;  
			   last = head2;
			   head2  = head2->next;  }
	}
	
	if (head1) 
		last->next = head1;
	else 
		last->next = head2;
        
	return newHead;
}


// check all completed jobs between the last simulation time and the current one
void UpdateRunningJobs(Server *server)
{
	SchedJob *sJob, *nsJob;
	
	for (sJob = server->running; sJob; sJob = nsJob) { // the running queue may increase while checking (this 'for' loop)
		if (sJob->endTime <= curSimTime) { // completed
			SchedJob *wSJob;

			MoveSchedJobState(sJob, Completed, server);
			UpdateServerCapacity(server, &sJob->job->resReq, INCREASE);
			// when a job completes check if one or more waiting jobs that can run
			while ((wSJob = GetNextJobToRun(server, sJob->endTime))) {	// no backfilling
				MoveSchedJobState(wSJob, Running, server);
				UpdateServerCapacity(server, &wSJob->job->resReq, DECREASE);
			}
			// this is necessary since the running job queue is sorted by end time and 
			// the completed job (sJob) will be removed from the running job queue and 
			// moved to the completed job queue in MoveSchedJobState
			nsJob = server->running;
		}
		else
			nsJob = sJob->next;
	}
}

// update server states based on curSimTime
// available resources should be updated accounting for running and waiting jobs
unsigned int UpdateResStates()
{
	int i, nsTypes = systemInfo.numServTypes;
	ServerTypeProp *sTypes = systemInfo.sTypes;
	Server **servers = systemInfo.servers;
	int lastSchdOpTime;
	
	logCnt = 0;
	schedOps = (Log *)malloc(sizeof(Log) * DEFAULT_LOG_CNT);
	
	for (i = 0; i < nsTypes; i++) {
		int j, limit = sTypes[i].limit;

		for (j = 0; j < limit; j++) {
			Server *server = &servers[i][j];
			SchedJob *sJob, *nsJob;

			UpdateRunningJobs(server);
			
			//if (server->state == Active && !server->running)	// all jobs completed
			//		server->state = Idle;
			if (server->startTime <= curSimTime && server->state == Booting) { // should become Active
				for (sJob = server->waiting; sJob && sJob->startTime != UNKNOWN; sJob = nsJob) {	// there must be some waiting job(s) that has run by this time
					nsJob = sJob->next;
					if (sJob->startTime <= curSimTime) {
						MoveSchedJobState(sJob, Running, server);
						server->state = Active;
					}
				}
				// after the server has become with one or more running jobs, 
				// check if any job has completed between the last simulation time and the current time
				UpdateRunningJobs(server); 
			}
			if (server->state == Active && !server->running)	// all jobs completed
				server->state = Idle;
		}
	}
	lastSchdOpTime = PrintLog();
	free(schedOps);
	
	return lastSchdOpTime;
}


int FindResTypeByName(char *name)
{
	ServerTypeProp *sTypes = systemInfo.sTypes;
	ServerTypeProp *sType;
	int i, nsTypes = systemInfo.numServTypes;
	
	for (i = 0, sType = &sTypes[i]; i < nsTypes && strcmp(sType->name, name); i++, sType = &sTypes[i]);
	if (i >= nsTypes) {
		fprintf(stderr, "No such server type by %s exists!\n", name);
		return UNKNOWN;
	}
	
	return i;
}

inline char *FindResTypeNameByType(int type)
{
	return (systemInfo.sTypes[type].name);
}

void SendDataHeader(int conn)
{
	char buffer[MAX_BUF];
	
	strcpy(buffer, "DATA");
	SendMsg(conn, buffer);
	RecvMsg(conn, buffer);
	if (strcmp(buffer, "OK")) {
		fprintf(stderr, "The message is supposed to be \"OK\", but received \"%s\"\n", buffer);
	}
}

inline int GetServerBootupTime(int type)
{
	assert(type >= 0 && type <= systemInfo.numServTypes);
	return systemInfo.sTypes[type].bootupTime;
}

// DATA format: server_type: int server_id: int state: int avail_time: int avail_cores: int avail_mem: int avail_disk: int; 
// the available time will be -1 if there is a running task since the actual runtime is unknown
int SendResInfoAll(int conn)
{
	int i, nsTypes = systemInfo.numServTypes;
	int numMsgSent = 0;
	
	for (i = 0; i < nsTypes; i++)
		numMsgSent += SendResInfoByType(conn, i, NULL);
		
	return numMsgSent;
}

int SendResInfoByType(int conn, int type, ResCapacity *resReq)
{
	ServerTypeProp *sTypes = systemInfo.sTypes;
	int i;
	int numMsgSent = 0;	
	int limit = sTypes[type].limit;
		
	for (i = 0; i < limit; i++) {
		Server *server = &systemInfo.servers[type][i];
		
		// resReq is specified in the case of 'RESC Avail'; 
		// in this case, if there are insufficient available resources or waiting jobs,
		// the server is not available for the job
		// TODO: check the option of RESC explicitly
		if (resReq && (!IsSufficientRes(resReq, server) || server->waiting))
			continue;
		numMsgSent += SendResInfoByServer(conn, type, i, resReq);
	}
	
	return numMsgSent;	
}

int SendResInfoByServer(int conn, int type, int id, ResCapacity *resReq)
{
	Server *server = &systemInfo.servers[type][id];
	// get the available time of server
	int availTime = UNKNOWN;
	char buffer[MAX_BUF];

	if (server->state == Idle)
		availTime = curSimTime;
	else
	if (server->state == Inactive)
		availTime = curSimTime + GetServerBootupTime(type);
	/* else	// booting indicates a job has been assigned already and the available time is unknown since the actual runtime is not known aprori.
	if (server[i].state == Booting)
		availTime = curSimTime + DEFAULT_BOOTUP_TIME - (curSimTime - server[i].startTime);*/
	
	if (resReq && availTime == UNKNOWN)	{// either Active or Booting
		if (IsSufficientRes(resReq, server) && !server->waiting)
			availTime = (server->state == Booting) ? server->startTime : curSimTime;
		else
			return 0;
	}

	if (systemInfo.rFailT)
		sprintf(buffer, "%s %d %d %d %d %d %d %d %d %d %d", FindResTypeNameByType(server->type), id, server->state, 
			availTime, server->availCores, server->availMem, server->availDisk, 
			server->failInfo.numFails, server->failInfo.totalFailTime, server->failInfo.mttf, server->failInfo.mttr);
	else
		sprintf(buffer, "%s %d %d %d %d %d %d", FindResTypeNameByType(server->type), id, server->state, 
			availTime, server->availCores, server->availMem, server->availDisk);

	SendMsg(conn, buffer);
	RecvMsg(conn, buffer);
	if (strcmp(buffer, "OK")) {
		fprintf(stderr, "The message is supposed to be \"OK\", but received \"%s\"\n", buffer);
	}
	return 1;
}


int SendResInfoByAvail(int conn, ResCapacity *resReq)
{
	int i, nsTypes = systemInfo.numServTypes;
	int numMsgSent = 0;
	
	for (i = 0; i < nsTypes; i++)
		numMsgSent += SendResInfoByType(conn, i, resReq);
		
	return numMsgSent;
}


int SendResInfoByCapacity(int conn, ResCapacity *resReq)
{
	int i, nsTypes = systemInfo.numServTypes;
	int numMsgSent = 0;
	
	for (i = 0; i < nsTypes; i++) {
		// &systemInfo.servers[i][0] to get the first server of a type to check the init capacity
		if (!IsServerCapable(resReq, &systemInfo.servers[i][0]))
			continue;
		numMsgSent += SendResInfoByType(conn, i, NULL);
	}
		
	return numMsgSent;
}

// it might not be suitable for some algorithms, such as WF
int SendResInfoByCapacityMin(int conn, ResCapacity *resReq)
{
	int i, j, nsTypes = systemInfo.numServTypes;
	int lastInactive, lastInactiveSType, lastInactiveSID, numMsgSent = 0;
	ServerTypeProp *sTypes = systemInfo.sTypes;
	int done = FALSE;
	
	lastInactive = FALSE;
	lastInactiveSType = lastInactiveSID = UNKNOWN;
	// find out the last Inactive server
	for (i = 0; i < nsTypes; i++) {
		Server *server = systemInfo.servers[i];
		int limit = sTypes[i].limit;
		
		if (!IsServerCapable(resReq, &server[0]))
			continue;
		for (j = 0; j < limit; j++) {
			if (server[j].state == Inactive) {
				if (lastInactiveSType == UNKNOWN) {
					lastInactiveSType = i;
					lastInactiveSID = j;
				}
				lastInactive = TRUE;
			}
			else {
				lastInactiveSType = lastInactiveSID = UNKNOWN;
				lastInactive = FALSE;
			}
		}
	}
	
	for (i = 0; i < nsTypes && !done; i++) {
		int limit = sTypes[i].limit;
		// &systemInfo.servers[i][0] to get the first server of a type to check the init capacity
		if (!IsServerCapable(resReq, &systemInfo.servers[i][0]))
			continue;
		for (j = 0; j < limit && !done; j++) {
			numMsgSent += SendResInfoByServer(conn, i, j, resReq);
			if (lastInactive && i == lastInactiveSType && j == lastInactiveSID)
				done = TRUE;
		}
	}
		
	return numMsgSent;
}


int SendResInfo(int conn, char *buffer)
{
	char *str;
	int numMsgSent = 0;
	
	for (str = &buffer[4]; !isalpha(*str) && *str != '\0'; str++); // skip spaces between "RESC" and the next search term
	if (*str != '\0') {
		if (!strncmp(str, "All", 3)) {
			SendDataHeader(conn);
			numMsgSent = SendResInfoAll(conn);
		}
		else
		if (!strncmp(str, "Type", 4)) {
			int resType;
			char resTypeName[MAX_BUF] = "";
			
			sscanf(str, "Type %s", resTypeName);
			resType = FindResTypeByName(resTypeName);
			if (resType == UNDEFINED) return UNDEFINED;
			SendDataHeader(conn);
			numMsgSent = SendResInfoByType(conn, resType, NULL);
		}
		else
		if (!strncmp(str, "Avail", 5)) {
			int numFields;
			ResCapacity resReq;
			ResCapacity *maxCapacity = &systemInfo.sTypes[systemInfo.maxServType].capacity;
			
			numFields = sscanf(str, "Avail %d %d %d", &resReq.cores, &resReq.mem, &resReq.disk);
			if (numFields < 3 || 
				IsOutOfBound(resReq.cores, 1, maxCapacity->cores, limits[CCnt_Limit].name) ||
				IsOutOfBound(resReq.mem, MIN_MEM_PER_JOB_CORE, maxCapacity->mem, limits[Mem_Limit].name) ||
				IsOutOfBound(resReq.disk, MIN_DISK_PER_JOB_CORE, maxCapacity->disk, limits[Disk_Limit].name))
				return UNDEFINED;
			SendDataHeader(conn);
			numMsgSent = SendResInfoByAvail(conn, &resReq);
		}
		else
		if (!strncmp(str, "Capable", 7)) {
			int numFields;
			ResCapacity resReq;
			ResCapacity *maxCapacity = &systemInfo.sTypes[systemInfo.maxServType].capacity;
			
			numFields = sscanf(str, "Capable %d %d %d", &resReq.cores, &resReq.mem, &resReq.disk);
			if (numFields < 3 || 
				IsOutOfBound(resReq.cores, 1, maxCapacity->cores, limits[CCnt_Limit].name) ||
				IsOutOfBound(resReq.mem, MIN_MEM_PER_JOB_CORE, maxCapacity->mem, limits[Mem_Limit].name) ||
				IsOutOfBound(resReq.disk, MIN_DISK_PER_JOB_CORE, maxCapacity->disk, limits[Disk_Limit].name))
				return UNDEFINED;
			SendDataHeader(conn);
			numMsgSent = SendResInfoByCapacity(conn, &resReq);
		}
		else
		if (!strncmp(str, "capable", 7)) {
			int numFields;
			ResCapacity resReq;
			ResCapacity *maxCapacity = &systemInfo.sTypes[systemInfo.maxServType].capacity;

			numFields = sscanf(str, "capable %d %d %d", &resReq.cores, &resReq.mem, &resReq.disk);
			if (numFields < 3 || 
				IsOutOfBound(resReq.cores, 1, maxCapacity->cores, limits[CCnt_Limit].name) ||
				IsOutOfBound(resReq.mem, MIN_MEM_PER_JOB_CORE, maxCapacity->mem, limits[Mem_Limit].name) ||
				IsOutOfBound(resReq.disk, MIN_DISK_PER_JOB_CORE, maxCapacity->disk, limits[Disk_Limit].name))
				return UNDEFINED;
			SendDataHeader(conn);
			numMsgSent = SendResInfoByCapacityMin(conn, &resReq);
		}
		else
			return UNDEFINED;
	}
	else
		return UNDEFINED;

	return numMsgSent;
}

int SendJobsPerStateOnServer(int conn, SchedJob *sJob, int numMsgSent)
{
	char buffer[MAX_BUF];	
	
	for (; sJob; sJob = sJob->next) {
		Job *job = sJob->job;
		ResCapacity *resReq = &job->resReq;
		
		sprintf(buffer, "%d %d %d %d %d %d %d", job->id, sJob->state, sJob->startTime, job->estRunTime, 
			resReq->cores, resReq->mem, resReq->disk);
		SendMsg(conn, buffer);
		memset(buffer, 0, sizeof(buffer));
		numMsgSent++;
		
		RecvMsg(conn, buffer);
		
		if (strcmp(buffer, "OK")) {
			fprintf(stderr, "The message is supposed to be \"OK\", but received \"%s\"\n", buffer);
		}
	}
	
	return numMsgSent;
}

int SendJobsOnServer(int conn, char *buffer)
{
	char resTypeName[MAX_BUF];
	int sID, resType;
	int numFields, numMsgSent = 0;
	Server *server;

	numFields = sscanf(buffer, "LSTJ %s %d", resTypeName, &sID);
	if (numFields < 2 || 
		(resType = FindResTypeByName(resTypeName)) == UNDEFINED ||
		IsOutOfBound(sID, 0, systemInfo.sTypes[resType].limit - 1, "Server ID"))
		return UNDEFINED;

	SendDataHeader(conn);
	
	server = &systemInfo.servers[resType][sID];
	numMsgSent = SendJobsPerStateOnServer(conn, server->running, numMsgSent); // Running jobs
	numMsgSent = SendJobsPerStateOnServer(conn, server->waiting, numMsgSent); // Waiting jobs
	
	return numMsgSent;
}


inline int CountJobs(SchedJob *sJob)
{
	int cnt;
	
	for (cnt = 0; sJob; sJob = sJob->next, cnt++);
	
	return cnt;
}


int SendJobCountOfServer(int conn, char *buffer)
{
	char resTypeName[MAX_BUF];
	int sID, resType, jState;
	int numFields;
	Server *server;
	SchedJob **sJobs;

	numFields = sscanf(buffer, "CNTJ %s %d %d", resTypeName, &sID, &jState);
	if (numFields < 3 || 
		(resType = FindResTypeByName(resTypeName)) == UNDEFINED ||
		IsOutOfBound(sID, 0, systemInfo.sTypes[resType].limit - 1, "Server ID") ||
		IsOutOfBound(jState, 0, END_JOB_STATE, "Job state"))
		return UNDEFINED;

	server = &systemInfo.servers[resType][sID];
	
	sJobs = GetSchedJobList(jState, server);

	return CountJobs(*sJobs);
}

inline int GetTotalEstRT(SchedJob *sJob)
{
	long int ewt;
	
	for (ewt = 0; sJob; sJob = sJob->next)
		ewt += sJob->job->estRunTime;
	
	assert (ewt <= INT_MAX);
	
	return ewt;
}


int SendEstWTOfServer(int conn, char *buffer)
{
	char resTypeName[MAX_BUF];
	int sID, resType;
	int numFields;
	Server *server;
	SchedJob **sJobs;

	numFields = sscanf(buffer, "EJWT %s %d", resTypeName, &sID);
	if (numFields < 2 || 
		(resType = FindResTypeByName(resTypeName)) == UNDEFINED ||
		IsOutOfBound(sID, 0, systemInfo.sTypes[resType].limit - 1, "Server ID"))
		return UNDEFINED;

	server = &systemInfo.servers[resType][sID];
	
	sJobs = GetSchedJobList(Waiting, server);

	return GetTotalEstRT(*sJobs);
}


int TerminateServer(char *cmd)
{
	char servTypeName[MAX_BUF];
	int servID, servType;
	int numFields, numJobsKilled = 0;
	int killedTime = curSimTime;
	Server *server;
	ResCapacity *capacity;

	numFields = sscanf(cmd, "TERM %s %d", servTypeName, &servID);
	if (numFields < 2 || 
		(servType = FindResTypeByName(servTypeName)) == UNDEFINED ||
		IsOutOfBound(servID, 0, systemInfo.sTypes[servType].limit - 1, "Server ID"))
		return UNDEFINED;
		
	server = &systemInfo.servers[servType][servID];
	
	if (server->state == Unavailable || server->state == Inactive)
		return UNDEFINED;
	
	capacity = &systemInfo.sTypes[servType].capacity;

	numJobsKilled += HandleKilledJobs(server->running, server, killedTime);
	server->running = NULL;
	
	numJobsKilled += HandleKilledJobs(server->waiting, server, killedTime);
	server->waiting = NULL;
	
	// job suspension is not supported at this stage!!!
	numJobsKilled += HandleKilledJobs(server->suspended, server, killedTime);
	server->suspended = NULL;

	if (server->state != Booting) 
		server->usedTime += killedTime - server->startTime;
	
	server->state = Inactive;
	server->availCores = capacity->cores;
	server->availMem = capacity->mem;
	server->availDisk =	capacity->disk;
	
	return numJobsKilled;
}


SchedJob *FindSchedJobByState(int jID, enum JobState state, Server *server)
{
	SchedJob *cur, **sJobList = GetSchedJobList(state, server);
	
	for (cur = *sJobList; cur && cur->job->id != jID; cur = cur->next);
	
	return cur;
}


SchedJob *FindSchedJob(int jID, Server *server)
{
	SchedJob *sJob;
	
	if ((sJob = FindSchedJobByState(jID, Running, server)))
		return sJob;
	if ((sJob = FindSchedJobByState(jID, Waiting, server)))
		return sJob;
	if ((sJob = FindSchedJobByState(jID, Suspended, server)))
		return sJob;
	
	return sJob;
}


int KillJob(char *cmd)
{
	char servTypeName[MAX_BUF];
	int servID, servType, jID;
	int numFields;
	int killedTime = curSimTime;
	Server *server;
	SchedJob *sJob, **sJobList;

	numFields = sscanf(cmd, "KILJ %s %d %d", servTypeName, &servID, &jID);
	if (numFields < 3 || 
		(servType = FindResTypeByName(servTypeName)) == UNDEFINED ||
		IsOutOfBound(servID, 0, systemInfo.sTypes[servType].limit - 1, "Server ID"))
		return UNDEFINED;
		
	server = &systemInfo.servers[servType][servID];
	
	if (!(sJob = FindSchedJob(jID, server)))	// no such job found
		return UNDEFINED;
	
	RemoveSchedJob(sJob, server);
	HandlePreemptedJob(sJob, Killed, killedTime);	
	sJobList = GetSchedJobList(Killed, server);
	AddSchedJobToEnd(sJob, sJobList);

	return jID;
}


int BackFillJob(Job *job)
{
	WaitingJob *wJob, *newWJob;
	Job *nextJob;
	int simEndTime = sConfig.termination.simEndTime * 1.5;	// 50% more time for now
	
	assert(wJobs);
	
	wJob = GetFirstWaitingJob();
	// the job sending back to the next job must be the first waiting job 
	assert(wJob->job->id == job->id);
	
	wJob = RemoveWaitingJob(job->id);	
	newWJob = GetFirstWaitingJob();
	if (newWJob) {
		/* set the submit time of wJob to one time unit larger than that of newWJob;
			this is to avoid two jobs keep swapping positions when they both cannot be scheduled 
			(i.e., all servers capable of running them are unavailable/failed) without
			progressing simulation (time)
		*/
		wJob->submitTime = newWJob->submitTime + 1;
		// if the submit time of next job to schedule for the first time is less than that of wJob
		// then add the next job to wJobs
		if (curJobID < workloadInfo.numJobs) {
			nextJob = &workloadInfo.jobs[curJobID];
			if (nextJob->submitTime < wJob->submitTime) {
				AddToWaitingJobList(nextJob, nextJob->submitTime);
				curJobID++;
			}
		}
	}
	else {
		// increase submit time of wJob by 1 to progress simulation
		wJob->submitTime++;
		// no more jobs to schedule other than wJob
		if (curJobID >= workloadInfo.numJobs && wJob->submitTime > simEndTime)
			return UNDEFINED;
		else 
		if (curJobID < workloadInfo.numJobs) {
			nextJob = &workloadInfo.jobs[curJobID];
			AddToWaitingJobList(nextJob, nextJob->submitTime);
			curJobID++;
		}
	}
	
	InsertWaitingJob(wJob);
	
	return TRUE;
}


int IsSchdValid(int jID, char *stName, int sID)
{
	int sType;
	
	if (!GetWaitingJob(jID))
		return InvalidJob;
	if ((sType = FindResTypeByName(stName)) == UNDEFINED)
		return InvalidServType;
	if (sID >= systemInfo.sTypes[sType].limit)
		return InvalidServID;
		
	return ValidSchd;
}

SchedJob **GetSchedJobList(int jState, Server *server)
{
	switch (jState) {
		case Waiting:
			return &server->waiting;
		case Running:
			return &server->running;
		case Failed:
			return &server->failed;
		case Suspended:
			return &server->suspended;
		case Completed:
			return &server->completed;
		case Killed:
			return &server->killed;
		default:
			return NULL;
	}
}

int AssignToServer(SchedJob *sJob, Server *server)
{
	SchedJob **sJobList;
	
	sJobList = GetSchedJobList(sJob->state, server);
	if (sJob->state == Running)
		InsertSJobByEndTime(sJob, sJobList);
	else
		AddSchedJobToEnd(sJob, sJobList);
	
	if (verbose != VERBOSE_STATS)
		printf("t: %10d job %5d (%s) on #%2d of server %s (%s) SCHEDULED\n", 
			curSimTime, sJob->job->id, jobStates[sJob->state].stateStr, server->id, 
			systemInfo.sTypes[server->type].name, servStates[server->state].stateStr);
		//* for debugging
		/*printf("t: %10d job %5d (#cores: %2d, mem: %6d & disk: %6d, state: %d) on #%2d of server %s (state: %s) starting at %d - %d SCHEDULED\n", 
			curSimTime, sJob->job->id, sJob->job->resReq.cores, sJob->job->resReq.mem, sJob->job->resReq.disk, sJob->state,
			server->id, systemInfo.sTypes[server->type].name, servStates[server->state].stateStr, sJob->startTime, sJob->endTime);*/
	
	// either running immediately or the first job to run after booting complete
	if (sJob->state == Running || (server->state == Booting && sJob->startTime != UNKNOWN)) {
		UpdateServerCapacity(server, &sJob->job->resReq, DECREASE);
		if (sJob->state == Running) {
			if (verbose != VERBOSE_STATS)
				printf("t: %10d job %5d on #%2d of server %s RUNNING\n", 
					sJob->startTime, sJob->job->id, server->id, systemInfo.sTypes[server->type].name);
#ifdef DEBUG
	fprintf(stdout, "JOB: %d -- Finish Time: %d\n", sJob->job->id, sJob->endTime);
#endif
		}
	}
	
	return 0;
}

int IsSufficientRes(ResCapacity *resReq, Server *server)
{
	return (server->availCores >= resReq->cores &&
		server->availMem >= resReq->mem &&
		server->availDisk >= resReq->disk);
}

int IsServerCapable(ResCapacity *resReq, Server *server)
{
	ResCapacity *capacity = &systemInfo.sTypes[server->type].capacity;
	
	return (capacity->cores >= resReq->cores && 
		capacity->mem >= resReq->mem && 
		capacity->disk >= resReq->disk);
}

// check tha availablity of server including state and resource capacities.
// if valid, create SchedJob and add it to server, and remove the job from wJobs
int ScheduleJob(int jID, int sType, int sID)
{
	Server *server = &systemInfo.servers[sType][sID];
	ResCapacity *resReq = &workloadInfo.jobs[jID].resReq;
	SchedJob *sJob;
	WaitingJob *wJob;
	Job *job;
	int actWJob = TRUE;
	
	if (server->state == Unavailable)
		return ServUnavailable;
	if (!IsServerCapable(resReq, server))
		return ServIncapable;
	
	wJob = GetWaitingJob(jID);	// currently only one job at any time; in the future, there might be more than one jobs with failures
	job = wJob->job;
	sJob = (SchedJob *)malloc(sizeof(SchedJob));
	sJob->job = job;
	sJob->next = NULL;
		
	if (server->state == Idle) {	// can start immediately
		sJob->startTime = curSimTime;
		sJob->endTime = sJob->startTime + job->actRunTime;
		sJob->state = Running;
		server->state = Active;
	}
	else
	if (server->state == Inactive) {
		sJob->startTime = curSimTime + GetServerBootupTime(server->type);
		// only record the initial start time; i.e., start time after recovery from failure is not recorded
		//if (server->startTime == UNDEFINED)
			server->startTime = sJob->startTime;	
		sJob->endTime = sJob->startTime + job->actRunTime;
		sJob->state = Waiting;
		server->state = Booting;
	}
	else
	if (server->state == Booting || server->state == Active) {
		// in the case of server being booted, even the very first job scheduled is waiting; 
		// and thus, the start time of the last job has to be checked.
		// In other words, if the start time of the last job is set (but in the waiting state), 
		// the next job can also start when the booting completes 
		// as long as the remaining resources are sufficient for the job
		//if (((server->state == Active && !server->waiting) || server->state == Booting) && IsSufficientRes(resReq, server)) { 
		//if (!server->waiting && IsSufficientRes(resReq, server)) { 
		if (server->state == Booting && server->waiting) {
			SchedJob *cur;
			
			for (cur = server->waiting; cur->next; cur = cur->next);	// find the last scheduled job
			if (cur->startTime != UNKNOWN)
				actWJob = FALSE;
		}
		else
		if (server->state == Active && !server->waiting)
			actWJob = FALSE;
			
		// add the job to the end of the waiting job queue
		// For an Active server, if no jobs in the waiting queue and sufficient resources to run the job 
		// in parallel with whatever running at the moment
		// For a Booting server, there could be more than one waiting jobs 
		// since jobs can be only scheduled if there are sufficient resources
		if (!actWJob && IsSufficientRes(resReq, server)) { 
			sJob->startTime = server->state == Booting ? server->startTime : curSimTime;
			sJob->endTime = sJob->startTime + job->actRunTime;
			sJob->state = sJob->startTime > curSimTime ? Waiting : Running;
		}
		else
		{
			sJob->startTime = UNKNOWN;
			sJob->endTime = UNKNOWN;
			sJob->state = Waiting;
		}
	}
	
	AssignToServer(sJob, server);
	FreeWaitingJob(job->id);
	
	return ValidSchd;
}

int CountWJobs()
{
	int i;
	WaitingJob *cur;
	
	for (i = 0, cur = wJobs; cur; cur = cur->next, i++);
	
	return i;
}

void PrintUnscheduledJobs()
{
	WaitingJob *wJob;
	Job *job;
	int i;
	
	printf("# -----------[ Jobs in the waiting queue ]-----------\n");
	for (wJob = wJobs; wJob && wJob; wJob = wJob->next)
		if (wJob->submitTime != wJob->job->submitTime)	// resubmitted due perhaps to resource failure
			printf("# Job %d initially submitted at %d and resubmitted at %d...\n", wJob->job->id, wJob->job->submitTime, wJob->submitTime);
		else
			printf("# Job %d submitted at %d...\n", wJob->job->id, wJob->job->submitTime);
	printf("# ---------------------------------------------------\n");
	printf("# --------[ Jobs created and yet, submitted ]--------\n");
	
	for (i = curJobID, job = &workloadInfo.jobs[i]; i < workloadInfo.numJobs; i++, job = &workloadInfo.jobs[i])
		printf("# Job %d created to be submitted at %d...\n", job->id, job->submitTime);
	printf("# ---------------------------------------------------\n");
}

void PrintStats()
{
	int i, nsTypes = systemInfo.numServTypes;
	ServerTypeProp *sTypes = systemInfo.sTypes;
	Server **servers = systemInfo.servers;
	Stats *stats = (Stats *)calloc(nsTypes, sizeof(Stats));
	int totalServCnt = 0;
	float totalUtil = 0;
	float grandTotal = 0;
	long totalWT = 0;
	long totalRT = 0;
	long totalTT = 0;
	unsigned int actSimEndTime = 0;
	int numSJobs = 0;
	
	printf("# ---------------------------------------------------------------------------\n");
	for (i = 0; i < nsTypes; i++) {
		int j, limit = sTypes[i].limit;
		float totalUsage = 0, totalCost = 0;
		
		stats[i].usage = (float *)calloc(limit, sizeof(int));
		stats[i].rentalCost = (float *)calloc(limit, sizeof(double));
		
		for (j = 0; j < limit; j++) {
			SchedJob *sJob;
			unsigned int st, et, usage;
			int wt, rt;
						
			Server *server = &servers[i][j];
			if (!(sJob = server->completed)) continue;
			/*wt = sJob->startTime - sJob->job->submitTime;
			totalWT += wt;
			rt = sJob->job->actRunTime;
			totalRT += rt;
			totalTT += wt + rt;
			*/
			stats[i].numServUsed++;
			numSJobs++;
			st = sJob->startTime;
			et = sJob->endTime;
			usage = 0;
			//for (sJob = sJob->next, numSJobs++;	sJob; sJob = sJob->next, numSJobs++) {
			for (;	sJob; sJob = sJob->next, numSJobs++) {
				// Accumulate waiting times, execution times and turnaround times
				wt = sJob->startTime - sJob->job->submitTime;
				totalWT += wt;
				rt = sJob->job->actRunTime;
				totalRT += rt;
				totalTT += wt + rt;
				// Calculate usage 
				// started the execution before the previous job completes and finished the execution later than the previous one
				if (sJob->startTime <= et && sJob->endTime > et) 
					et = sJob->endTime;
				else
				if (sJob->startTime > et) {
					usage += et - st;
					st = sJob->startTime;
					et = sJob->endTime;
				}
				if (sJob->endTime > actSimEndTime)
					actSimEndTime = sJob->endTime;
			}
			// note server start time (server->startTime) may have changed 
			// if the server has either terminated (KILS) or failed, and restarted;
			// usedTime gets updated accordingly.
			// the following calucalution is for the duration of last instance of server uptime
			if (server->state != Inactive && server->state != Unavailable) 
				server->usedTime += actSimEndTime - server->startTime;
			usage += et - st;
			stats[i].usage[j] = (float)usage / server->usedTime;
			totalUsage += stats[i].usage[j];
			stats[i].rentalCost[j] = server->usedTime * (sTypes[i].rate / HOUR_IN_SECONDS);
			totalCost += stats[i].rentalCost[j];
		}
		printf("# %d %s servers used with a utilisation of %.2f at the cost of $%.2f\n", 
			stats[i].numServUsed, FindResTypeNameByType(i), stats[i].numServUsed > 0 ? 
			totalUsage / stats[i].numServUsed * 100 : stats[i].numServUsed, totalCost);
		totalServCnt += stats[i].numServUsed;
		totalUtil += totalUsage;
		grandTotal += totalCost;
	}
	printf("# =============================== [ Overall ] ===============================\n");
	printf("# actual simulation end time: %u\n", actSimEndTime);
	printf("# total #servers used: %d, avg utilisation: %.2f and total cost: $%.2f\n", 
		totalServCnt, totalUtil / totalServCnt * 100, grandTotal);
	printf("# avg waiting time: %ld, avg exec time: %ld and avg turnaround time: %ld\n",
		totalWT / numSJobs, totalRT / numSJobs, totalTT / numSJobs);
	
	free(stats);
}

void FreeSchedJobs(SchedJob *sJob)
{
	SchedJob *next;

	while (sJob) {
		next = sJob->next;
		free(sJob);
		sJob = next;
	}
}

void FreeSystemInfo()
{
	int i, nsTypes = systemInfo.numServTypes;
	ServerTypeProp *sTypes = systemInfo.sTypes;
	Server **servers = systemInfo.servers;

	if (!sTypes) return;	// nothing to free
	if (servers) {
		for (i = 0; i < nsTypes; i++) {
			int j, limit = sTypes[i].limit;
			Server *server = servers[i];
			if (server) {
				for (j = 0; j < limit; j++) {
					FreeSchedJobs(server[j].waiting);
					FreeSchedJobs(server[j].running);
					FreeSchedJobs(server[j].failed);
					FreeSchedJobs(server[j].suspended);
					FreeSchedJobs(server[j].completed);
					FreeSchedJobs(server[j].killed);
				}
				free(servers[i]);
			}
		}
		free(servers);
		systemInfo.servers = NULL;
	}

	for (i = 0; i < nsTypes; i++)
		if (!sTypes[i].name)
			free(sTypes[i].name);
	free(sTypes);
	systemInfo.sTypes = NULL;
	if (systemInfo.rFailT) {
		free(systemInfo.rFailT->resFailures);
		free(systemInfo.rFailT);
	}
}

void FreeWorkloadInfo()
{
	int i;

	if (!workloadInfo.jobTypes) return;	// nothing to free
	if (workloadInfo.name)
		free(workloadInfo.name);

	for (i = 0; i < workloadInfo.numJobTypes; i++) {
		JobTypeProp *jType = &workloadInfo.jobTypes[i];
		free(jType->name);
	}
	free(workloadInfo.jobTypes);

	if (workloadInfo.jobs)
		free(workloadInfo.jobs);
}

void FreeAll()
{
	FreeSystemInfo();
	FreeWorkloadInfo();
	xmlCleanupParser();
}
