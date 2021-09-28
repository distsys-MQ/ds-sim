#ifndef DS_SIM_DATA_STRUCTS_H
#define DS_SIM_DATA_STRUCTS_H

#define UNDEFINED					-1
#define UNKNOWN						UNDEFINED
#define TRUE						1
#define FALSE						0
#define FORWARD						1
#define BACKWARD					-1
#define INCREASE					1
#define DECREASE					-1
#define INTACT_CMD					1
#define INTACT_NEXT_EVENT			-8
#define INTACT_QUIT					-9
#define CMD_LENGTH					4
#define MAX_NAME_LENGTH				(64 + 1)
#define SERVER_TYPE_LENGTH			MAX_NAME_LENGTH
#define JOB_TYPE_LENGTH				MAX_NAME_LENGTH
#define NUM_RES_FAILURE_FIELDS		4
#define DEFAULT_BUF_SIZE			256
#define LARGE_BUF_SIZE				1024
#define XLARGE_BUF_SIZE				LARGE_BUF_SIZE * 100
#define DEFAULT_LOG_CNT				1000
#define DEFAULT_RES_FAILURES		DEFAULT_LOG_CNT
#define DEFAULT_PORT				50000			// Ports 49152-65535
#define MAX_CONNECTIONS				10
#define DEFAULT_JOB_CNT				1000
#define MAX_JOB_CNT					1000000
#define MAX_JOB_RUNTIME				(60*60*24*7)	// 7 days 
#define MAX_SIM_END_TIME			(60*60*24*30)	// 30 days in seconds
#define MIN_IN_SECONDS				60
#define HOUR_IN_SECONDS 			(60*60)			// in seconds
#define TWELVE_HOURS				(60*60*12)
#define ONE_DAY_IN_SEC				(60*60*24)
#define DEFAULT_RES_FAIL_T_GRAIN	(MIN_IN_SECONDS * 5)	// five mins
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
#define UNKNOWN_SERVER_TYPE			"UNKNOWN"
#define JOB_RIGID					1
#define JOB_MALLEABLE				2
#define TIME_MAX					LONG_MAX
#define MAX_RUN_TIME_EST_ERR		100				// in percentage
#define MAX_REL_SPEED				100				// used to generate relative CPU speed between 0.1 and 10.0; may be used for extension
#define DEFAULT_BOOTUP_TIME			60				// 60 seconds
#define DEFAULT_HOURLY_RATE			0.2				// in dollars
#define MAX_HOURLY_RATE				1000000			// $1M
#define SYS_INFO_FILENAME			"ds-system.xml"
#define JOB_FILENAME				"ds-jobs.xml"
#define RES_FAILURE_FILENAME		"ds-res-failures.txt"
#define END_DATA					"."

#define STYLE_UNDERLINE    			"\033[4m"
#define STYLE_NO_UNDERLINE 			"\033[24m"
#define STYLE_BOLD        			"\033[1m"
#define STYLE_NO_BOLD      			"\033[22m"

#define BOUND_INT_MAX(x)			((x) < 0 ? INT_MAX : (x))	// when positive overflow, limit the value (x) to INT_MAX
#define ROUND(x)					((int)((x) + 0.5))
#define MIN(x,y)					((x) < (y) ? (x) : (y))
#define MAX(x,y)					((x) > (y) ? (x) : (y))


enum Verbose {VERBOSE_NONE, VERBOSE_STATS, VERBOSE_BRIEF, VERBOSE_ALL, END_VERBOSE};

enum Sim_Command {CMD_HELO, CMD_AUTH, CMD_OK, CMD_ERR, CMD_REDY, CMD_PSHJ, CMD_JOBN, CMD_JOBP, CMD_NONE, \
				CMD_GETS, CMD_RESC, CMD_SCHD, CMD_JCPL, CMD_RESF, CMD_RESR, CMD_LSTJ, CMD_CNTJ, \
				CMD_MIGJ, CMD_EJWT, CMD_TERM, CMD_KILJ, CMD_QUIT, END_SIM_CMD};
				
enum Cmd_Issuer {CI_Client, CI_Server, CI_Both, END_CI};

enum LimitName {Port_Limit, JCnt_Limit, JRunTime_Limit, JPop_Limit, SEnd_Limit, 
				SType_Limit, Res_Limit, SBootTime_Limit, SHourlyRate_Limit, RSeed_Limit, T_Grain_Limit, F_Scale_Limit, 
				CCnt_Limit, Mem_Limit, Disk_Limit, WMinLoad_Limit, WMaxLoad_Limit, JType_Limit, WorkloadTime_Limit};

enum SimulationOption {SO_BreakPoint, SO_Config, SO_Duration, SO_Failure, SO_Granularity, SO_Help, SO_Interactive,
				SO_JobCount, SO_Limit, SO_Newline, SO_OmitFailMsg, SO_Port, SO_RandomSeed, SO_Scale, SO_Verbose, END_SIM_OPTION};

enum HelpOption {H_All, H_Usage, H_Limits, H_States, H_Cmd, END_HELP_OPTION};

enum JobState {JS_Submitted, JS_Waiting, JS_Running, JS_Suspended, JS_Completed, JS_Preempted, JS_Failed, JS_Killed, END_JOB_STATE};

enum extraJobState {XJS_None, XJS_Migrated, END_EXTRA_JOB_STATE};

enum JobType {JT_Instant, JT_Short, JT_Medium, JT_Long, END_JOB_TYPE};

enum LoadState {LS_Low, LS_Transit_Low, LS_Transit_Med, LS_Transit_High, LS_High, END_LOAD_STATE}; // go back and forth

enum ServerType {Tiny, Small, Medium, Large, XLarge, XXLarge, XXXLarge, XXXXLarge, END_SERVER_TYPE};

enum ServerState {SS_Inactive, SS_Booting, SS_Idle, SS_Active, SS_Unavailable, END_SERVER_STATE};

enum GETS_Option {GO_All, GO_Type, GO_One, GO_Avail, GO_Capable, GO_Bounded, GO_END};	// GETS stands for GET Server info

enum ResFailureEvent {RES_Failure, RES_Recovery};

enum ResFailureModel {FM_Teragrid, FM_ND07CPU, FM_Websites02, FM_LDNS04, FM_PL05, FM_G5K06, END_FAILURE_MODEL}; 

enum SchdStatus {SCHD_Valid, SCHD_InvalidJob, SCHD_InvalidServType, SCHD_InvalidServID, SCHD_ServUnavailable, SCHD_ServIncapable, END_SCHD_STATUS};

enum RegularGETSFieldID {RDF_Server_TypeName, RDF_Server_ID, RDF_Server_State, 
							RDF_Server_StartTime, RDF_Server_Core, RDF_Server_Memory, RDF_Server_Disk, 
							RDF_Num_WaitingJobs, RDF_Num_RunningJobs, END_RDF};
enum FailureGETSFieldID {FDF_Num_Failures, FDF_Total_FailTime, FDF_MTTF, FDF_MTTR, 
							FDF_MADF, FDF_Last_OpTime, END_FDF};

enum LSTJFieldID {LF_Job_ID, LF_Job_State, LF_SubmitTime, LF_StartTime, LF_Est_RunTime, LF_Job_Core, LF_Job_Memory, LF_Job_Disk, END_LF};


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
	int used;
} SimOption;

typedef struct {
	unsigned int simEndTime;
	unsigned int maxJobCnt;
} SimTermCondition;

typedef struct {
	unsigned short port;		// TCP/IP port number
	unsigned char configFile;	// flag to check if an xml configuration file has been specified
	char configFilename[DEFAULT_BUF_SIZE];
	unsigned char jobFile;		// flag if a job list file (.xml) is used
	char jobFileName[DEFAULT_BUF_SIZE];
	unsigned int rseed;
	unsigned short resLimit;	// only used for uniform resource limit set by the "-r" option
	unsigned int bp;	// break point job id
	int regGETSRecLen;
	int failGETSRecLen;
	int LSTJRecLen;
	SimTermCondition termination;
} SimConfig;

typedef struct {
	int cores;
	int mem;
	int disk;
	float relSpeed;
} ServerRes;

typedef struct {
	unsigned int id;
	int type;
	int submitTime;
	int estRunTime;
	int actRunTime;
	ServerRes	resReq;
} Job;

typedef struct {
	int type;
	char *name;
	int min;
	int max;
	int rate;
} JobTypeProp;

typedef struct {
	int id;
	char *state;
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
	unsigned short sType;
	unsigned short sID;
	int startTime;
	int endTime;
	enum JobState state;
	enum extraJobState xState;
	Job *job;
	struct SchedJob *next;
} SchedJob;

typedef struct SchedJobListNode {
	SchedJob *sJob;
	struct SchedJobListNode *next;
} SchedJobListNode;

typedef struct {
	unsigned short type;		// e.g., Amazon EC2 m3.xlarge, c4.2xlarge, etc.
	char *name;
	unsigned short limit;		// the max number of servers
	unsigned short bootupTime;
	ServerRes capacity;
	float rate;					// hourly rate in cents; however, the actual charge is based on #seconds
} ServerTypeProp;

typedef struct {
	int numFails;
	int totalFailTime;
	int mttf;	// mean time to failure
	int mttr;	// mean time to recovery
	int madf;	// average absolute deviation or mean absolute deviation (MAD) for failures
	int madr;	// MAD for recoveries
	int lastOpTime;	// last time of operation whether it's the start time of server or the time after recovery (i.e., resume)
	int *fIndices;	// failure trace indices
	int *rIndices;	// recovery, in failure trace, indices
} ServerFailInfo;

typedef struct {
	unsigned short type;
	unsigned short id;
	ServerRes availCapa;
	int startTime;		// current start time after the booting
	int lastAccTime;	// the last point of time that usage is accounted for
	int actUsage;		// actual usage excluding idle times
	int totalUsage;		// in seconds; includes idle times, but excludes times during failures
	ServerFailInfo failInfo;
	SchedJob *waiting;
	SchedJob *running;
	SchedJob *failed;
	SchedJob *suspended;
	SchedJob *completed;
	SchedJob *preempted;
	SchedJob *killed;
	enum ServerState state;
} Server;

typedef struct {
	int id;
	char *state;
} ServState;

typedef struct {
	int id;
	char *optstr;
	int optlen;
} SubOption;

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
	int *jobGr;					// job granularity in terms of #cores
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
} StatsPerServType;

typedef struct {
	unsigned int curJobID;
	unsigned int curSimTime;
	unsigned int actSimEndTime;
	int numJobsCompleted;
	WaitingJob *waitJQ;
} SimStatus;

typedef struct {
	int id;
	char cmd[CMD_LENGTH + 1];
	int issuer;
	char *desc;
	int (*CmdHandler)(char *, char *);
} Command;

typedef struct DATAFieldSize {
	int id;
	int maxLength;
} DATAFieldSize;

#endif
