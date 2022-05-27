/*********************************************************************
	Server of ds-sim
	Department of Computing, Macquarie University, Australia
	Initially developed by Young Choon Lee in Jan. 2019
	
	Change log:
		+ server resource limits have increased to 1024, 2147483647 and 2147483647 for #cores, RAM and disk, respectively.
		+ the XML attribute name of server core count has changed to "cores" to be consistent with that for job requirement.
		
	TODO:
		- tidy up the main function w.r.t. different commands
		- relative path for failure trace file and job file (mayby use of an env variable like CLASSPATH in Java)

		- implement the complete interactive mode that the user can run event by event and make responses manually
		
		- simulated a global queue by implementing QUEJ (queue job) and 
			LSTJ option (LSTJ *queued* n; this can be simulated by maintaining a queue on the client side, n for the number of queued jobs (1 - n)); 
			LSTJ queued #: the number of queued jobs
			LSTJ queued *: all queued jobs
			LSTJ queued i: ith job in the queue
		- have an option for GETS to send the server state information that has changed since the last inquiry
	
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
#include "dataStructs.h"
#include "resFailure.h"
#include "ds_server.h"
//#define DEBUG
//#define FAIL_DEBUG

#define VERSION						"27-May, 2022 @ MQ - client-server" 
#define DEVELOPERS					"Young Choon Lee, Young Ki Kim and Jayden King"

// global variables
Command cmds[] = {	// only those that need to be explicitly processed/handled
	{CMD_HELO, "HELO", CI_Client, "initial message for the connection", NULL},
	{CMD_AUTH, "AUTH", CI_Client, "authenticate user", NULL},
	{CMD_OK, "OK", CI_Both, "general acknowledgement", NULL},
	{CMD_ERR, "ERR", CI_Server, "in response to an invalid command", NULL},
	{CMD_JOBN, "JOBN", CI_Server, "normal job submission", NULL},
	{CMD_JOBP, "JOBP", CI_Server, "resubmitted job after preemption (e.g., failed or killed)", NULL},
	{CMD_NONE, "NONE", CI_Server, "no more jobs", NULL},
	{CMD_RESF, "RESF", CI_Server, "resource/server failure", NULL},
	{CMD_RESR, "RESR", CI_Server, "resource/server recovery", NULL},
	{CMD_QUIT, "QUIT", CI_Both, "terminating simulation", NULL},
	{CMD_REDY, "REDY", CI_Client, "ready to handle a server message (job submission/completion)", HandleREDY},
	{CMD_PSHJ, "PSHJ", CI_Client, "push the current job after the next job or increment submission time by 1 if it is the last job", HandlePSHJ},
	{CMD_GETS, "GETS", CI_Client, "get server state information", HandleGETS},
	{CMD_RESC, "RESC", CI_Client, "old version of GETS (for backward compatibility)", HandleGETS},
	{CMD_SCHD, "SCHD", CI_Client, "scheduling decision", HandleSCHD},
	{CMD_JCPL, "JCPL", CI_Server, "job completion", HandleJCPL},
	{CMD_LSTJ, "LSTJ", CI_Client, "list of running and waiting jobs on a specified server", HandleLSTJ},
	{CMD_CNTJ, "CNTJ", CI_Client, "the number of jobs in a particular state on a specified server", HandleCNTJ},
	{CMD_MIGJ, "MIGJ", CI_Client, "job migration", HandleMIGJ},
	{CMD_EJWT, "EJWT", CI_Client, "estimate of total waiting time on a specified server", HandleEJWT},
	{CMD_TERM, "TERM", CI_Client, "terminate a server", HandleTERM},
	{CMD_KILJ, "KILJ", CI_Client, "kill a job", HandleKILJ}};

char *issuers[] = {"Client", "Server", "Both"};

Limit limits[] = {
					{"TCP/IP port range", 49152, 65535, DEFAULT_PORT},
					{"Max Job count", 1, MAX_JOB_CNT, DEFAULT_JOB_CNT},
					{"Job run time", 1, MAX_JOB_RUNTIME, MAX_JOB_RUNTIME},	
					{"Job population rate", 1, 100, 10},
					{"Simulation end time", 1, MAX_SIM_END_TIME, MAX_SIM_END_TIME},
					{"The number of server types", 1, 100, END_SERVER_TYPE},
					{"Resource limit (#servers/type)", 1, 1000, 20},
					{"Server bootup time", 0, 600, DEFAULT_BOOTUP_TIME},	// instant - 10 mins
					{"Hourly rental rate", 0, MAX_HOURLY_RATE, DEFAULT_HOURLY_RATE},
					{"Random seed limit", 1, UINT_MAX, DEFAULT_RND_SEED},
					{"Server failure time granularity", MIN_IN_SECONDS, HOUR_IN_SECONDS, DEFAULT_RES_FAIL_T_GRAIN},
					{"Server failure scale factor", 1, 100, 100},	// in percentage with respect to the original failure distribution
					{"Core count limit", 1, 1024, 4},
					{"Memory limit", 1000, 2147483647, 32000},
					{"Disk limit", 1000, 2147483647, 64000},
					{"Min load limit", 1, 99, 10},
					{"Max load limit", 1, 100, 90},
					{"Job type limit", 1, CHAR_MAX, END_JOB_TYPE},
					{"Time limit of load state", HOUR_IN_SECONDS, TWELVE_HOURS, HOUR_IN_SECONDS},
					{NULL, 0, 0, 0}
				};
		
SimOption simOpts[] = {
					{"b(reak) point: stop at the submission of a particular job", 'b', TRUE, "job id", FALSE},
					{"c(onfiguration file): use of configuration file", 'c', TRUE, "configuration file name (.xml)", FALSE},
					{"d(uration): simulation duration/end time", 'd', TRUE, "n (in seconds)", FALSE},
					{"f(ailure): failure distribution model (e.g., teragrid, nd07cpu and websites02)", 'f', TRUE, "teragrid|nd07cpu|websits02|g5k06|pl05|ldns04", FALSE},
					{"g(ranularity): time granularity for resource failures", 'g', TRUE, "n (in second)", FALSE},
					{"h(elp): usage", 'h', TRUE, "all|usage|limits|stats", FALSE},
					{"i(nteractive): run simulation in the interactive mode", 'i', FALSE, "", FALSE},
					{"j(ob): set job count", 'j', TRUE, "n (max #jobs to generate)", FALSE},
					{"l(imit of #servers): the number of servers (uniform to all server types)", 'l', TRUE, "n", FALSE},
					{"n(ewline): newline character (\\n) at the end of each message", 'n', FALSE, NULL, FALSE},
					{"o(mit): omit to send server failure event messages (RESF and RESR)", 'o', FALSE, NULL, FALSE},
					{"p(ort number): TCP/IP port number", 'p', TRUE, "n", FALSE},
					{"r(andom seed): random seed", 'r', TRUE, "n (some integer)", FALSE},
					{"s(cale factor for resource failures): between 1 and 100; \n\t 1 for nearly no failures and 100 for original", 's', TRUE, "n (in percentage)", FALSE},
					{"v(erbose): verbose", 'v', TRUE, "all|brief|stats", FALSE},
					{NULL, ' ', FALSE, NULL, FALSE}
				};
				
JobTypeProp defaultJobTypes[] = {
							{JT_Instant, "instant", 1, 60, 58},
							{JT_Short, "short", 61, 600, 30},
							{JT_Medium, "medium", 601, 3600, 10},
							{JT_Long, "long", 3601, MAX_JOB_RUNTIME, 2}
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

SchduleStatus SchdStatusList[] = {{SCHD_Valid, "OK"}, 
								{SCHD_InvalidJob, "ERR: No such waiting job exists"},
								{SCHD_InvalidServType, "ERR: No such server type exists"}, 
								{SCHD_InvalidServID, "ERR: No such server exists (server id: out of bound)"},
								{SCHD_ServUnavailable, "ERR: The specified server unavailable"},
								{SCHD_ServIncapable, "ERR: Server incapable of running such a job"}};

ServState servStates[] = {{SS_Inactive, "inactive"},
						{SS_Booting, "booting"},
						{SS_Idle, "idle"},
						{SS_Active, "active"},
						{SS_Unavailable, "unavailable"},
						{UNKNOWN, NULL}};


JobState jobStates[] = {{JS_Submitted, "submitted"},
							{JS_Waiting, "waiting"},
							{JS_Running, "running"},
							{JS_Suspended, "suspended"},
							{JS_Completed, "completed"},
							{JS_Preempted, "preempted"},
							{JS_Failed, "failed"},
							{JS_Killed, "killed"},
							{UNKNOWN, NULL}};

const SubOption getsOptions[] = {{GO_All, "All", 3}, 
								{GO_Type, "Type", 4}, 
								{GO_One, "One", 3}, 
								{GO_Avail, "Avail", 5},
								{GO_Capable, "Capable", 7},
								{GO_Bounded, "Bounded", 7}};

const DATAFieldSize regGETSFSizes[] = {{RDF_Server_TypeName, MAX_NAME_LENGTH}, 
										{RDF_Server_ID, 3}, 
										{RDF_Server_State, 12}, 
										{RDF_Server_StartTime, 6}, 
										{RDF_Server_Core, 3}, 
										{RDF_Server_Memory, 7}, 
										{RDF_Server_Disk, 7}, 
										{RDF_Num_WaitingJobs, 6}, 
										{RDF_Num_RunningJobs, 6}};

const DATAFieldSize failGETSFSizes[] = {{FDF_Num_Failures, 6}, 
										{FDF_Total_FailTime, 6}, 
										{FDF_MTTF, 6}, 
										{FDF_MTTR, 6}, 
										{FDF_MADF, 6}, 
										{FDF_Last_OpTime, 6}};

const DATAFieldSize LSTJFSizes[] = {{LF_Job_ID, 6}, 
										{LF_Job_State, 10},
										{LF_SubmitTime, 6}, 										
										{LF_StartTime, 6}, 
										{LF_Est_RunTime, 6}, 
										{LF_Job_Core, 3}, 
										{LF_Job_Memory, 7},
										{LF_Job_Disk, 7}};

const SubOption verboseOptions[] = {{VERBOSE_NONE, "", 0},
								{VERBOSE_STATS, "stats", 5},
								{VERBOSE_BRIEF, "brief", 5},
								{VERBOSE_ALL, "all", 3}};



SimConfig g_sConfig;
System g_systemInfo;
Workload g_workloadInfo;
SimStatus g_ss = {0, 0, 0, 0, NULL};
int g_conn;
int g_fd;
SchedJob *g_cSJob = NULL;
SchedJobListNode *g_ncJobs = NULL;
Job *g_lastJobSent = NULL;
int g_perServerStats = FALSE;
char g_bufferedMsg[XLARGE_BUF_SIZE] = "";
char *g_batchMsg = NULL;
Log *g_schedOps;
int g_logSize;
int g_logCnt;


void SigHandler(int signo)
{
  if (signo == SIGINT)
	GracefulExit();
}

int main(int argc, char **argv)
{
	struct sockaddr_in serv;
	char msgToSend[LARGE_BUF_SIZE] = ""; 
	char msgRcvd[LARGE_BUF_SIZE] = ""; 
	char username[LARGE_BUF_SIZE] = "";
	int ret;

	InitSim();

	if (!ConfigSim(argc, argv)) {
		if (!simOpts[SO_Help].used)
			printf("Usage: %s -h all\n", argv[0]);
		FreeAll();
		return 1;
	}

	if (!g_sConfig.configFile) {	// configuration file not specified
		GenerateSystemInfo();
		if (!g_sConfig.jobFile)
			GenerateWorkload();
	}
	else
	if (!ValidateSystemInfo() || (!g_sConfig.jobFile && !ValidateWorkloadInfo())) {
		FreeAll();
		return 1;
	}
	
	CompleteConfig();

	if (!simOpts[SO_Interactive].used) {
		serv.sin_family = AF_INET;
		serv.sin_port = htons(g_sConfig.port); // set the port
		serv.sin_addr.s_addr = INADDR_ANY;
		assert((g_fd = socket(AF_INET, SOCK_STREAM, 0)) >= 0); // create a TCP socket

		if (setsockopt(g_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &(int){ 1 }, sizeof(int)) < 0)
			perror("setsockopt(SO_REUSEADDR | SO_REUSEPORT) failed");

		bind(g_fd, (struct sockaddr *)&serv, sizeof(serv)); //assigns the address specified by serv to the socket
		//listen(g_fd, MAX_CONNECTIONS); //Listen for client connections. Maximum 10 connections will be permitted.
		listen(g_fd, 1); //Listen for client connections. 
	
		PrintWelcomeMsg(argc, argv);
		
		//Now we start handling the connections.
		if ((g_conn = accept(g_fd, (struct sockaddr *)NULL, NULL)) < 0) {
			perror("accept"); 
			GracefulExit();
		}
	}
	else
		PrintWelcomeMsg(argc, argv);

	if (signal(SIGINT, SigHandler) == SIG_ERR)
		fprintf(stderr, "\ncan't catch SIGINT\n");
	
	// read "HELO"
	while (TRUE) {
		ret = RecvMsg("", msgRcvd);
		if (simOpts[SO_Interactive].used) {
			if (ret == INTACT_QUIT) 
				goto Quit;
			else
			if (ret != INTACT_CMD)
				continue;
		}
		if (!strcmp(msgRcvd, "HELO")) {
			strcpy(msgToSend, "OK");
			break;
		}
		fprintf(stderr, "ERR: invalid message (%s)!\n", msgRcvd);
		strcpy(msgToSend, "ERR");
		
		SendMsg(msgToSend);
	}
	SendMsg(msgToSend);

	while (TRUE) {
		ret = RecvMsg(msgToSend, msgRcvd);
		if (simOpts[SO_Interactive].used) {
			if (ret == INTACT_QUIT) 
				goto Quit;
			else
			if (ret != INTACT_CMD)
				continue;
		}
		if (strlen(msgRcvd) > CMD_LENGTH && !strncmp(msgRcvd, "AUTH", CMD_LENGTH)) {
			strncpy(username, &msgRcvd[CMD_LENGTH], strlen(msgRcvd) - CMD_LENGTH);
			strcpy(msgToSend, "OK");
			printf("# Welcome %s!\n", username);
			WriteSystemInfo();
			printf("# The system information can be read from '%s'\n", SYS_INFO_FILENAME);
			break;
		}
		fprintf(stderr, "ERR: invalid message (%s)!\n", msgRcvd);
		strcpy(msgToSend, "ERR");
		SendMsg(msgToSend);
	}
	SendMsg(msgToSend);

	while (TRUE) {
		ret = RecvMsg(msgToSend, msgRcvd);
		if (simOpts[SO_Interactive].used) {
			if (ret == INTACT_QUIT) 
				goto Quit;
			else
			if (ret != INTACT_CMD)
				continue;
		}
		if (!strcmp(msgRcvd, "REDY"))
			break;
			
		fprintf(stderr, "ERR: invalid message (%s)!\n", msgRcvd);
		strcpy(msgToSend, "ERR");
		SendMsg(msgToSend);
	}
	// scheduling loop
	while (strcmp(msgRcvd, "QUIT") && ret != INTACT_QUIT) {
		int i, printed;
		static int nextEvent = FALSE;
		
		g_cSJob = NULL;
		
		if (!strncmp(msgRcvd, "PSHJ", CMD_LENGTH) && !g_lastJobSent)
			strcpy(msgRcvd, "REDY");

		for (i = 0; i < END_SIM_CMD && strncmp(msgRcvd, cmds[i].cmd, strlen(cmds[i].cmd)); i++);

		if (i == END_SIM_CMD)
			sprintf(msgToSend, "ERR: invalid command (%s)", msgRcvd);
		else
		if (cmds[i].CmdHandler) {
			//char *buffer = (char *)calloc(LARGE_BUF_SIZE, sizeof(char));
			char buffer[LARGE_BUF_SIZE];
			ret = cmds[i].CmdHandler(msgRcvd, buffer);	// actual command handling
			if (ret == INTACT_QUIT) goto Quit;
			strcpy(msgToSend, buffer);
			//free(buffer);
		}
		
		// if there is any completed job in this round; 
		if (g_cSJob && (!g_ncJobs || !(g_cSJob->sType == g_ncJobs->sJob->sType && g_cSJob->sID == g_ncJobs->sJob->sID)))
			RunReadyJobs(&g_systemInfo.servers[g_cSJob->sType][g_cSJob->sID]);

		// if the 'o' option is used and there has been a failure event (RESF/RESR), 
		// skip/omit to send the message, primarily for the sake of fast simulation
		if ((!strncmp(msgToSend, "RESF", 4) || !strncmp(msgToSend, "RESR", 4)) && simOpts[SO_OmitFailMsg].used) continue;

		printed = SendMsg(msgToSend);
		if (printed && (nextEvent || (
			simOpts[SO_BreakPoint].used && !strncmp(msgToSend, "JOB", 3) && g_sConfig.bp == g_ss.curJobID-1))) {
			nextEvent = BreakPointMode(msgToSend, nextEvent);
		}

		ret = RecvMsg(msgToSend, msgRcvd);
	}

	if (!simOpts[SO_Interactive].used || ret != INTACT_QUIT) {
		// complete the execution of all scheduled jobs	
		g_ss.curSimTime = UINT_MAX;
		g_ss.curSimTime = UpdateServerStates();
		
		strcpy(msgToSend, "QUIT");
		SendMsg(msgToSend);
		
		if (g_ss.waitJQ || g_ss.curJobID < g_workloadInfo.numJobs) { // scheduling incomplete
			fprintf(stderr, "%d jobs not scheduled!\n", g_workloadInfo.numJobs - g_ss.curJobID + CountWJobs());
			if (simOpts[SO_Verbose].used)
				PrintUnscheduledJobs();
		}
		if (!simOpts[SO_Interactive].used)
			close(g_fd);

		if (!g_sConfig.jobFile)
			WriteJobs();
		
		if (g_systemInfo.rFailT)
			WriteResFailures();
		
		PrintStats();
	}
Quit:	
	FreeAll();
	
	return 0;
}

void PrintWelcomeMsg(int argc, char **argv)
{
	int i;
	char port[LARGE_BUF_SIZE];
	
	printf("# ds-sim server %s\n", VERSION);
	printf("# Server-side simulator started with '");
	for (i = 0; i < argc; i++) {
		printf("%s", argv[i]);
		if (i + 1 < argc)
			printf(" ");
	}
	printf("'\n");
	sprintf(port, "%d", g_sConfig.port);
	printf("# Waiting for connection to port %s of IP address 127.0.0.1\n", port);
}

void InitSim()
{
	int i;
	
	g_sConfig.port = limits[Port_Limit].def;
	g_sConfig.configFile = FALSE;
	g_sConfig.configFilename[0] = '\0';
	g_sConfig.jobFile = FALSE;
	g_sConfig.jobFileName[0] = '\0';
	g_sConfig.rseed = 0;
	g_sConfig.resLimit = 0;
	g_sConfig.termination.simEndTime = limits[SEnd_Limit].max;
	g_sConfig.termination.maxJobCnt = limits[JCnt_Limit].def;
	g_sConfig.regGETSRecLen = 
	g_sConfig.failGETSRecLen = 
	g_sConfig.LSTJRecLen = 0;
	for (i = 0; i < END_RDF; i++)
		g_sConfig.regGETSRecLen += regGETSFSizes[i].maxLength;
	g_sConfig.regGETSRecLen += END_RDF;		// for spaces between fields
	for (i = 0; i < END_FDF; i++)
		g_sConfig.failGETSRecLen += failGETSFSizes[i].maxLength;
	g_sConfig.failGETSRecLen += END_FDF;	// for spaces between fields
	for (i = 0; i < END_LF; i++)
		g_sConfig.LSTJRecLen += LSTJFSizes[i].maxLength;
	g_sConfig.LSTJRecLen += END_LF;			// for spaces between fields
	
	g_systemInfo.numServTypes = limits[SType_Limit].def;
	g_systemInfo.maxServType = UNKNOWN;
	g_systemInfo.totalNumServers = 0;
	g_systemInfo.sTypes = NULL;
	g_systemInfo.servers = NULL;
	g_systemInfo.rFailT = NULL;
	
	g_workloadInfo.type = UNDEFINED;
	g_workloadInfo.name = NULL;
	g_workloadInfo.minLoad = 
	g_workloadInfo.maxLoad = 
	g_workloadInfo.loadOffset = 0;
	g_workloadInfo.avgLowTime =
	g_workloadInfo.avgTransitTime = 
	g_workloadInfo.avgHighTime = 0;
	g_workloadInfo.numJobTypes = 0;
	g_workloadInfo.jobTypes = NULL;
	g_workloadInfo.numJobs = 0;
	g_workloadInfo.jobGr = NULL;
	g_workloadInfo.jobs = NULL;
	
	for (int i = 0; i < END_SIM_OPTION; i++)
		simOpts[i].used = FALSE;
}


void CreateResFailTrace()
{
	g_systemInfo.rFailT = (ResFailureTrace *)malloc(sizeof(ResFailureTrace));
	ResFailureTrace *rFT = g_systemInfo.rFailT;
	
	assert(g_systemInfo.rFailT);
	
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
	close(g_fd);
	exit(EXIT_FAILURE);
}

void CompleteRecvMsg(char *msg, int length)
{
	if (simOpts[SO_Newline].used) {
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
	if (simOpts[SO_Newline].used) {
		msg[length] = '\n';
		msg[length+1] = '\0';
	}
}

int SendMsg(char *msg)
{
	char *orgMsg = strdup(msg);
	int ret = 0;
	
	CompleteSendMsg(msg, strlen(msg));
	if (!simOpts[SO_Interactive].used) {
		if ((send(g_conn, msg, strlen(msg), 0)) < 0) {
			fprintf(stderr, "%s wasn't not sent successfully!\n", msg);
			GracefulExit();
		}

		if (simOpts[SO_Verbose].used == VERBOSE_ALL ||
			(simOpts[SO_Verbose].used == VERBOSE_BRIEF && 
				(!strncmp(orgMsg, "JOB", 3) || 
				!strcmp(orgMsg, "OK") || 
				!strcmp(orgMsg, "NONE") ||
				!strncmp(orgMsg, "JCPL", CMD_LENGTH) ||
				!strncmp(orgMsg, "RESF", CMD_LENGTH) ||
				!strncmp(orgMsg, "RESR", CMD_LENGTH) ||
				!strcmp(orgMsg, "QUIT"))))
					ret = printf("SENT %s\n", orgMsg);
	}
	else
		ret = printf("%s\n", orgMsg);

	free(orgMsg);
	
	return ret;
}

int RecvMsg(char *msgSent, char *msgRcvd)
{
	int recvCnt;
	int ret = 0;
	
	if (!simOpts[SO_Interactive].used) {
		recvCnt = read(g_conn, msgRcvd, LARGE_BUF_SIZE - 1);
		CompleteRecvMsg(msgRcvd, recvCnt);
		
		if (simOpts[SO_Verbose].used == VERBOSE_ALL ||
			(simOpts[SO_Verbose].used == VERBOSE_BRIEF && 
				(!strcmp(msgRcvd, "HELO") ||
				!strncmp(msgRcvd, "AUTH", CMD_LENGTH) || 
				!strcmp(msgRcvd, "REDY") || 
				!strncmp(msgRcvd, "SCHD", CMD_LENGTH) || 
				!strncmp(msgRcvd, "MIGJ", CMD_LENGTH) || 
				!strcmp(msgRcvd, "QUIT")))) {
				ret = printf("RCVD %s\n", msgRcvd); 
		}
	}
	else {
		ret = InteractiveMode(msgSent, msgRcvd);
	}
	
	return ret;
}

inline int IsValidNum(char *buf)
{
	for (int i = 0; i < strlen(buf); i++)
		if (!isdigit(buf[i]))
			return FALSE;
	
	return TRUE;
}

int BreakPointMode(char *msgToSend, int nextEvent)
{
	char cmd[DEFAULT_BUF_SIZE];
	int jID = UNDEFINED;

	nextEvent = FALSE;

	while (!nextEvent && jID == UNDEFINED) {
		printf("\n# \033[4m\033[1mn\033[24m\033[22mext event|\033[4mjob id\033[24m|\033[4m\033[1mp\033[24m\033[22mrint \033[4mserver_type\033[24m [\033[4mserver_id\033[24m]|\033[4m\033[1mq\033[24m\033[22muit > ");
		int ch = getc(stdin);
		if (ch != '\n') {
			ungetc(ch, stdin);
			fgets(cmd, DEFAULT_BUF_SIZE, stdin);
		}

		if (ch == '\n' || cmd[0] == 'n')
			nextEvent = TRUE;
		else {
			int numFields = UNDEFINED, servType, sID;
			char *optStr, *token;
			
			switch (cmd[0]) {
				case 'q':
					jID = INT_MAX;
					break;
				case 'p':
					optStr = strdup(cmd);
					token = strtok(optStr, " ");
					if (isspace(token[strlen(token)-1]))
							token[strlen(token)-1] = '\0';

					if (strcmp(token, "p")) 
						break;
					numFields = 0;
					if ((token = strtok(0, " "))) {
						if (isspace(token[strlen(token)-1]))
							token[strlen(token)-1] = '\0';
						servType = FindResTypeByName(token);
						if (servType != UNDEFINED) {
							numFields++;
							if ((token = strtok(0, " "))) {
								if (isspace(token[strlen(token)-1]))
									token[strlen(token)-1] = '\0';
								if (IsValidNum(token) && (sID = atoi(token)) >= 0 && sID < GetServerLimit(servType))
									numFields++;
							}
						}
					}

					if (numFields == 0)	{ // print the states of all servers
						SendResInfoAll(FALSE);
						FinalizeBatchMsg();
					}
					else {
						if (numFields == 1) {	// print the states of all servers of specified type
							SendResInfoByType(servType, NULL, FALSE);
							FinalizeBatchMsg();
			
						}
						else { // print the state of specified server
							SendResInfoByServer(servType, sID, NULL, FALSE);
							FinalizeBatchMsg();
						}
					}
					puts(g_batchMsg);
					free(g_batchMsg);
					g_batchMsg = NULL;
					free(optStr);
					break;
				default:
					if (isdigit(cmd[0]) && 
						((jID = GetIntValue(cmd, limits[JCnt_Limit].name)) == UNDEFINED || 
						IsOutOfBound(jID, 0, limits[JCnt_Limit].max, limits[JCnt_Limit].name)))
						jID = UNDEFINED;
			}
			if (jID != UNDEFINED)
				g_sConfig.bp = jID;
		}
	}
	
	return nextEvent;
}

// IsCmd checks the validity of cmd (cmdStr) and also cleans cmd if valid
int IsCmd(char *cmdStr)
{
	int cmdLen, i, j, k;
	char buffer[DEFAULT_BUF_SIZE], cmd[DEFAULT_BUF_SIZE];

	cmdLen = strlen(cmdStr);
	
	for (i = 0; i < cmdLen && isspace(cmdStr[i]); i++);
	if (i == cmdLen) return FALSE;
	for (j = 0; i < cmdLen && isalpha(cmdStr[i]); i++, j++)
		cmd[j] = toupper(cmdStr[i]);
	cmd[j] = '\0';
	for (k = 0; k < END_SIM_CMD; k++) {
		if ((cmds[k].issuer == CI_Client || cmds[k].issuer == CI_Both) && !strcmp(cmd, cmds[k].cmd)) {
			for (; i < cmdLen && isspace(cmdStr[i]); i++);
			if (i == cmdLen)
				strcpy(buffer, cmd);
			else {
				// the option cmd must start with a uppercase letter, e.g., All and Type
				if ((!strcmp(cmd, "GETS") || !strcmp(cmd, "RESC")) && isalpha(cmdStr[i]))
					cmdStr[i] = toupper(cmdStr[i]);
				sprintf(buffer, "%s %s", cmd, &cmdStr[i]);
			}
			strcpy(cmdStr, buffer); // make the command uppercase when valid
			if (cmdStr[strlen(cmdStr) - 1] == '\n')	// get rid of '\n'
				cmdStr[strlen(cmdStr) - 1] = '\0';

			return TRUE;
		}
	}
	
	return FALSE;
}

int InteractiveMode(char *msgSent, char *msgRcvd)
{
	char cmd[DEFAULT_BUF_SIZE] = " ";

	while (TRUE) {
		int numFields = UNDEFINED, servType, sID, ch;
		char *optStr, *token;
		
		cmd[0] = '\0';
		printf("\n# \033[4m\033[1mn\033[24m\033[22mext event|");
		printf("\033[4m\033[1mp\033[24m\033[22mrint \033[4mserver_type\033[24m [\033[4mserver_id\033[24m]|");
		printf("\033[4m\033[1mr\033[24m\033[22mepeat prev cmd|\033[4mds-sim cmd\033[24m|\033[4m\033[1mq\033[24m\033[22muit > ");
		
		ch = getc(stdin);
		if (ch != '\n') {
			ungetc(ch, stdin);
			fgets(cmd, DEFAULT_BUF_SIZE, stdin);
		}

		strcpy(msgRcvd, cmd);

		if (ch == '\n' || cmd[0] == 'n')
			return INTACT_NEXT_EVENT;

		if (IsCmd(cmd)) break;	// IsCmd also cleans cmd if valid
		switch (cmd[0]) {
			case 'p':
				optStr = strdup(cmd);
				token = strtok(optStr, " ");
				if (isspace(token[strlen(token)-1]))
						token[strlen(token)-1] = '\0';

				if (strcmp(token, "p")) 
					break;
				numFields = 0;
				if ((token = strtok(0, " "))) {
					if (isspace(token[strlen(token)-1]))
						token[strlen(token)-1] = '\0';
					servType = FindResTypeByName(token);
					if (servType != UNDEFINED) {
						numFields++;
						if ((token = strtok(0, " "))) {
							if (isspace(token[strlen(token)-1]))
								token[strlen(token)-1] = '\0';
							if (IsValidNum(token) && (sID = atoi(token)) >= 0 && sID < GetServerLimit(servType))
								numFields++;
						}
					}
				}

				if (numFields == 0)	{ // print the states of all servers
					SendResInfoAll(FALSE);
					FinalizeBatchMsg();
				}
				else {
					if (numFields == 1) {	// print the states of all servers of specified type
						SendResInfoByType(servType, NULL, FALSE);
						FinalizeBatchMsg();
		
					}
					else { // print the state of specified server
						SendResInfoByServer(servType, sID, NULL, FALSE);
						FinalizeBatchMsg();
					}
				}
				puts(g_batchMsg);
				free(g_batchMsg);
				g_batchMsg = NULL;
				free(optStr);
				break;
			case 'q':
				return INTACT_QUIT;
			case 'r':
				printf("%s\n", msgSent);
				break;
			default:
				printf("No such command!\n");
				break;
		}
	}
	
	strcpy(msgRcvd, cmd);
	
	return INTACT_CMD;
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
	char optStr[LARGE_BUF_SIZE] = "";
	int optlen;
	
	opterr = 0;
	errno = 0;

	// construct option string
	for (i = 0; simOpts[i].desc; i++) {
		sprintf(optStr, "%s%c", optStr, simOpts[i].optChar);
		if (simOpts[i].useArg)
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
				if (!strcmp(optarg, "cmd"))
					Help(H_Cmd);
				else
					Help(H_All);
				
				simOpts[SO_Help].used = TRUE;
				
				return FALSE;
			case 'v': 
				optlen = strlen(optarg);
				
				if (!strncmp(optarg, verboseOptions[VERBOSE_ALL].optstr, verboseOptions[VERBOSE_ALL].optlen))
					simOpts[SO_Verbose].used = VERBOSE_ALL;
				else
				if (!strncmp(optarg, verboseOptions[VERBOSE_BRIEF].optstr, verboseOptions[VERBOSE_BRIEF].optlen))
					simOpts[SO_Verbose].used = VERBOSE_BRIEF;
				else
				if (!strncmp(optarg, verboseOptions[VERBOSE_STATS].optstr, verboseOptions[VERBOSE_STATS].optlen))
					simOpts[SO_Verbose].used = VERBOSE_STATS;
				else
					simOpts[SO_Verbose].used = VERBOSE_NONE;

				if (simOpts[SO_Verbose].used && optarg[optlen - 1] == '+')
					g_perServerStats = TRUE;

				break;
			case 'b':	// set a break point
				if ((retValue = GetIntValue(optarg, limits[JCnt_Limit].name)) == UNDEFINED)
					retValue = 0;	// set to the first job by default
				if (IsOutOfBound(retValue, 0, limits[JCnt_Limit].max, limits[JCnt_Limit].name))
					return FALSE;
				g_sConfig.bp = retValue;
				simOpts[SO_BreakPoint].used = TRUE;
				break;
			case 'i':	// interactive mode
				simOpts[SO_Interactive].used = TRUE;
				break;
			case 'j':	// job count
				if ((retValue = GetIntValue(optarg, limits[JCnt_Limit].name)) == UNDEFINED)
					return FALSE;
				if (IsOutOfBound(retValue, limits[JCnt_Limit].min, limits[JCnt_Limit].max, limits[JCnt_Limit].name))
					return FALSE;
				g_sConfig.termination.maxJobCnt = retValue;
				simOpts[SO_JobCount].used = TRUE;
				break;
			case 'd':	// end time
				if ((retValue = GetIntValue(optarg, limits[SEnd_Limit].name)) == UNDEFINED)
					return FALSE;
				if (IsOutOfBound(retValue, limits[SEnd_Limit].min, limits[SEnd_Limit].max, limits[SEnd_Limit].name))
					return FALSE;
				g_sConfig.termination.simEndTime = retValue;
				simOpts[SO_Duration].used = TRUE;
				break;
			case 'r':	// random seed
				if ((retValue = GetIntValue(optarg, limits[RSeed_Limit].name)) == UNDEFINED)
					return FALSE;
				if (IsOutOfBound(retValue, limits[RSeed_Limit].min, limits[RSeed_Limit].max, limits[RSeed_Limit].name))
					return FALSE;
				if (!g_sConfig.rseed) {
					g_sConfig.rseed = retValue;
					srand(g_sConfig.rseed);
					simOpts[SO_RandomSeed].used = TRUE;
				}
				break;
			case 'l':	// resource limit
				if ((retValue = GetIntValue(optarg, limits[Res_Limit].name)) == UNDEFINED)
					return FALSE;
				if (IsOutOfBound(retValue, limits[Res_Limit].min, limits[Res_Limit].max, limits[Res_Limit].name))
					return FALSE;
				g_sConfig.resLimit = retValue;
				simOpts[SO_Limit].used = TRUE;
				break;
			case 'c':	// configuration file
				if (access(optarg, R_OK) == -1) {
					fprintf(stderr, "No such file (%s) exist!\n", optarg);
					return FALSE;
				}
				g_sConfig.configFile = TRUE;
				strcpy(g_sConfig.configFilename, optarg);
				simOpts[SO_Config].used = TRUE;
				break;
			case 'f':	// resource failure
					if (!g_systemInfo.rFailT)
						CreateResFailTrace();

					g_systemInfo.rFailT->fModel = GetFailureModel(optarg);
					if (g_systemInfo.rFailT->fModel == END_FAILURE_MODEL) {
						fprintf(stderr, "Invalid resource failure model!\n");
						return FALSE;
					}
					simOpts[SO_Failure].used = TRUE;
				break;
			case 'o':
				simOpts[SO_OmitFailMsg].used = TRUE;
				break;
			case 'p':	// port number
				if ((retValue = GetIntValue(optarg, limits[Port_Limit].name)) == UNDEFINED)
					return FALSE;
				if (IsOutOfBound(retValue, limits[Port_Limit].min, limits[Port_Limit].max, limits[Port_Limit].name))
					return FALSE;
				g_sConfig.port = retValue;
				simOpts[SO_Port].used = TRUE;
				break;
			case 'n':	// new line ('\n') at the end of each message
				simOpts[SO_Newline].used = TRUE;
				break;
			case 'g':
				if ((retValue = GetIntValue(optarg, limits[T_Grain_Limit].name)) == UNDEFINED)
					return FALSE;
				if (IsOutOfBound(retValue, limits[T_Grain_Limit].min, limits[T_Grain_Limit].max, limits[T_Grain_Limit].name))
					return FALSE;
				g_systemInfo.rFailT->timeGrain = retValue;
				simOpts[SO_Granularity].used = TRUE;
				break;
			case 's': // failure distribution scale factor
				if ((retValue = GetIntValue(optarg, limits[F_Scale_Limit].name)) == UNDEFINED)
					return FALSE;
				if (IsOutOfBound(retValue, limits[F_Scale_Limit].min, limits[F_Scale_Limit].max, limits[F_Scale_Limit].name))
					return FALSE;
				g_systemInfo.rFailT->scaleFactor = retValue;
				simOpts[SO_Scale].used = TRUE;
				break;
			case '?':
				if (optopt == 'b' || optopt == 'j' || optopt == 'd' || optopt == 's' || optopt == 'r' ||
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

	if (g_sConfig.configFile) {
		if (!(g_sConfig.configFile = ReadSimConfig(g_sConfig.configFilename))) {
			fprintf(stderr, "Configuration file (%s) invalid!\n", g_sConfig.configFilename);
			return FALSE;
		}
	}
	
	if (simOpts[SO_BreakPoint].used && simOpts[SO_Verbose].used < VERBOSE_BRIEF) {
		simOpts[SO_Verbose].used = VERBOSE_BRIEF;
	}

	return TRUE;
}

void Help(int opt)
{
	printf("ds-sim (ds-server: %s)\n\t by %s\n\n", VERSION, DEVELOPERS);
	switch (opt) {
		case H_All:
			ShowUsage();
			ShowCmds();
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
		case H_Cmd:
			ShowCmds();
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
	for (int i = 0; simOpts[i].desc; i++) {
		if (simOpts[i].useArg)
			printf("-%c %s\n", simOpts[i].optChar, simOpts[i].optArg);
		else
			printf("-%c\n", simOpts[i].optChar);
		printf("\t%s\n\n", simOpts[i].desc);
	}
	printf("---------------------- [ Client-side command line options ] --------------------\n");
	printf("-a scheduling algorithm name\n\n");
}

/*
	May have options to show (1) a list of commands with just a short description and (2) show usage
*/
void ShowCmds()
{
	int i;

	printf("---------------------------------- [ Commands ] --------------------------------\n");
	for (i = 0; i < END_SIM_CMD; i++)
		printf("%s (%s): %s\n", cmds[i].cmd, issuers[cmds[i].issuer], cmds[i].desc);
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
	for (i = 0; servStates[i].id != UNKNOWN; i++)
		printf("%d: %s\n", servStates[i].id, servStates[i].state);
	
	printf("--------------------------------- [ Job states ] -------------------------------\n");
	for (i = 0; jobStates[i].id != UNKNOWN; i++)
		printf("%d: %s\n", jobStates[i].id, jobStates[i].state);

}

void SetSimTermCond()
{
	// set max to one that has not been specified since simulation ends whichever condition meets first
	if (!g_sConfig.termination.simEndTime && !g_sConfig.termination.maxJobCnt)
		g_sConfig.termination.maxJobCnt = limits[JCnt_Limit].def;
	if (!g_sConfig.termination.simEndTime)
		g_sConfig.termination.simEndTime = limits[SEnd_Limit].max;
	if (!g_sConfig.termination.maxJobCnt)
		g_sConfig.termination.maxJobCnt = limits[JCnt_Limit].max;
}


void CreateResFailureLists()
{
	Server **servers = g_systemInfo.servers;
	int i, j, nsTypes = g_systemInfo.numServTypes;
	int actNumFailures = g_systemInfo.rFailT->numFailues / 2;	// due to the addition of a recovery event for every failure
	ServerTypeProp *sTypes = g_systemInfo.sTypes;

	for (i = 0; i < nsTypes; i++) {
		int limit = sTypes[i].limit;

		for (j = 0; j < limit; j++) {
			Server *server = &servers[i][j];
			server->failInfo.fIndices = (int *) malloc(sizeof(int) * actNumFailures);
			server->failInfo.rIndices = (int *) malloc(sizeof(int) * actNumFailures);
		}
	}
}

void CompleteConfig()
{
	SetSimTermCond();
	if (g_systemInfo.rFailT)
		CreateResFailureLists();
}

void CreateServerTypes(int nsTypes) 
{
	g_systemInfo.servers = (Server **)calloc(nsTypes, sizeof(Server *));	// allocate mem and initialise
	assert(g_systemInfo.servers);
	g_systemInfo.sTypes = (ServerTypeProp *)calloc(nsTypes, sizeof(ServerTypeProp));
	assert(g_systemInfo.sTypes);
}

// store server properties per type
int StoreServerType(xmlNode *node)
{
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	int i, retValue, nsTypes = g_systemInfo.numServTypes;
	static int maxCoreCnt = 0;
	ServerRes *capacity;
	
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
	if (strlen(sTypes[i].name) > SERVER_TYPE_LENGTH) {
		fprintf(stderr, "ERR: server type name, %s is too long (max: %d)\n", sTypes[i].name, SERVER_TYPE_LENGTH);
		return FALSE;
	}
	
	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"limit"), "Resource limit")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[Res_Limit].min, limits[Res_Limit].max, limits[Res_Limit].name))
		return FALSE;
	sTypes[i].limit = retValue;
	
	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"bootupTime"), "Bootup time")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[SBootTime_Limit].min, limits[SBootTime_Limit].max, limits[SBootTime_Limit].name))
		return FALSE;
	sTypes[i].bootupTime = retValue;
	
	if ((sTypes[i].rate = GetFloatValue((char *)xmlGetProp(node, (xmlChar *)"hourlyRate"), "Server rental rate")) == UNDEFINED ||
		IsOutOfBound(retValue, limits[SHourlyRate_Limit].min, limits[SHourlyRate_Limit].max, limits[SHourlyRate_Limit].name))
		return FALSE;
	
	capacity = &sTypes[i].capacity;
	
	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"cores"), "Core count")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[CCnt_Limit].min, limits[CCnt_Limit].max, limits[CCnt_Limit].name))
		return FALSE;
	capacity->cores = retValue;
	
	// max server type is determined by the number of cores; 
	// memory and disk are supposed to be "proportionally" set based on core count
	if (capacity->cores > maxCoreCnt) {
		maxCoreCnt = capacity->cores;
		g_systemInfo.maxServType = i;
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
	
	g_systemInfo.numServTypes++;
	
	return TRUE;
}

void CreateJobTypes(int njTypes) 
{
	g_workloadInfo.jobTypes = (JobTypeProp *)calloc(njTypes, sizeof(JobTypeProp));
	assert(g_workloadInfo.jobTypes);
}

// store job properties per type
int StoreJobType(xmlNode *node)
{
	JobTypeProp *jTypes = g_workloadInfo.jobTypes;
	int i, retValue, njTypes = g_workloadInfo.numJobTypes;
	
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
	if (strlen(jTypes[i].name) > JOB_TYPE_LENGTH) {
		fprintf(stderr, "ERR: job type name, %s is too long (max: %d)\n", jTypes[i].name, JOB_TYPE_LENGTH);
		return FALSE;
	}
	
	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"minRunTime"), "Job min run time")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[JRunTime_Limit].min, limits[JRunTime_Limit].max - 1, limits[JRunTime_Limit].name))
		return FALSE;
	jTypes[i].min = retValue;
	
	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"maxRunTime"), "Job max run time")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[JRunTime_Limit].min, limits[JRunTime_Limit].max, limits[JRunTime_Limit].name))
		return FALSE;
	jTypes[i].max = retValue;
	
	if (jTypes[i].max <= jTypes[i].min) {
		fprintf(stderr, "max job runtime must be greater than min job runtime!\n");
		return FALSE;
	}
	
	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"populationRate"), "Job population rate")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[JPop_Limit].min, limits[JPop_Limit].max, limits[JPop_Limit].name))
		return FALSE;
	jTypes[i].rate = retValue;
	
	if (!jTypes[i].min)
		jTypes[i].min = limits[JRunTime_Limit].min;
	if (!jTypes[i].max)
		jTypes[i].max = jTypes[i].min + 1;
	
	g_workloadInfo.numJobTypes++;
	
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
	g_workloadInfo.name = strdup((char *)xmlGetProp(node, (xmlChar *)"type"));

	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"minLoad"), "Min load")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[WMinLoad_Limit].min, limits[WMinLoad_Limit].max, limits[WMinLoad_Limit].name))
		return FALSE;
	g_workloadInfo.minLoad = retValue;

	if ((retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"maxLoad"), "Max load")) == UNDEFINED || 
		IsOutOfBound(retValue, limits[WMaxLoad_Limit].min, limits[WMaxLoad_Limit].max, limits[WMaxLoad_Limit].name))
		return FALSE;
	g_workloadInfo.maxLoad = retValue;
	
	g_workloadInfo.loadOffset = (g_workloadInfo.maxLoad - g_workloadInfo.minLoad) / GetNumLoadTransitions();
	
	if ((char *)xmlGetProp(node, (xmlChar *)"avgLowTime") &&
		(retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"avgLowTime"), "avgLowTime")) != UNDEFINED && 
		IsOutOfBound(retValue, limits[WorkloadTime_Limit].min, limits[WorkloadTime_Limit].max, limits[WorkloadTime_Limit].name))
		return FALSE;
	g_workloadInfo.avgLowTime = retValue;

	if ((char *)xmlGetProp(node, (xmlChar *)"avgHighTime") &&
		(retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"avgHighTime"), "avgHighTime")) != UNDEFINED && 
		IsOutOfBound(retValue, limits[WorkloadTime_Limit].min, limits[WorkloadTime_Limit].max, limits[WorkloadTime_Limit].name))
		return FALSE;
	g_workloadInfo.avgHighTime = retValue;

	if ((char *)xmlGetProp(node, (xmlChar *)"avgTransitTime") &&
		(retValue = GetIntValue((char *)xmlGetProp(node, (xmlChar *)"avgTransitTime"), "avgTransitTime")) != UNDEFINED && 
		IsOutOfBound(retValue, limits[WorkloadTime_Limit].min, limits[WorkloadTime_Limit].max, limits[WorkloadTime_Limit].name))
		return FALSE;
	g_workloadInfo.avgTransitTime = retValue;

	return TRUE;
}


inline int GetFailureModel(char *fModelName)
{
	int i;
	
	for (i = 0; i < END_FAILURE_MODEL && strcmp(fModelName, resFailureModels[i].model); i++);
		
	return (i < END_FAILURE_MODEL ? i : END_FAILURE_MODEL);
}

/*int GetFailureTrace(char *fTraceName)
{
	int fTrace = UNKNOWN;
	
	return fTrace;
}*/


inline int GetServProp(int no, unsigned short *sID)
{
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	int i, sTypeID, tServCnt;
	int numServTypes = g_systemInfo.numServTypes;
	
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


void *Malloc(size_t size)
{
	return (size ? malloc(size) : NULL);
}

void *Realloc(void *mem, size_t size)
{
	return (size ? realloc(mem, size) : NULL);
}

//********************** RESOURCE FAILURE GENERATION (Jayden) ********************
// make_and_save_distribution
//void distGen(timeP time, nodeP node, char* trace, char* config) {
int GenerateResFailures(int fModel)
{
	int rawTotalServers = g_systemInfo.totalNumServers;
	int totalTime = g_sConfig.termination.simEndTime * 10;	// 10x longer time for failures as jobs may run after simEndTime, due to failures for example
	nodeP *nParam = &resFailureModels[fModel].nParam;
	timeP *tParam = &resFailureModels[fModel].tParam;
    int totalServers = rawTotalServers * nParam->failureRatio * (g_systemInfo.rFailT->scaleFactor / 100.0f);
	int timeGrain = g_systemInfo.rFailT->timeGrain;
	int totalTimeMin = totalTime / (float)timeGrain + 1;
	int *sIndices = GetRandomServIndices(rawTotalServers, totalServers);
	PreGenLogNormal **preGenLognorm;
	int numMaxServers = g_systemInfo.sTypes[g_systemInfo.maxServType].limit;
	int numNonFServers = ROUND(log2(numMaxServers) / 2.0);
	/*int	numNonFServers = numMaxServers >= nonFailure * minNonFServers ? 
						ROUND(numMaxServers * (nonFailure / 100.0)) : 
						numMaxServers > minNonFServers ? minNonFServers : ROUND(numMaxServers / 2.0);	
	*/

    // pre_generate_lognormal_time_distribution
    int maxNum = ((totalServers/10) > 10) ? (totalServers/10) : 10;	// use only 10% of servers if more than 100 servers
	preGenLognorm = (PreGenLogNormal **)malloc(sizeof(PreGenLogNormal *) * maxNum);
    
	if (simOpts[SO_Verbose].used == VERBOSE_ALL)
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
		failedTime *= g_systemInfo.rFailT->scaleFactor / 100.0f;
		
        totalFailedTime += failedTime;
        ftCount[i] = failedTime;
        
		nodeDArr[i] = (int *)Malloc(sizeof(int) * failedTime);
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
	
	int sFailIndex = 0;
		
	g_systemInfo.rFailT->numFailues = totalFailures * 2;	// for every failure, its corresponding recovery is added; hence, * 2.
	g_systemInfo.rFailT->resFailures = (ResFailure *)Malloc(sizeof(ResFailure) * totalFailures * 2);
	ResFailure *resFailures = g_systemInfo.rFailT->resFailures;

	if (totalFailures) {
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
					if (resFailures[sFailIndex].servType == g_systemInfo.maxServType && 
						resFailures[sFailIndex].servID >= g_systemInfo.sTypes[resFailures[sFailIndex].servType].limit - numNonFServers)
						continue;
						
					resFailures[sFailIndex].eventType = RES_Failure;
					sFailIndex++;
					AddResRecovery(&resFailures[sFailIndex - 1], &resFailures[sFailIndex]);
					sFailIndex++;
				}
				
			}
		}
		free(ftDur);
	}

	free(ftCount);
    freeArr(nodeDArr, totalServers);

    if (simOpts[SO_Verbose].used == VERBOSE_ALL)
		printf("Total #failures: %d\n", totalFailures);
	
	// Realloc ensures no mem allocation when no failures (totalFailures/sFailIndex == 0)
    resFailures = (ResFailure *)Realloc(resFailures, sizeof(ResFailure) * sFailIndex);
	g_systemInfo.rFailT->numFailues = sFailIndex;
	
	qsort(resFailures, sFailIndex, sizeof(ResFailure), CmpResFailures);
	
#ifdef FAIL_DEBUG
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
        if (arr[i]) 
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


int ReadResFailure(FILE *file, ResFailure *resFail)
{	char servType[LARGE_BUF_SIZE];

	int retValue = fscanf(file, "%d %d %s %hu", &resFail->startTime, &resFail->endTime, servType, &resFail->servID);
	if (retValue != EOF && retValue == NUM_RES_FAILURE_FIELDS) {
		resFail->servType = FindResTypeByName(servType);
		resFail->eventType = RES_Failure;
	}
	
	return retValue;
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
	int retValue;
	
	fFile = fopen(failFileName, "r");
	
	if (!fFile) {
		fprintf(stderr, "%s: no such file found!\n", failFileName);
		return FALSE;
	}
	
	SkipComments(fFile);
	
	g_systemInfo.rFailT->numFailues = 0;
	g_systemInfo.rFailT->resFailures = (ResFailure *)calloc(maxFailures, sizeof(ResFailure));
	resFailures = g_systemInfo.rFailT->resFailures;

	// for every resource failure event, there is an extra event (resource recovery) created 
	// with the end time of failure (endTime)
	while ((retValue = ReadResFailure(fFile, &resFailures[fCnt++])) != EOF) {
		assert(retValue == NUM_RES_FAILURE_FIELDS);	
		AddResRecovery(&resFailures[fCnt - 1], &resFailures[fCnt]);
		fCnt++;
		// not enough space to record failures
		// fCnt + 1 as we add two records (failure and recovery) in batch
		if (fCnt + 1 >= maxFailures) {	
			maxFailures += DEFAULT_RES_FAILURES;	// increase by DEFAULT_RES_FAILURES (i.e., 1000)
			g_systemInfo.rFailT->resFailures = (ResFailure *)realloc(resFailures, sizeof(ResFailure) * maxFailures);
			resFailures = g_systemInfo.rFailT->resFailures;
		}
	}
	
	if (fCnt > 0)
		g_systemInfo.rFailT->resFailures = (ResFailure *)realloc(resFailures, sizeof(ResFailure) * fCnt);
	else {
		free(g_systemInfo.rFailT->resFailures);
		g_systemInfo.rFailT->resFailures = NULL;
	}
	g_systemInfo.rFailT->numFailues = fCnt;
	
	fclose(fFile);
	
	// sort resource failure events by startTime, 
	// so that the resource recovery events in particular can be handled properly
	qsort(g_systemInfo.rFailT->resFailures, fCnt, sizeof(ResFailure), CmpResFailures);
#ifdef DEBUG
	for (int i = 0; i < fCnt; i++)
		fprintf(stderr, "### %d %d %hu %hu (%d)\n", 
			g_systemInfo.rFailT->resFailures[i].startTime, 
			g_systemInfo.rFailT->resFailures[i].endTime,
			g_systemInfo.rFailT->resFailures[i].servType,
			g_systemInfo.rFailT->resFailures[i].servID,
			g_systemInfo.rFailT->resFailures[i].eventType);
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
				if (!xmlStrcmp(xmlGetProp(curNode, (xmlChar *)"newline"), (xmlChar *)"true"))
					simOpts[SO_Newline].used = TRUE;
				
				if (!xmlGetProp(curNode, (xmlChar *)"randomSeed"))
					g_sConfig.rseed = DEFAULT_RND_SEED;
				else {
					if ((retValue = GetIntValue((char *)xmlGetProp(curNode, (xmlChar *)"randomSeed"), "Random seed")) == UNDEFINED || 
						IsOutOfBound(retValue, limits[RSeed_Limit].min, limits[RSeed_Limit].max, limits[RSeed_Limit].name))
						return FALSE;
					g_sConfig.rseed = retValue;
				}
				srand(g_sConfig.rseed);
			}
			if (!xmlStrcmp(curNode->name, (xmlChar *)"servers")) { // (char *)xmlNodeGetContent(curNode)
			
				if (g_systemInfo.sTypes) { // sTypes already created meaning too many "servers" elements
					fprintf(stderr, "Only one system (\"servers\" element) is supported!\n");
					return FALSE;
				}
				g_systemInfo.numServTypes = 0;
				CreateServerTypes(limits[SType_Limit].max);

				// resource failures need to be dealt with after obtaining all server information 
				// as failures are generated considering servers specified in the configuration, and 
				if (((char *)xmlGetProp(curNode, (xmlChar *)"failureModel") || 
					(char *)xmlGetProp(curNode, (xmlChar *)"failureTimeGrain") ||
					(char *)xmlGetProp(curNode, (xmlChar *)"failureFile")) &&
					!(g_systemInfo.rFailT))
					CreateResFailTrace();
				else if (((char *)xmlGetProp(curNode, (xmlChar *)"failureModel") || 
					(char *)xmlGetProp(curNode, (xmlChar *)"failureTimeGrain") ||
					(char *)xmlGetProp(curNode, (xmlChar *)"failureFile")) &&
					g_systemInfo.rFailT) {
					fprintf(stderr, "The '-f' command line option is used already!\n");
					return FALSE;
				}

				if ((char *)xmlGetProp(curNode, (xmlChar *)"failureModel") && g_systemInfo.rFailT->fModel == UNDEFINED) {
					g_systemInfo.rFailT->fModel = GetFailureModel((char *)xmlGetProp(curNode, (xmlChar *)"failureModel"));
					if (g_systemInfo.rFailT->fModel == END_FAILURE_MODEL) {
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
						g_systemInfo.rFailT->ftFilename = strdup(failFileName);
					else
						return FALSE;
				}
			}
			else
			if (!xmlStrcmp(curNode->name, (xmlChar *)"server")) {
				if (!g_systemInfo.sTypes) { // sTypes not yet created meaning the "servers" element is missing
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
					g_sConfig.termination.simEndTime = retValue;
				}
				else
				if (!strcmp(str, JCNT_STR)) {
					if (IsOutOfBound(retValue, limits[JCnt_Limit].min, limits[JCnt_Limit].max, limits[JCnt_Limit].name))
						return FALSE;
					g_sConfig.termination.maxJobCnt = retValue;
				}
			}
			if (!xmlStrcmp(curNode->name, (xmlChar *)"jobs")) {
				char *jobFileName;
				
				if ((jobFileName = (char *)xmlGetProp(curNode, (xmlChar *)"file"))) {	// job list file specified
					g_sConfig.jobFile = TRUE;
					strcpy(g_sConfig.jobFileName, jobFileName);
				}
				else
				{
					// if both job list file is specified and <job> elements exist, the latter will overwrite what's read from the file
					if (g_workloadInfo.jobTypes) { // jobTypes already created meaning too many "jobs" elements
						fprintf(stderr, "Only one set of job types (\"jobs\" element) is supported!\n");
						return FALSE;
					}
					g_workloadInfo.numJobTypes = 0;
					CreateJobTypes(limits[JType_Limit].max);
				}
			}
			if (!xmlStrcmp(curNode->name, (xmlChar *)"job")) {
				if (!g_workloadInfo.jobTypes) { // jobTypes not yet created meaning the "jobs" element is missing
					fprintf(stderr, "The \"jobs\" element is missing!\n");
					return FALSE;
				}
				if (!StoreJobType(curNode))
					return FALSE;
			}
			if (!xmlStrcmp(curNode->name, (xmlChar *)"workload")) {
				if (g_workloadInfo.name) { 
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
	int nsTypes = g_systemInfo.numServTypes;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	Server **servers = g_systemInfo.servers;
	
	servers = (Server **)realloc(servers, sizeof(Server *) * nsTypes);
	assert(servers);
	g_systemInfo.servers = servers;
	sTypes = (ServerTypeProp *)realloc(sTypes, sizeof(ServerTypeProp) * nsTypes);
	assert(sTypes);
	g_systemInfo.sTypes = sTypes;
}

void SetServerRes(ServerRes *tgtSRes, ServerRes *srcSRes)
{
	tgtSRes->cores = srcSRes->cores;
	tgtSRes->mem = srcSRes->mem;
	tgtSRes->disk = srcSRes->disk;
}

void InitServers(Server *servers, ServerTypeProp *sType)
{
	int i, limit = sType->limit, type = sType->type;
	ServerRes *capacity = &sType->capacity;
	
	for (i = 0; i < limit; i++) {
		servers[i].type = type;
		servers[i].id = i;
		SetServerRes(&servers[i].availCapa, capacity);
		servers[i].startTime = UNKNOWN;
		servers[i].lastAccTime = UNKNOWN;
		servers[i].actUsage = 0;
		servers[i].totalUsage = 0;
		servers[i].failInfo.numFails = 0;
		servers[i].failInfo.totalFailTime = 0;
		servers[i].failInfo.mttf = UNKNOWN;
		servers[i].failInfo.mttr = UNKNOWN;
		servers[i].failInfo.madf = UNKNOWN;
		servers[i].failInfo.madr = UNKNOWN;
		servers[i].failInfo.lastOpTime = 0;
		servers[i].failInfo.fIndices = NULL;
		servers[i].failInfo.rIndices = NULL;
		servers[i].waiting = 
		servers[i].running = 
		servers[i].failed = 
		servers[i].suspended = 
		servers[i].completed =
		servers[i].preempted =
		servers[i].killed = NULL;
		servers[i].state = SS_Inactive;
	}
}

void CreateServers()
{
	int nsTypes = g_systemInfo.numServTypes;
	int i, totalNumServers = 0;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	Server **servers = g_systemInfo.servers;
	
	for (i = 0; i < nsTypes; i++) {
		ServerTypeProp *sType = &sTypes[i];
		int limit = sType->limit;
		
		totalNumServers += limit;
		servers[i] = (Server *)malloc(sizeof(Server) * limit);
		//InitServers(servers[i], sType->type, limit, capacity);
		InitServers(servers[i], sType);
	}
	
	g_systemInfo.totalNumServers = totalNumServers;
}

void ResizeJobTypes()
{
	g_workloadInfo.jobTypes = (JobTypeProp *)realloc(g_workloadInfo.jobTypes, sizeof(JobTypeProp) * g_workloadInfo.numJobTypes);
	assert(g_workloadInfo.jobTypes);
}

int ValidateJobRates()
{
	JobTypeProp *jobTypes = g_workloadInfo.jobTypes;
	int i, totalJobRate = 0;
	
	for (i = 0; i < g_workloadInfo.numJobTypes; i++)
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
		SortServersByResCapacity();
		CreateServers();

		if (!g_sConfig.jobFile) {
			ResizeJobTypes();

			if ((ret = ValidateJobRates()))
				g_workloadInfo.jobs = GenerateJobs();
		}
		if (g_systemInfo.rFailT) {
			if (g_systemInfo.rFailT->fModel > UNKNOWN && 
				g_systemInfo.rFailT->fModel < END_FAILURE_MODEL) 
				ret = GenerateResFailures(g_systemInfo.rFailT->fModel);
			else
			if (g_systemInfo.rFailT->ftFilename)
				ret = LoadResFailures(g_systemInfo.rFailT->ftFilename);
		}
	}

	xmlFreeDoc(doc);
	if (ret && g_sConfig.jobFile)
		ret = ReadJobs(g_sConfig.jobFileName);

	return ret;
}

void GenerateSystemInfo()
{
	int stIndex = g_systemInfo.numServTypes - 1;
	int lastSType = END_SERVER_TYPE;
	int servType;
	int i;
	ServerTypeProp *sTypes;
	Server **servers;
	
	CreateServerTypes(g_systemInfo.numServTypes);
	servers = g_systemInfo.servers;
	sTypes = g_systemInfo.sTypes;

	while (stIndex >= 0) {
		int resLimit;
		servType = stIndex + rand() % (lastSType - stIndex);
		sTypes[stIndex].type = servType;

		if (!g_sConfig.resLimit) {
			resLimit = limits[Res_Limit].min + rand() % (limits[Res_Limit].def * 2);
			// keep if it's less than 10, otherwise make it a multiple of 10
			sTypes[stIndex].limit = (resLimit <= 10) ? resLimit : resLimit + (10 - resLimit % 10);
		}
		else
			sTypes[stIndex].limit = g_sConfig.resLimit;
		
		lastSType = servType;
		stIndex--;		
	}
	
	g_systemInfo.maxServType = sTypes[g_systemInfo.numServTypes - 1].type;

	for (i = 0; i < g_systemInfo.numServTypes; i++) {
		ServerTypeProp *sType = &sTypes[i];
		ServerTypeProp *stProp = &defaultServerTypes[sType->type];
		ServerRes *capacity = &sType->capacity;
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
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	int i, nsTypes = g_systemInfo.numServTypes;
	
	for (i = 0; i < nsTypes; i++)
		totalCores += sTypes[i].limit * sTypes[i].capacity.cores;

	return totalCores;
}


int CmpCoreCnt(const void *p1, const void *p2)
{
	int left = *(int *)p1;
	int right = *(int *)p2;

	return (left - right);
}


/*
	Calculate core usage for the entire system based on simply the total number of cores in the system 
	and the total number of cores required by jobs deemed to run in parallel
*/
long int CalcTotalCoreUsage(int submitTime, Job *jobs, int jID)
{
	long int coresInUse = 0;
	int i, numServTypes, *coreCnts;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	
	numServTypes = g_systemInfo.numServTypes;
	coreCnts = (int *)malloc(sizeof(int) * numServTypes);
	
	for (i = 0; i < numServTypes; i++)
		coreCnts[i] = sTypes[i].capacity.cores;

	qsort(coreCnts, numServTypes, sizeof(int), CmpCoreCnt);	
	
	for (i = 0; i < jID; i++) {
		Job *job = &jobs[i];
		int j;
		int completionTime = job->submitTime + job->actRunTime;
		
		if (completionTime <= submitTime) continue;	// skip

		// conservative counting as jobs can't be run across more than one servers, 
		// i.e., a job requiring 5 cores cannot use a 2-core server and 3 cores of a 4-core server; 
		// rather, it is run on a server with 5 cores or more
		for (j = 0; j < numServTypes && coreCnts[j] < job->resReq.cores; j++);
		coresInUse += coreCnts[j];
	}
	
	free(coreCnts);
	
	return coresInUse;
}

/*
	Calculate current load considering the number of required cores for current job
	Specifically, the calculation only considers servers of all types that have cores >= #cores required by job
	Load is calculated by the actual core usage / the total #cores for these servers
*/
int CalcLocalCurLoad(int submitTime, Job *jobs, int jID)
{
	long int tUsableCores = 0, usableCoresInUse = 0;
	int i, numServTypes, *coreCnts, load;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	int curJobCores = jobs[jID].resReq.cores;
	
	numServTypes = g_systemInfo.numServTypes;
	coreCnts = (int *)malloc(sizeof(int) * numServTypes);

	for (i = 0; i < numServTypes; i++)
		coreCnts[i] = sTypes[i].capacity.cores;

	qsort(coreCnts, numServTypes, sizeof(int), CmpCoreCnt);	
	
	for (i = 0; i < numServTypes && coreCnts[i] < curJobCores; i++);
	
	assert(i < numServTypes);
	
	for (; i < numServTypes; i++)
		tUsableCores += coreCnts[i] * sTypes[i].limit;
	
	for (i = 0; i < jID; i++) {
		Job *job = &jobs[i];
		int j;
		int completionTime = job->submitTime + job->actRunTime;
		
		if (completionTime <= submitTime) continue;	// skip

		// conservative counting as jobs can't be run across more than one servers, 
		// i.e., a job requiring 5 cores cannot use a 2-core server and 3 cores of a 4-core server; 
		// rather, it is run on a server with 5 cores or more
		for (j = 0; j < numServTypes && coreCnts[j] < job->resReq.cores; j++);
		if (coreCnts[j] >= curJobCores)
			usableCoresInUse += coreCnts[j];
	}
	
	free(coreCnts);
	
	load = usableCoresInUse / (float)tUsableCores * 100;
	
	return load;
}


inline int GetNextLoadDuration(enum LoadState lState)
{
	int lDuration = 0;
	
	switch (lState) {
		case LS_Low:
			lDuration = g_workloadInfo.avgLowTime;
			break;
		case LS_High:
			lDuration = g_workloadInfo.avgHighTime;
			break;
		default:
			lDuration = g_workloadInfo.avgTransitTime;
			break;
	}
	
	return lDuration;
}

inline int GetJobType()
{
	int i, r, rMin, rMax, jType;
	JobTypeProp *jobTypes = g_workloadInfo.jobTypes;

	r = rand() % 100;	// out of 100%
	for (i = 0, jType = UNKNOWN, rMin = 0, rMax = jobTypes[i].rate; 
		i < g_workloadInfo.numJobTypes && jType == UNKNOWN; 
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
	unsigned int maxJobCnt = limits[JCnt_Limit].max;
	int ret;

	adoc = xmlReadFile(jobFileName, NULL, 0);
	assert(adoc);
	rootNode = xmlDocGetRootElement(adoc);

	assert(!g_workloadInfo.jobs);
	g_workloadInfo.jobs = (Job *)calloc(maxJobCnt, sizeof(Job));
	ret = LoadJobs(rootNode);

	if (ret && g_workloadInfo.numJobs < maxJobCnt)
		g_workloadInfo.jobs = (Job *)realloc(g_workloadInfo.jobs, sizeof(Job) * g_workloadInfo.numJobs);

	xmlFreeDoc(adoc);

	// if jobs file is specified, termination conditions are (re)set by jobs in the file
	// if termination conditions are specified, they're ignored
	g_sConfig.termination.simEndTime = limits[SEnd_Limit].max;
	g_sConfig.termination.maxJobCnt = g_workloadInfo.numJobs;

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
				Job *job = &g_workloadInfo.jobs[jCnt];
				
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
	
	g_workloadInfo.numJobs = jCnt;
		
	return ret;
}

int FindPrevSmallerCoreCnt(int curCoreCnt, int startSTInx)
{
	int i, cores;
	int nsTypes = g_systemInfo.numServTypes;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	
	for (i = startSTInx; i >= 0 && sTypes[i].capacity.cores >= curCoreCnt; i--);
	// i < 0 means same core count with all previous server types of current server type w/ curCoreCnt
	
	return (i < 0 ? 0 : sTypes[i].capacity.cores);
}

int ComputeJobCoreReq(ServerRes *maxCapa, int loadTooLow)
{
	int i, r, rMax, minCores, cores;
	int nsTypes = g_systemInfo.numServTypes;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	int *jobGr = g_workloadInfo.jobGr;

	if (loadTooLow || nsTypes == 1) {
		cores = (maxCapa->cores / 2) + rand() % (maxCapa->cores / 2) + 1;
	}
	else // two or more server types
	{
		r = rand() % 100;	// out of 100%
		for (i = 0, rMax = jobGr[i]; i < nsTypes-1 && r >= rMax; i++, rMax += jobGr[i]);
		
		if (i == 0) 
			minCores = 0;
		else
		if (i < nsTypes-1)
			//minCores = sTypes[i-1].capacity.cores;
			minCores = FindPrevSmallerCoreCnt(sTypes[i].capacity.cores, i-1);
		else	// i == nsTypes-1, i.e., last server type
			//minCores = sTypes[i-2].capacity.cores; // remember >= 2 server types
			minCores = FindPrevSmallerCoreCnt(sTypes[i].capacity.cores, i-2);

		cores = minCores + rand() % (sTypes[i].capacity.cores - minCores) + 1;
	}
	
	return cores;
}


void GenerateJobResReq(Job *job, ServerRes *maxCapa, int curLoad, int targetLoad, int loadTooLow)
{
	ServerRes *resReq = &job->resReq;
	//int actRunTime = job->actRunTime;
	//int estError;

	resReq->cores = ComputeJobCoreReq(maxCapa, loadTooLow);
	/*
	// #cores is determined by primarily the target load range (minLoad and maxLoad)
	if (curLoad < targetLoad) 	// when load is below the min load, set #cores at least the half of max #cores
		resReq->cores = rand() % maxCapa->cores + 1;
	else {	// otherwise #cores is relative to job type (i.e., runtime)
		resReq->cores = MIN(MAX(1, ROUND((float)MIN(HOUR_IN_SECONDS, actRunTime) / (MIN_IN_SECONDS * 10))), maxCapa->cores);
		estError = rand() % (resReq->cores * 2);
		resReq->cores = rand() % 2 == 0 ? MAX(1, resReq->cores - estError) : MIN(resReq->cores + estError, maxCapa->cores);
	}
	*/
	//resReq->cores = rand() % maxCapa->cores + 1;
	resReq->mem = (MIN_MEM_PER_JOB_CORE + rand() % ((1 + resReq->cores / 10) * MIN_MEM_PER_CORE)) * resReq->cores;
	resReq->mem -= resReq->mem % 100; 	// truncate tens and ones to make it in hundreds
	resReq->mem = MIN(resReq->mem, maxCapa->mem);
	resReq->disk = (MIN_DISK_PER_JOB_CORE + rand() % ((1 + resReq->cores / 10) * MIN_DISK_PER_CORE)) * resReq->cores;
	resReq->disk -= resReq->disk % 100; 	// truncate tens and ones to make it in hundreds
	resReq->disk = MIN(resReq->disk, maxCapa->disk);
}

void GenerateJobRuntimes(Job *job, int jType, JobTypeProp *jobTypes)
{
	int estRunTime, actRunTime, estError;
	
	actRunTime = jobTypes[jType].min + rand() % (MAX(1, jobTypes[jType].max - jobTypes[jType].min));
	job->actRunTime = actRunTime;
	estError = rand() % actRunTime;
	estRunTime = rand() % 2 == 0 ? actRunTime - estError : BOUND_INT_MAX(actRunTime + estError);
	job->estRunTime = estRunTime;
}

void CalcJobGranu()
{
	int i, j, nsTypes = g_systemInfo.numServTypes;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	int totalCapaSCnt = 0;
	int *jobGr = g_workloadInfo.jobGr = (int *)malloc(sizeof(int) * nsTypes);
	
	for (i = 0; i < nsTypes; i++) {
		int capaSCnt = 0;
		
		for (j = 0; j < nsTypes; j++) 
			capaSCnt += sTypes[j].capacity.cores >= sTypes[i].capacity.cores ? sTypes[j].limit : 0;

		totalCapaSCnt += capaSCnt;
		jobGr[i] = capaSCnt;
	}
	
	for (i = 0; i < nsTypes; i++)
		jobGr[i] = ROUND(jobGr[i] / (float)totalCapaSCnt * 100);
}

Job *GenerateJobs()
{
	unsigned int simEndTime = g_sConfig.termination.simEndTime;
	unsigned int maxJobCnt = g_sConfig.termination.maxJobCnt;
	JobTypeProp *jobTypes;
	ServerRes *maxCapa = &g_systemInfo.sTypes[g_systemInfo.maxServType].capacity;
	int jID, jType;
	long int coresInUse, totalCores;
	enum LoadState lState = LS_Low;
	// lcLoad is load per server type (local), gcLoad is load per system
	int lcLoad, gcLoad = 0, targetLoad, curLoadSTime, curLoadETime;
	int submitTime, submitInterval;
	int lDir = FORWARD;
	int lTimeOffset = 5;	// 5 seconds by default
	int minMet = FALSE;
	const int loadWindow = 10;	
	const int loadDiffThreshold = 2;
	int loadTooLow = FALSE;
	int loadCnt = 0;
	int prevLoad = gcLoad;

	jobTypes = g_workloadInfo.jobTypes;

	Job *jobs = (Job *)calloc(maxJobCnt, sizeof(Job));	// allocate sufficient for max job count
	
	submitInterval = MIN_IN_SECONDS;	// 1 min by default
	submitTime = 0;
	curLoadSTime = 0;
	curLoadETime = g_workloadInfo.avgLowTime;
	targetLoad = g_workloadInfo.minLoad;
	totalCores = CalcTotalCoreCount();
	CalcJobGranu();

	for (jID = 0; jID < maxJobCnt && submitTime < simEndTime; jID++) {
		Job *job = &jobs[jID];
		
		job->id = jID;

		jType = GetJobType();
		job->type = jType;

		// runtimes and resource requirements need to be generated 
		// before the submission time to ensure the appropriate load level 
		// considering #cores of job and #cores w.r.t. server types
		GenerateJobRuntimes(job, jType, jobTypes);
		GenerateJobResReq(job, maxCapa, gcLoad, targetLoad, loadTooLow);

		do {
			// when load is too low due primarily to too many short jobs, 
			// use the same submission time and res (CPU) requirement is higher than usual
			if (loadTooLow) submitInterval = 1;
			submitTime += rand() % submitInterval;	
			// transiting to the next load state
			if (submitTime > curLoadETime && ((lDir > 0 && gcLoad >= targetLoad) || (lDir < 0 && gcLoad <= targetLoad))) {
				curLoadSTime = curLoadETime;
				if (lState + lDir < LS_Low || lState + lDir >= END_LOAD_STATE)
					lDir *= -1;	// flip the sign
				lState += lDir;
				targetLoad = targetLoad + g_workloadInfo.loadOffset * lDir;
				curLoadETime = curLoadSTime + GetNextLoadDuration(lState);
			}

			lcLoad = CalcLocalCurLoad(submitTime, jobs, jID);
			// if all servers with sufficient cores are occupied, reduce job's core requirement by 1
			// This is to avoid all large servers being overloaded and smaller servers being 'underloaded'
			if (lcLoad >= 100 && job->resReq.cores > 1)
				job->resReq.cores--;
			
			coresInUse = CalcTotalCoreUsage(submitTime, jobs, jID);
			gcLoad = coresInUse / (float)totalCores * 100;
			if (gcLoad >= targetLoad)
				loadTooLow = FALSE;
			//fprintf(stderr, "Current load (target: %d, loadState: %d, gcLoad: %d\n", targetLoad, lState, gcLoad);
			submitInterval += gcLoad < targetLoad ? -lTimeOffset : lTimeOffset;
			
			if (submitInterval < lTimeOffset)
				submitInterval = lTimeOffset;
			else
			if (submitInterval > limits[WorkloadTime_Limit].max)
				submitInterval = limits[WorkloadTime_Limit].max;
		} while (minMet && (gcLoad > g_workloadInfo.maxLoad || lcLoad >= 100));

		if (!minMet && gcLoad >= g_workloadInfo.minLoad)
			minMet = TRUE;
		
		job->submitTime = submitTime;

		loadCnt++;
		
		// if overall load is below target load for 10 consecutive jobs and 
		// the load before 10 jobs (prevLoad) is within loadDiffThreshold (2%),
		// set loadTooLow to TRUE, so that many jobs with the same submission time get generated
		// to reach the target load
		if (gcLoad < targetLoad && loadCnt == loadWindow) {
			loadTooLow = abs(gcLoad - prevLoad) <= loadDiffThreshold ? TRUE : FALSE;
			prevLoad = gcLoad;
			loadCnt = 0;
		}
		else 
		if (gcLoad >= targetLoad) {
			prevLoad = gcLoad;
			loadCnt = 0;
			loadTooLow = FALSE;
		}
	}

	if (jID < maxJobCnt)	// simulation end time criteron met first
		jobs = (Job *)realloc(jobs, sizeof(Job) * jID);
	
	g_workloadInfo.numJobs = jID;

	return jobs;
}


void WriteJobs()
{
	FILE *f;
	int i;

	f = fopen(JOB_FILENAME, "w");
	assert(f);
	
	fprintf(f, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
	fprintf(f, "<!-- job list for ds-sim, %s -->\n", VERSION);
	fprintf(f, "<jobs>\n");
	
	for (i = 0; i < g_workloadInfo.numJobTypes; i++) {
		JobTypeProp *jType = &g_workloadInfo.jobTypes[i];
		fprintf(f, "\t<type name=\"%s\" minRunTime=\"%d\" maxRunTime=\"%d\" populationRate=\"%d\" />\n", 
				jType->name, jType->min, jType->max, jType->rate);
	}
	for (i = 0; i < g_workloadInfo.numJobs; i++) {
		Job *job = &g_workloadInfo.jobs[i];
		ServerRes *resReq = &job->resReq;
#ifdef JOB		
		fprintf(f, "\t<job id=\"%d\" type=\"%s\" submitTime=\"%d\" estRunTime=\"%d\" actRunTime=\"%d\" \
cores=\"%d\" memory=\"%d\" disk=\"%d\" />\n", 
			job->id, g_workloadInfo.jobTypes[job->type].name, job->submitTime, 
			job->estRunTime, job->actRunTime, resReq->cores, resReq->mem, resReq->disk);
#else
		fprintf(f, "\t<job id=\"%d\" type=\"%s\" submitTime=\"%d\" estRunTime=\"%d\" \
cores=\"%d\" memory=\"%d\" disk=\"%d\" />\n", 
			job->id, g_workloadInfo.jobTypes[job->type].name, job->submitTime, 
			job->estRunTime, resReq->cores, resReq->mem, resReq->disk);
#endif
	}
	fprintf(f, "</jobs>\n");
	fclose(f);
}

inline int GetNumLoadTransitions()
{
	return LS_High - LS_Low;
}

void GenerateWorkload()
{
	int i;

	g_workloadInfo.name = strdup("alternating");	// by default
	g_workloadInfo.minLoad = limits[WMinLoad_Limit].min + rand() % limits[WMinLoad_Limit].max;
	g_workloadInfo.maxLoad = g_workloadInfo.minLoad + rand() % (limits[WMaxLoad_Limit].max - g_workloadInfo.minLoad + 1);
	g_workloadInfo.loadOffset = (g_workloadInfo.maxLoad - g_workloadInfo.minLoad) / GetNumLoadTransitions();
	g_workloadInfo.avgLowTime = limits[WorkloadTime_Limit].min + rand() % limits[WorkloadTime_Limit].max;
	g_workloadInfo.avgHighTime = limits[WorkloadTime_Limit].min + rand() % limits[WorkloadTime_Limit].max;
	g_workloadInfo.avgTransitTime = g_workloadInfo.avgLowTime < g_workloadInfo.avgHighTime ?
								g_workloadInfo.avgLowTime : g_workloadInfo.avgHighTime;
	g_workloadInfo.numJobTypes = END_JOB_TYPE;
	g_workloadInfo.jobTypes = (JobTypeProp *)malloc(sizeof(JobTypeProp) * g_workloadInfo.numJobTypes);
	for (i = 0; i < g_workloadInfo.numJobTypes; i++) {
		JobTypeProp *jType = &g_workloadInfo.jobTypes[i];
		jType->type = i;
		jType->name = strdup(defaultJobTypes[i].name);
		jType->min = defaultJobTypes[i].min;
		jType->max = defaultJobTypes[i].max;
		jType->rate = defaultJobTypes[i].rate;
	}

	g_workloadInfo.jobs = GenerateJobs();
}

void WriteResFailures()
{
	FILE *f;
	int i, totalFailures;
	char configFName[MAX_NAME_LENGTH];

	if (g_sConfig.configFile)
		strcpy(configFName, g_sConfig.configFilename);
	else
		sprintf(configFName, "random (internal)");

	f = fopen(RES_FAILURE_FILENAME, "w");
	assert(f);

	fprintf(f, "#base_trace: %s, config_file: %s, total_servers: %d, total_time: %d, \
time_dist_mean: %f, time_dist_stdev: %f, node_dist_mean: %f, node_dist_stdev: %f\n",
				resFailureModels[g_systemInfo.rFailT->fModel].model, configFName,
				g_systemInfo.totalNumServers, g_sConfig.termination.simEndTime, 
				resFailureModels[g_systemInfo.rFailT->fModel].tParam.mean,
				resFailureModels[g_systemInfo.rFailT->fModel].tParam.stdev,
				resFailureModels[g_systemInfo.rFailT->fModel].nParam.mean,
				resFailureModels[g_systemInfo.rFailT->fModel].nParam.stdev);

	for (i = 0, totalFailures = g_systemInfo.rFailT->numFailues; i < totalFailures; i++) {
		ResFailure *resFailure = &g_systemInfo.rFailT->resFailures[i];

		// print only up to the failure that starts before the actual simulation end time
		if (resFailure->startTime >= g_ss.actSimEndTime) break;
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
	for (i = 0; i < g_systemInfo.numServTypes; i++) {
		ServerTypeProp *sType = &g_systemInfo.sTypes[i];
		ServerRes *capacity = &sType->capacity;
		int limit = sType->limit;
		
		if (IsOutOfBound(limit, limits[Res_Limit].min, limits[Res_Limit].max, limits[Res_Limit].name) ||
			IsOutOfBound(capacity->cores, limits[CCnt_Limit].min, limits[CCnt_Limit].max, limits[CCnt_Limit].name) ||
			IsOutOfBound(capacity->mem, limits[Mem_Limit].min, limits[Mem_Limit].max, limits[Mem_Limit].name) ||
			IsOutOfBound(capacity->disk, limits[Disk_Limit].min, limits[Disk_Limit].max, limits[Disk_Limit].name))
			return FALSE;
		// current resource requirement calculation for job is based on MIN_MEM_PER_JOB_CORE (100MB) * 2
		if (capacity->mem < capacity->cores * (MIN_MEM_PER_JOB_CORE * 2)) {
			int minMem = MIN_MEM_PER_JOB_CORE * 2;
			fprintf(stderr, "Insufficient memory (min/core: %d for server type: %s)!\n", minMem, sType->name);
			return FALSE;
		}
		else
		if (capacity->disk < capacity->cores * (MIN_DISK_PER_JOB_CORE * 2)) {
			int minDisk = MIN_DISK_PER_JOB_CORE * 2;
			fprintf(stderr, "Insufficient disk space (min/core: %d for server type: %s)!\n", minDisk, sType->name);
			return FALSE;
		}
		
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
	
	for (i = 0; i < g_workloadInfo.numJobTypes; i++) {
		JobTypeProp *jType = &g_workloadInfo.jobTypes[i];
		
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
	
	if (g_workloadInfo.minLoad > g_workloadInfo.maxLoad) {
		fprintf(stderr, "Min load (%d) should be less than max load (%d)!\n", 
				g_workloadInfo.minLoad, g_workloadInfo.maxLoad);
		return FALSE;
	}
	
	numTransitLoads = LS_High - LS_Low - 1;	// three transit load patterns by default
	g_workloadInfo.loadOffset = (g_workloadInfo.maxLoad - g_workloadInfo.minLoad) / numTransitLoads;
	if (g_workloadInfo.avgLowTime <= 0)
		g_workloadInfo.avgLowTime = limits[WorkloadTime_Limit].min + rand() % limits[WorkloadTime_Limit].max;
	if (g_workloadInfo.avgHighTime <= 0)
		g_workloadInfo.avgHighTime = limits[WorkloadTime_Limit].min + rand() % limits[WorkloadTime_Limit].max;
	if (g_workloadInfo.avgTransitTime <= 0)
		g_workloadInfo.avgTransitTime = g_workloadInfo.avgLowTime < g_workloadInfo.avgHighTime ?
								g_workloadInfo.avgLowTime : g_workloadInfo.avgHighTime;
								
	return TRUE;
}

int CompareCoreCnt(const void *p1, const void *p2)
{
	ServerTypeProp *left = (ServerTypeProp *)p1;
	ServerTypeProp *right = (ServerTypeProp *)p2;

	return (left->capacity.cores > right->capacity.cores);
}


void SortServersByResCapacity()
{
	int i;
	
	qsort(g_systemInfo.sTypes, g_systemInfo.numServTypes, sizeof(ServerTypeProp), CompareCoreCnt);
	for (i = 0; i < g_systemInfo.numServTypes; i++)	// reset server type IDs
		g_systemInfo.sTypes[i].type = i;
	g_systemInfo.maxServType = g_systemInfo.sTypes[g_systemInfo.numServTypes - 1].type;
}

void WriteSystemInfo()
{
	FILE *f;
	int i;
		
	f = fopen(SYS_INFO_FILENAME, "w");
	assert(f);
	
	fprintf(f, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
	fprintf(f, "<!-- system information generated for ds-sim@MQ, %s -->\n", VERSION);
	fprintf(f, "<system>\n");
	fprintf(f, "\t<servers>\n");
	for (i = 0; i < g_systemInfo.numServTypes; i++) {
		ServerTypeProp *sType = &g_systemInfo.sTypes[i];
		ServerRes *capacity = &sType->capacity;
	
		fprintf(f, "\t\t<server type=\"%s\" limit=\"%d\" bootupTime=\"%d\" hourlyRate=\"%.2f\" \
cores=\"%d\" memory=\"%d\" disk=\"%d\" />\n", sType->name, sType->limit, sType->bootupTime, \
			sType->rate, capacity->cores, capacity->mem, capacity->disk);
	}
	
	fprintf(f, "\t</servers>\n");
	fprintf(f, "</system>\n");
	
	fclose(f);
}

int HandleREDY(char *msgRcvd, char *msgToSend)
{
	static long int njFTime;	// finish time of next completing job; it has to be here as it doesn't get updated every time
	long int nJobSubmT;	// submission time of next job
	long int nFailET;	// next failure event time, either failure or recovery
	int noMoreJobs;
	char cmd[LARGE_BUF_SIZE] = "";
	char buffer[LARGE_BUF_SIZE] = ""; 
	Job *job;

	nFailET = TIME_MAX; // assume no times ever become or go beyond TIME_MAX (INT_MAX)

	if (!g_ncJobs)
		njFTime = GetEFTime();

	nJobSubmT = GetNextJobSubmitTime();

	if (nJobSubmT == UNKNOWN)	// no more jobs to be submitted
		nJobSubmT = TIME_MAX;
	
	if (g_systemInfo.rFailT) {
		nFailET = GetNextResFailEventTime();
		if (nFailET == UNKNOWN)
			nFailET = TIME_MAX;
	}

	noMoreJobs = njFTime == TIME_MAX && nJobSubmT == TIME_MAX;
		
	// "njFTime <= nFailET" below and "nFailET < njFTime" in 'else-if' indicate 
	// the precedence between job completion and resource failure;
	// in other words, if job completion and resource failure/recovery take place at the same time
	// job completion is processed first
	if (!noMoreJobs && njFTime != TIME_MAX && njFTime <= nFailET && njFTime <= nJobSubmT) {
		if (!g_ncJobs) {
			g_ncJobs = GetNextCmpltJobs(njFTime);
			if (g_ncJobs) {
				g_ss.curSimTime = njFTime;
				// run waiting jobs on booting servers, for which the start times are at or earlier than njFTime
				UpdateBootingServerStates();
			}
		}
		g_cSJob = g_ncJobs->sJob;
		// update the state of current completing job (g_ncJobs->sJob)
		CompleteJob(g_cSJob);
		strcpy(cmd, "JCPL");
		sprintf(buffer, "%s %d %d %s %d", cmd, g_cSJob->endTime, g_cSJob->job->id, FindResTypeNameByType(g_cSJob->sType), g_cSJob->sID);
		g_ncJobs = RemoveFJobFromList(g_ncJobs);
	}
	else // nFailET indicates if failures are simulated
	if (!noMoreJobs && nFailET != TIME_MAX && nFailET < njFTime && nFailET <= nJobSubmT) {
		int failure = CheckForFailures(nFailET);
		// now with sending a job completion message, the very next simulation event is a resource failure 
		// when the above if conditions are satisfied
		if (failure != UNDEFINED) {	// there is a failure
			ResFailure *resFail = &g_systemInfo.rFailT->resFailures[failure];
			
			if (resFail->eventType == RES_Failure)
				strcpy(cmd, "RESF");
			else
			if (resFail->eventType == RES_Recovery) 
				strcpy(cmd, "RESR");
			sprintf(buffer, "%s %s %hu %d", cmd, FindResTypeNameByType(resFail->servType), resFail->servID, resFail->startTime);
		}
		FreeSJList(g_ncJobs);
	}
	else {
		int submitTime;

		job = GetNextJobToSched(&submitTime);

		if (!job) {	// no more jobs to schedule and all jobs have completed
			assert(g_ss.numJobsCompleted == g_workloadInfo.numJobs);
			strcpy(buffer, "NONE");
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

			g_lastJobSent = job;

			UpdateServerStates();
		}
		FreeSJList(g_ncJobs);
		g_ncJobs = NULL;
	}
	
	strcpy(msgToSend, buffer);
			
	return TRUE;
}

int HandlePSHJ(char *msgRcvd, char *msgToSend)
{
	int ret = BackFillJob(g_lastJobSent);
	if (ret == UNDEFINED)
		sprintf(msgToSend, "Error: simulation can't continue at Job %d at %d\n", g_lastJobSent->id, g_ss.curSimTime);
	else {
		g_lastJobSent = NULL;
		strcpy(msgToSend, "OK");
	}

	return TRUE;
}


int HandleGETS(char *msgRcvd, char *msgToSend)
{
	int ret = SendResInfo(msgRcvd);
			
	if (ret == UNDEFINED) 
		sprintf(msgToSend, "ERR: invalid resource infomation query (%s)!", msgRcvd);
	else
		strcpy(msgToSend, END_DATA);

	return TRUE;
}

int HandleSCHD(char *msgRcvd, char *msgToSend)
{
	int jID, sID, status;
	char stName[MAX_NAME_LENGTH];
	char buffer[LARGE_BUF_SIZE] = ""; 
	
	sscanf(msgRcvd, "SCHD %d %s %d", &jID, stName, &sID);
	if ((status = IsSchdInvalid(jID, stName, sID)))
		sprintf(buffer, "%s", SchdStatusList[status].status);
	else {
		int sType = FindResTypeByName(stName);
		status = ScheduleJob(jID, sType, sID, NULL);
		sprintf(buffer, "%s", SchdStatusList[status].status);
		if (status == SCHD_Valid)	// job got scheduled successfully
			g_lastJobSent = NULL;
	}

	strcpy(msgToSend, buffer);

	return TRUE;
}

int HandleJCPL(char *msgRcvd, char *msgToSend)
{
	return 0;
}

int HandleLSTJ(char *msgRcvd, char *msgToSend)
{
	int ret = SendJobsOnServer(msgRcvd);
			
	if (ret == UNDEFINED)
		sprintf(msgToSend, "ERR: invalid job listing query (%s)!", msgRcvd);
	else
	if (ret != INTACT_QUIT)
		strcpy(msgToSend, END_DATA);

	return ret;
}

int HandleCNTJ(char *msgRcvd, char *msgToSend)
{
	int ret = SendJobCountOfServer(msgRcvd);
			
	if (ret == UNDEFINED)
		sprintf(msgToSend, "ERR: invalid job count query (%s)!", msgRcvd);
	else
		sprintf(msgToSend, "%d", ret);

	return TRUE;
}

int HandleMIGJ(char *msgRcvd, char *msgToSend)
{
	int ret = MigrateJob(msgRcvd);

	if (!ret)
		sprintf(msgToSend, "ERR: %s!", msgRcvd);
	else
		sprintf(msgToSend, "%s", SchdStatusList[ret-1].status);

	return TRUE;
}

int HandleEJWT(char *msgRcvd, char *msgToSend)
{
	int ret = SendEstWTOfServer(msgRcvd);
			
	if (ret == UNDEFINED)
		sprintf(msgToSend, "ERR: invalid estimated waiting time query (%s)!", msgRcvd);
	else
		sprintf(msgToSend, "%d", ret);

	return TRUE;
}

int HandleTERM(char *msgRcvd, char *msgToSend)
{
	int ret = TerminateServer(msgRcvd);
			
	if (ret == UNDEFINED) // either incorrect format or unavailable/failed server specified
		sprintf(msgToSend, "ERR: invalid server termination command (%s)!", msgRcvd);
	else
		sprintf(msgToSend, "%d jobs killed", ret);

	return TRUE;
}

int HandleKILJ(char *msgRcvd, char *msgToSend)
{
	int ret = KillJob(msgRcvd);
			
	if (ret == UNDEFINED)
		sprintf(msgToSend, "ERR: invalid job termination command (%s)!", msgRcvd);
	else
		sprintf(msgToSend, "OK");

	return TRUE;
}

inline Server *GetServer(int type, int id)
{
	return &g_systemInfo.servers[type][id];
}


int HandleKilledJobs(SchedJob *sJob, Server *server, int killedTime)
{
	return HandlePreemptedJobs(sJob, server, JS_Killed, killedTime);
}


int HandleFailedJobs(SchedJob *sJob, Server *server, int failedTime)
{
	return HandlePreemptedJobs(sJob, server, JS_Failed, failedTime);
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
	Server *server = GetServer(sJob->sType, sJob->sID);
	
	sJob->state = eventType;
	sJob->endTime = eventTime;
	if (simOpts[SO_Verbose].used != VERBOSE_STATS)
		printf("t: %10d job %5d on #%2d of server %s %s\n", 
			g_ss.curSimTime, sJob->job->id, server->id, g_systemInfo.sTypes[server->type].name, jobStates[eventType].state);
	AddToWaitingJobList(sJob->job, eventTime);
	UpdateServerUsage(sJob, server);
}


// find the start time of running job started earliest
inline int GetESTimeJob(SchedJob *sJob)
{
	int est = INT_MAX;
	
	if (!sJob) return UNKNOWN;
	for (; sJob; sJob = sJob->next)
		if (sJob->startTime < est)
			est = sJob->startTime;

	return est;
}


int CalcFailureMAD(unsigned short servType, unsigned short servID, ServerFailInfo *failInfo)
{
	int i, numFails, mttf, diffSum = 0;
	ResFailure *resFailures = g_systemInfo.rFailT->resFailures;
	
	mttf = failInfo->mttf;
	for (i = 0, numFails = failInfo->numFails; i < numFails; i++) {
		int fInx = failInfo->fIndices[i];
		
		assert(resFailures[fInx].servType == servType && resFailures[fInx].servID == servID);
		diffSum += abs((resFailures[fInx].endTime - resFailures[fInx].startTime) - mttf);
	}
	
	return (diffSum / numFails);
}

inline void InitServerRes(ServerRes *sRes)
{
	sRes->cores = sRes->mem = sRes->disk = 0;
}

void FailRes(ResFailure *resFail, int fInx)
{
	unsigned short servType = resFail->servType;
	unsigned short servID = resFail->servID;
	int	failStartTime = resFail->startTime;
	int totalOpTime, numFJobs = 0;
	Server *server = GetServer(servType, servID);
			
	// deduct time spent for jobs running at the time of failure to particularly exclude it for cost calculation
	// in the case of booting, no elapseed time is accounted for server usage
	if (server->state == SS_Idle || server->state == SS_Active) {
//		int est = GetESTimeJob(server->running);
//		int deduction = est == UNKNOWN ? 0 : failStartTime - est;
//		server->totalUsage += failStartTime - server->startTime - deduction;
		server->totalUsage += failStartTime - server->startTime;
	}

	numFJobs = HandleFailedJobs(server->running, server, failStartTime);
	server->running = NULL;
	
	numFJobs += HandleFailedJobs(server->waiting, server, failStartTime);
	server->waiting = NULL;

	// job suspension is not supported at this stage!!!
	numFJobs += HandleFailedJobs(server->suspended, server, failStartTime);
	server->suspended = NULL;	

	server->state = SS_Unavailable;
	server->startTime = UNKNOWN;
	InitServerRes(&server->availCapa);

	server->failInfo.fIndices[server->failInfo.numFails] = fInx;
	server->failInfo.numFails++;
	totalOpTime = failStartTime - server->failInfo.totalFailTime;
	server->failInfo.mttf = totalOpTime / server->failInfo.numFails;
	server->failInfo.madf = CalcFailureMAD(servType, servID, &server->failInfo);	
}


//void RecoverRes(unsigned short servType, unsigned short servID, int recoverTime)
void RecoverRes(ResFailure *resRecover, int rInx)
{
	unsigned short servType = resRecover->servType;
	unsigned short servID = resRecover->servID;
	Server *server = GetServer(servType, servID);

	SetServerToInactive(server);
	server->failInfo.lastOpTime = resRecover->startTime;
	
	server->failInfo.rIndices[server->failInfo.numFails] = rInx;
	// resRecover->endTime has the start time of failure;
	// -1 for resRecover->startTime is 1 later than failure end time
	server->failInfo.totalFailTime += (resRecover->startTime - resRecover->endTime - 1);
	server->failInfo.mttr = server->failInfo.totalFailTime / (float) server->failInfo.numFails;	
}


// GetNextResFailEventTime returns the start time of either resource failure or resource recovery
inline int GetNextResFailEventTime()
{	
	if (g_systemInfo.rFailT->nextFailureInx < g_systemInfo.rFailT->numFailues)
		return (g_systemInfo.rFailT->resFailures[g_systemInfo.rFailT->nextFailureInx].startTime);
	else
		return UNKNOWN;
}

// check any resource failures before the next job submission;
// if found, get jobs running on the failed server(s) and 
// put them in waiting job queue for rescheduling
int CheckForFailures(int nextFET)
{
	ResFailure *resFail, *resFailures = g_systemInfo.rFailT->resFailures;
	int fCnt = g_systemInfo.rFailT->numFailues;
	int nextFInx = g_systemInfo.rFailT->nextFailureInx;

	if (nextFET == UNKNOWN)
		// g_ss.curJobID is a global variable to keep track of the next job to be submitted
		nextFET = g_ss.waitJQ ? g_ss.waitJQ->submitTime : g_workloadInfo.jobs[g_ss.curJobID].submitTime;	

	if (nextFInx >= fCnt) return UNDEFINED;
	resFail = &resFailures[nextFInx];

	// there is at least one job to be submitted before the next resource failure event
	if (nextFET < resFail->startTime) return UNDEFINED;
	
	g_ss.curSimTime = resFail->startTime;
	UpdateServerStates();
	
	if (resFail->eventType == RES_Failure) 
		FailRes(resFail, nextFInx);
	else 
	if (resFail->eventType == RES_Recovery) 
		RecoverRes(resFail, nextFInx);

	nextFInx++;
	g_systemInfo.rFailT->nextFailureInx = nextFInx;
	
	return (nextFInx - 1);
}


int FreeWaitingJob(int jID)
{
	WaitingJob *prev, *cur;
	
	for (prev = NULL, cur = g_ss.waitJQ; cur && cur->job->id != jID; prev = cur, cur = cur->next);
	if (!cur)	// not found
		return FALSE;
	else {
		if (!prev)
			g_ss.waitJQ = cur->next;
		else
			prev->next = cur->next;
		free(cur);
	}
	
	return TRUE;
}


WaitingJob *DisconnectWaitingJob(int jID)
{
	WaitingJob *prev, *cur;
	
	for (prev = NULL, cur = g_ss.waitJQ; cur && cur->job->id != jID; prev = cur, cur = cur->next);
	if (cur) {
		if (!prev)
			g_ss.waitJQ = cur->next;
		else
			prev->next = cur->next;
		cur->next = NULL;
	}
	
	return cur;
}


WaitingJob *GetWaitingJob(int jID)
{
	WaitingJob *wJob;
	
	for (wJob = g_ss.waitJQ; wJob && wJob->job->id != jID; wJob = wJob->next);
	
	return wJob;
}

// jobs are placed in the order of their submission; hence, the first job to select
WaitingJob *GetFirstWaitingJob()
{
	WaitingJob *wJob = g_ss.waitJQ;
		
	return wJob;
}


void InsertWaitingJob(WaitingJob *wJob)
{
	WaitingJob *prev, *cur;
	
	if (!g_ss.waitJQ)
		g_ss.waitJQ = wJob;
	else {
		for (prev = NULL, cur = g_ss.waitJQ; cur && cur->submitTime < wJob->submitTime; prev = cur, cur = cur->next);
		if (!prev) {
			wJob->next = cur;
			g_ss.waitJQ = wJob;
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
	if (!g_ss.waitJQ)
		g_ss.waitJQ = wJob;
	else {
		for (cur = g_ss.waitJQ, next = cur->next; next; cur = next, next = next->next);
		cur->next = wJob;
	}
}


int GetNextJobSubmitTime()
{
	WaitingJob *wJob;
	int submitTime = UNKNOWN;
	
	if (g_ss.waitJQ) {
		wJob = GetFirstWaitingJob();
		submitTime = wJob->submitTime;
	}
	else
	if (g_ss.curJobID < g_workloadInfo.numJobs)
		submitTime = g_workloadInfo.jobs[g_ss.curJobID].submitTime;

	return submitTime;
}


Job *GetNextJobToSched(int *submitTime)
{
	WaitingJob *wJob;
	Job *job;
	
	*submitTime = UNKNOWN;
	
	if (g_ss.waitJQ) {
		wJob = GetFirstWaitingJob();
		// if this is a failed job, the submitTime will be the time of failure
		*submitTime = 
		g_ss.curSimTime = wJob->submitTime;
		job = wJob->job;
		return job;
	}

	if (g_ss.curJobID >= g_workloadInfo.numJobs)
		return NULL;
	
	job = &g_workloadInfo.jobs[g_ss.curJobID];
	AddToWaitingJobList(job, job->submitTime);
	*submitTime = 
	g_ss.curSimTime = job->submitTime;
	g_ss.curJobID++;
	
	return job;
}

SchedJob *DisconnectSchedJob(SchedJob *sJob, Server *server)
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


int UpdateServerUsage(SchedJob *sJob, Server *server)
{
	int usage = 0, lat = server->lastAccTime, est;
	SchedJob *rSJob;
	
	est = sJob->startTime > lat ? sJob->startTime : INT_MAX;
	
	if (est != INT_MAX)
	// find the running job for which the start time is earliest, but after lastAccTime
		for (rSJob = server->running; rSJob; rSJob = rSJob->next) {
			int st = rSJob->startTime;
			//if (st >= lat && st < est)
			if (st < est)
				est = st;
		}

	if (est != INT_MAX)
		lat =  est > lat ? est : lat;

	usage = sJob->endTime - lat;

	server->lastAccTime = sJob->endTime;
	server->actUsage += usage;
	
	return usage;
}


void MoveSchedJobState(SchedJob *sJob, int jStateNew, Server *server)
{
	SchedJob **sJobList;

	DisconnectSchedJob(sJob, server);
	sJobList = GetSchedJobList(jStateNew, server);
	sJob->state = jStateNew;
	if (jStateNew == JS_Running) {
		InsertSJobByEndTime(sJob, sJobList);
		sprintf(g_schedOps[g_logCnt].msg, "t: %10d job %5d on #%2d of server %s RUNNING\n", 
			sJob->startTime, sJob->job->id, server->id, g_systemInfo.sTypes[server->type].name);
#ifdef DEBUG
	fprintf(stdout, "JOB: %d -- Finish Time: %d\n", sJob->job->id, sJob->endTime);
#endif
			
		g_schedOps[g_logCnt].time = sJob->startTime;
	}
	if (jStateNew == JS_Completed) {
		AddSchedJobToEnd(sJob, sJobList);
		sprintf(g_schedOps[g_logCnt].msg, "t: %10d job %5d on #%2d of server %s COMPLETED\n", 
			sJob->endTime, sJob->job->id, server->id, g_systemInfo.sTypes[server->type].name);
		g_schedOps[g_logCnt].time = sJob->endTime;
		if (sJob->endTime > g_ss.actSimEndTime)	// keep track of the actual simulation end time
			g_ss.actSimEndTime = sJob->endTime;
		UpdateServerUsage(sJob, server);
		g_ss.numJobsCompleted++;
	}

	g_logCnt++;
	if (g_logCnt >= g_logSize) {
		g_logSize += DEFAULT_LOG_CNT;
		g_schedOps = realloc(g_schedOps, sizeof(Log) * g_logSize);
	}
}

void UpdateServerRes(ServerRes *sRes1, ServerRes *sRes2, int op)
{
	sRes1->cores += sRes2->cores * op;
	assert(sRes1->cores >= 0);
	sRes1->mem += sRes2->mem * op;
	assert(sRes1->mem >= 0);
	sRes1->disk += sRes2->disk * op;
	assert(sRes1->disk >= 0);
}

void UpdateServerCapacity(Server *server, ServerRes *resReq, int op)
{
	UpdateServerRes(&server->availCapa, resReq, op);
}

/*
* SetSJobTimes is primarily for waiting jobs that are about to run
*/
inline void SetSJobTimes(SchedJob *sJob, int startTime)
{
	sJob->startTime = startTime;
	sJob->endTime = startTime + sJob->job->actRunTime;
}

// currently, only the first job in the queue is checked; i.e., no backfilling is supported
SchedJob *GetNextJobToRun(Server *server, int prevJobET)
{
	SchedJob *wSJob;

	if ((wSJob = server->waiting) && IsSuffAvailRes(&wSJob->job->resReq, &server->availCapa))
		SetSJobTimes(wSJob, prevJobET);
	else 
		wSJob = NULL;
	
	return wSJob;
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
	
	if (g_logCnt <= 0) return 0;
	qsort(g_schedOps, g_logCnt, sizeof(Log), CmpLog);
	for (i = 0; i < g_logCnt; i++)
		if (simOpts[SO_Verbose].used != VERBOSE_STATS)
			printf("%s", g_schedOps[i].msg);
	
	return g_schedOps[g_logCnt - 1].time;
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

// Get the earliest finish time among all incomplete jobs
long int GetEFTime()
{
	int i, nsTypes = g_systemInfo.numServTypes;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	Server **servers = g_systemInfo.servers;
	long int eft = TIME_MAX;
	
	for (i = 0; i < nsTypes; i++) {
		int j, limit = sTypes[i].limit;

		for (j = 0; j < limit; j++) {
			Server *server = &servers[i][j];
			SchedJob *sJob;
			
			for (sJob = server->running; sJob; sJob = sJob->next) {
				if (sJob->endTime < eft)
					eft = sJob->endTime;
			}
			// check waiting jobs for booting servers; 
			// only jobs that can run immediately after the completion of booting have the valid end time
			for (sJob = server->waiting; sJob; sJob = sJob->next) {
				if (sJob->endTime != UNKNOWN && sJob->endTime < eft)
					eft = sJob->endTime;
			}
		}
	}
	
	return eft;
}

SchedJobListNode *CreateSJLNode(SchedJob *sJob)
{
	SchedJobListNode *sjlNode = (SchedJobListNode *)malloc(sizeof(SchedJobListNode));
	sjlNode->sJob = sJob;
	sjlNode->next = NULL;
	
	return sjlNode;
}


SchedJobListNode *AddSJobToList(SchedJob *sJob, SchedJobListNode *head)
{
	SchedJobListNode *last;
	
	assert(sJob);
	
	if (!head) 
		head = CreateSJLNode(sJob);
	else {
		for (last = head; last->next; last = last->next);
		last->next = CreateSJLNode(sJob);
	}
	
	return head;
}

// remove the first scheduled job from the list
SchedJobListNode *RemoveFJobFromList(SchedJobListNode *head)
{	
	if (head) {
		SchedJobListNode *oldHead = head;
		head = head->next;
		free(oldHead);
	}
	
	return head;
}

void FreeSJList(SchedJobListNode *head)
{
	SchedJobListNode *cur, *next;

	for (cur = head; cur; cur = next) {
		next = cur->next;
		free(cur);
	}
}


// get the list of next completing jobs of the same completion time
SchedJobListNode *GetNextCmpltJobs(int eft)
{
	int i, nsTypes = g_systemInfo.numServTypes;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	Server **servers = g_systemInfo.servers;
	SchedJobListNode *ncJobs = NULL; 
	
	if (eft != TIME_MAX) { // TIME_MAX means no jobs to process
		for (i = 0; i < nsTypes; i++) {
			int j, limit = sTypes[i].limit;

			for (j = 0; j < limit; j++) {
				Server *server = &servers[i][j];
				SchedJob *sJob;
				
				for (sJob = server->running; sJob; sJob = sJob->next) {
					if (sJob->endTime == eft)
						ncJobs = AddSJobToList(sJob, ncJobs);
				}
				// check waiting jobs for booting servers; 
				// only jobs that can run immediately after the completion of booting have the valid end time
				for (sJob = server->waiting; sJob; sJob = sJob->next) {
					if (sJob->endTime == eft)
						ncJobs = AddSJobToList(sJob, ncJobs);
				}
			}
		}
	}
	
	return ncJobs;
}

int UpdateBootingServerState(Server *server)
{
	SchedJob *sJob, *nsJob;
	int nRJobs = 0;
	
	// there must be some waiting job(s) that has run by this time
	for (sJob = server->waiting; sJob && sJob->startTime != UNKNOWN; sJob = nsJob) {
		nsJob = sJob->next;
		if (sJob->startTime <= g_ss.curSimTime) {
			MoveSchedJobState(sJob, JS_Running, server);
			server->state = SS_Active;
			nRJobs++;
		}
	}
	
	return nRJobs;
}

int UpdateBootingServerStates()
{
	int i, nsTypes = g_systemInfo.numServTypes;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	int nUpdatedServers = 0;
	
	g_logCnt = 0;
	g_logSize = DEFAULT_LOG_CNT;
	g_schedOps = (Log *)malloc(sizeof(Log) * g_logSize);
	
	for (i = 0; i < nsTypes; i++) {
		int j, limit = sTypes[i].limit;

		for (j = 0; j < limit; j++) {
			Server *server = GetServer(i, j);
			
			if (server->startTime <= g_ss.curSimTime && server->state == SS_Booting) { // should become Active
				int nRJobs = UpdateBootingServerState(server);
				// there must be one or more jobs to run at g_ss.curSimTime since server gets booted (activated) by assigning a job
				assert(nRJobs);
				nUpdatedServers++;
			}
		}
	}
	
	PrintLog();
	free(g_schedOps);
	
	return nUpdatedServers;
}


void CompleteJob(SchedJob *sJob)
{	
	Server *server = GetServer(sJob->sType, sJob->sID);
	
	assert(sJob->endTime <= g_ss.curSimTime);
	
	g_logCnt = 0;
	g_logSize = 1;
	g_schedOps = (Log *)malloc(sizeof(Log));	// only one job is to complete
	MoveSchedJobState(sJob, JS_Completed, server);
	UpdateServerCapacity(server, &sJob->job->resReq, INCREASE);

	PrintLog();
	free(g_schedOps);
}


int RunReadyJobs(Server *server)
{
	SchedJob *wSJob;
	int nPJobs = 0;
	
	g_logCnt = 0;
	g_logSize = 1;
	g_schedOps = (Log *)malloc(sizeof(Log));	// only one job is to complete

	while ((wSJob = GetNextJobToRun(server, g_ss.curSimTime))) {
		MoveSchedJobState(wSJob, JS_Running, server);
		UpdateServerCapacity(server, &wSJob->job->resReq, DECREASE);
		nPJobs++;	// keep track of #processed jobs
	}

	PrintLog();
	free(g_schedOps);
	
	return nPJobs;
}


SchedJob *CopySchedJob(SchedJob *srcSJob)
{
	SchedJob *tgtSJob = (SchedJob *)malloc(sizeof(SchedJob));
	
	assert(tgtSJob);
	
	tgtSJob->sType = srcSJob->sType;
	tgtSJob->sID = srcSJob->sID;
	tgtSJob->startTime = srcSJob->startTime;
	tgtSJob->endTime = srcSJob->endTime;
	tgtSJob->state = srcSJob->state;
	tgtSJob->xState = srcSJob->xState;
	tgtSJob->job = srcSJob->job;
	tgtSJob->next = NULL;
	
	return tgtSJob;
}

// MIGJ jID (on) src_server_type src_server_id (to) tgt_server_type tgt_server_id
int MigrateJob(char *msg)
{
	char orgMsg[LARGE_BUF_SIZE], cmd[MAX_NAME_LENGTH], srcSTypeName[MAX_NAME_LENGTH], tgtSTypeName[MAX_NAME_LENGTH];
	int ret, numFields, jID, srcSType, srcSID, tgtSType, tgtSID;
	Server *srcServer;
	SchedJob *sJob, *dupSJob;
	
	numFields = sscanf(msg, "%s %d %s %d %s %d", cmd, &jID, srcSTypeName, &srcSID, tgtSTypeName, &tgtSID);
	srcSType = FindResTypeByName(srcSTypeName);
	tgtSType = FindResTypeByName(tgtSTypeName);

	if (numFields != 6 || 
		(srcSType == UNDEFINED || tgtSType == UNDEFINED ||
		IsOutOfBound(srcSID, 0, g_systemInfo.sTypes[srcSType].limit - 1, "Source Server ID") ||
		IsOutOfBound(tgtSID, 0, g_systemInfo.sTypes[tgtSType].limit - 1, "Target Server ID")))
		return FALSE;

	strcpy(orgMsg, msg);
	
	if (srcSType == tgtSType && srcSID == tgtSID) {
		sprintf(msg, "Cannot migrate to itself (%s)", orgMsg);
		return FALSE;
	}
	
	srcServer = &g_systemInfo.servers[srcSType][srcSID];

	if (!(sJob = FindSchedJob(jID, srcServer))) {
		sprintf(msg, "No such job (%d) on %s %d (%s)", jID, srcSTypeName, srcSID, orgMsg);
		return FALSE;
	}
	else
	if (sJob->endTime == g_ss.curSimTime) {
		sprintf(msg, "Job %d cannot be migrated as it is completing at %d (NOW) (%s)", jID, g_ss.curSimTime, orgMsg);
		return FALSE;
	}
	
	dupSJob = CopySchedJob(sJob);
	
	
	dupSJob->sType = tgtSType;
	dupSJob->sID = tgtSID;
	dupSJob->xState = XJS_Migrated;

	ret = ScheduleJob(jID, tgtSType, tgtSID, dupSJob);

	if (ret == SCHD_Valid) {
		DisconnectSchedJob(sJob, srcServer);
		if (sJob->startTime != UNKNOWN) // a scheduled job is either running or ready to run as soon as the server gets finished the booting
			UpdateServerCapacity(srcServer, &sJob->job->resReq, INCREASE);
		free(sJob);
		if (srcServer->state == SS_Booting && !srcServer->waiting && !srcServer->running)
			SetServerToInactive(srcServer);
		else
		if (srcServer->state == SS_Active && !srcServer->waiting && !srcServer->running)
			srcServer->state = SS_Idle;
		else
		if (srcServer->state == SS_Active && srcServer->waiting)
			RunReadyJobs(srcServer);
	}
	else
		free(dupSJob);

	return (ret == SCHD_Valid);
}


// check all completed jobs between the last simulation time and the current one
void UpdateRunningJobs(Server *server)
{
	SchedJob *sJob, *nsJob;
	
	for (sJob = server->running; sJob; sJob = nsJob) { // the running queue may increase while checking (this 'for' loop)
		if (sJob->endTime <= g_ss.curSimTime) { // completed
			SchedJob *wSJob;

			MoveSchedJobState(sJob, JS_Completed, server);
			UpdateServerCapacity(server, &sJob->job->resReq, INCREASE);
			// when a job completes check if one or more waiting jobs that can run
			while ((wSJob = GetNextJobToRun(server, sJob->endTime))) {	// no backfilling
				MoveSchedJobState(wSJob, JS_Running, server);
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

// update server states based on g_ss.curSimTime
// available resources should be updated accounting for running and waiting jobs
unsigned int UpdateServerStates()
{
	int i, nsTypes = g_systemInfo.numServTypes;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	int lastSchdOpTime;
	
	g_logCnt = 0;
	g_logSize = DEFAULT_LOG_CNT;
	g_schedOps = (Log *)malloc(sizeof(Log) * g_logSize);
	
	for (i = 0; i < nsTypes; i++) {
		int j, limit = sTypes[i].limit;

		for (j = 0; j < limit; j++) {
			Server *server = GetServer(i, j);
			// checking Active server
			UpdateRunningJobs(server);

			// checking Booting server
			if (server->startTime <= g_ss.curSimTime && server->state == SS_Booting) { // should become Active
				int nRJobs = UpdateBootingServerState(server);
				assert (nRJobs);
				// after the server has become with one or more running jobs, 
				// check if any job has completed between the last simulation time and the current time
				UpdateRunningJobs(server); 
			}
			if (server->state == SS_Active && !server->running)	// all jobs completed
				server->state = SS_Idle;
		}
	}
	lastSchdOpTime = PrintLog();
	free(g_schedOps);
	
	return lastSchdOpTime;
}


int FindResTypeByName(char *name)
{
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	ServerTypeProp *sType;
	int i, nsTypes = g_systemInfo.numServTypes;
	
	for (i = 0, sType = &sTypes[i]; i < nsTypes && strcmp(sType->name, name); i++, sType = &sTypes[i]);
	if (i >= nsTypes) {
		fprintf(stderr, "No such server type by %s exists!\n", name);
		return UNKNOWN;
	}
	
	return i;
}

inline char *FindResTypeNameByType(int type)
{
	if (type < 0 || type >= g_systemInfo.numServTypes)
		return UNKNOWN_SERVER_TYPE;
	else
		return (g_systemInfo.sTypes[type].name);
}

inline int GetServerLimit(int type)
{
	if (type < 0 || type >= g_systemInfo.numServTypes)
		return UNKNOWN;
	else
		return g_systemInfo.sTypes[type].limit;
}


int GetServerAvailTime(int sType, int sID, ServerRes *resReq)
{
	Server *server = &g_systemInfo.servers[sType][sID];
	int availTime = UNKNOWN;	
		
	if (server->state == SS_Idle)
		availTime = g_ss.curSimTime;
	else
	if (server->state == SS_Inactive)
		availTime = g_ss.curSimTime + GetServerBootupTime(sType);
	
	if (resReq && availTime == UNKNOWN)	{// either Active or Booting
		if (IsSuffAvailRes(resReq, &server->availCapa) && !server->waiting)
			availTime = (server->state == SS_Booting) ? server->startTime : g_ss.curSimTime;
	}
	
	return availTime;
}


int SendDataHeader(char *cmd, int nRecs)
{
	char msgToSend[LARGE_BUF_SIZE];
	char msgRcvd[LARGE_BUF_SIZE];
	int recLen;	// record length
	int ret;

	if (!strcmp(cmd, "RESC"))
		strcpy(msgToSend, "DATA");	// old RECS
	else {
		if (!strcmp(cmd, "GETS"))
			recLen = !g_systemInfo.rFailT ? g_sConfig.regGETSRecLen : g_sConfig.regGETSRecLen + g_sConfig.failGETSRecLen;
		else
		if (!strcmp(cmd, "LSTJ"))
			recLen = g_sConfig.LSTJRecLen;
		
		sprintf(msgToSend, "DATA %d %d", nRecs, recLen);
	}
		
	SendMsg(msgToSend);
	while ((ret = RecvMsg(msgToSend, msgRcvd)) != INTACT_QUIT && strcmp(msgRcvd, "OK"))
		fprintf(stderr, "The message is supposed to be \"OK\", but received \"%s\"\n", msgRcvd);
	/*if (ret != INTACT_QUIT && strcmp(msgRcvd, "OK")) {
		fprintf(stderr, "The message is supposed to be \"OK\", but received \"%s\"\n", msgRcvd);
	}*/
	
	return ret;
}

inline int GetServerBootupTime(int type)
{
	assert(type >= 0 && type <= g_systemInfo.numServTypes);
	return g_systemInfo.sTypes[type].bootupTime;
}

// DATA format: server_type: int server_id: int state: int avail_time: int avail_cores: int avail_mem: int avail_disk: int; 
// the available time will be -1 if there are one or more running/waiting jobs
int SendResInfoAll(int oldRESC)
{
	int i, nsTypes = g_systemInfo.numServTypes;
	int numMsgSent = 0, ret;
	
	for (i = 0; i < nsTypes; i++) {
		 ret = SendResInfoByType(i, NULL, oldRESC);
		 if (ret == INTACT_QUIT) return ret;
		numMsgSent += ret;
	}
		
	return numMsgSent;
}

int SendResInfoByType(int type, ServerRes *resReq, int oldRESC)
{
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	int i, ret;
	int numMsgSent = 0;	
	int limit = sTypes[type].limit;
		
	for (i = 0; i < limit; i++) {
		Server *server = GetServer(type, i);
		
		// resReq is specified in the case of 'RESC Avail'; 
		// in this case, if there are insufficient available resources or waiting jobs,
		// the server is not available for the job
		// TODO: check the option of RESC explicitly
		if (resReq && (!IsSuffAvailRes(resReq, &server->availCapa) || server->waiting))
			continue;
		 ret = SendResInfoByServer(type, i, resReq, oldRESC);
		 if (ret == INTACT_QUIT) return ret;
		 numMsgSent += ret;
	}
	
	return numMsgSent;	
}

/*+++
int SendResInfoByType(int type, ServerRes *resReq, int oldRESC)
{
	int i, numMsgSent = 0;	
	int limit = g_systemInfo.sTypes[type].limit;
		
	for (i = 0; i < limit; i++)
		numMsgSent += SendResInfoByServer(type, i, resReq, oldRESC);
	
	return numMsgSent;	
}
+++*/

char *ConcatBatchMsg(char *dest, char *src)
{
	int destMsgSize = dest ? strlen(dest) : 0;
	int newMsgSize = destMsgSize + strlen(src) + 1;

	dest = (char *)realloc(dest, newMsgSize);
	if (destMsgSize == 0)	// for the first time
		strcpy(dest, src);
	else
		strcat(dest, src);

	return dest;
}


int SendResInfoByServer(int type, int id, ServerRes *resReq, int oldRESC)
{
	Server *server = GetServer(type, id);
	int availTime = UNKNOWN; // for RESC
	int numWJobs, numRJobs;
	int ret = TRUE; // 1 as the number of messages sent
	char msgToSend[LARGE_BUF_SIZE];
	char msgRcvd[LARGE_BUF_SIZE];
	char curMsg[LARGE_BUF_SIZE];

	if (oldRESC) {
		availTime = GetServerAvailTime(type, id, resReq);
		if (g_systemInfo.rFailT)
			sprintf(msgToSend, "%s %d %d %d %d %d %d %d %d %d %d %d %d", FindResTypeNameByType(server->type), id, server->state, 
				availTime, server->availCapa.cores, server->availCapa.mem, server->availCapa.disk, server->failInfo.numFails, 
				server->failInfo.totalFailTime, server->failInfo.mttf, server->failInfo.mttr, 
				server->failInfo.madf, server->failInfo.lastOpTime);
		else
			sprintf(msgToSend, "%s %d %d %d %d %d %d", FindResTypeNameByType(server->type), id, server->state, 
				availTime, server->availCapa.cores, server->availCapa.mem, server->availCapa.disk);

		SendMsg(msgToSend);
		
		while ((ret = RecvMsg(msgToSend, msgRcvd)) != INTACT_QUIT && strcmp(msgRcvd, "OK"))
			fprintf(stderr, "The message is supposed to be \"OK\", but received \"%s\"\n", msgRcvd);
	
		/*ret = RecvMsg(msgToSend, msgRcvd);
		if (ret != INTACT_QUIT && strcmp(msgRcvd, "OK")) {
			fprintf(stderr, "The message is supposed to be \"OK\", but received \"%s\"\n", msgRcvd);
		}*/
	}
	else { // GETS
		numWJobs = CountJobs(*GetSchedJobList(JS_Waiting, server));
		numRJobs = CountJobs(*GetSchedJobList(JS_Running, server));
		if (g_systemInfo.rFailT)
			sprintf(curMsg, "%s %d %s %d %d %d %d %d %d %d %d %d %d %d %d\n", FindResTypeNameByType(server->type), id, 
				servStates[server->state].state, server->startTime, 
				server->availCapa.cores, server->availCapa.mem, server->availCapa.disk, 
				numWJobs, numRJobs, server->failInfo.numFails, server->failInfo.totalFailTime, 
				server->failInfo.mttf, server->failInfo.mttr, server->failInfo.madf, server->failInfo.lastOpTime);
		else
			sprintf(curMsg, "%s %d %s %d %d %d %d %d %d\n", FindResTypeNameByType(server->type), id, servStates[server->state].state, 
				server->startTime, server->availCapa.cores, server->availCapa.mem, server->availCapa.disk, numWJobs, numRJobs);

		if (strlen(g_bufferedMsg) + strlen(curMsg) > XLARGE_BUF_SIZE) {
			g_batchMsg = ConcatBatchMsg(g_batchMsg, g_bufferedMsg);
			g_batchMsg = ConcatBatchMsg(g_batchMsg, curMsg);
			g_bufferedMsg[0] = '\0';
		}
		else
			strcat(g_bufferedMsg, curMsg);
	}

	return ret;
}

int SendResInfoByAvail(ServerRes *resReq, int oldRESC)
{
	int i, nsTypes = g_systemInfo.numServTypes;
	int numMsgSent = 0;
	
	for (i = 0; i < nsTypes; i++)
		numMsgSent += SendResInfoByType(i, resReq, oldRESC);
		
	return numMsgSent;
}

/*+++
int SendResInfoByAvail(ServerRes *resReq, int oldRESC)
{
	int i, j, nsTypes = g_systemInfo.numServTypes;
	int numMsgSent = 0;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	
	
	for (i = 0; i < nsTypes; i++) {
		int limit = sTypes[i].limit;
		Server *servers  = g_systemInfo.servers[i];

		for (j = 0; j < limit; j++) {
			Server *server = &servers[j];

			if ((!IsSuffAvailRes(resReq, &server->availCapa) || server->waiting))
				continue;
			numMsgSent += SendResInfoByServer(i, j, resReq, oldRESC);
		}	
	}
		
	return numMsgSent;
}
+++*/

int SendResInfoByCapacity(ServerRes *resReq, int oldRESC)
{
	int i, nsTypes = g_systemInfo.numServTypes;
	int numMsgSent = 0;
	
	for (i = 0; i < nsTypes; i++) {
		// &g_systemInfo.servers[i][0] to get the first server of a type to check the init capacity
		if (!IsSuffServerRes(resReq, GetServer(i, 0)))
			continue;
		numMsgSent += SendResInfoByType(i, NULL, oldRESC);
	}
		
	return numMsgSent;
}


/*+++
int SendResInfoByCapacity(ServerRes *resReq, int oldRESC)
{
	int i, j, nsTypes = g_systemInfo.numServTypes;
	int numMsgSent = 0;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	
	for (i = 0; i < nsTypes; i++) {
		int limit = sTypes[i].limit;
		Server *servers  = g_systemInfo.servers[i];
		
		// &g_systemInfo.servers[0] to get the first server of a type to check the init capacity
		if (!IsSuffServerRes(resReq, &servers[0]))
			continue;

		for (j = 0; j < limit; j++)
			numMsgSent += SendResInfoByServer(i, j, resReq, oldRESC);
	}
		
	return numMsgSent;
}
+++*/

/* Simular to GETS Capable, but the search stops when it reaches the last Inactive server 
	that meets resource requirements
	This option might not be suitable for some algorithms, such as WF
*/
int SendResInfoByBounded(ServerRes *resReq, int oldRESC)
{
	int i, j, nsTypes = g_systemInfo.numServTypes;
	int lastInactive, lastInactiveSType, lastInactiveSID, numMsgSent = 0;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	int done = FALSE;
	
	lastInactive = FALSE;
	lastInactiveSType = lastInactiveSID = UNKNOWN;
	// find out the last Inactive server
	for (i = 0; i < nsTypes; i++) {
		int limit = sTypes[i].limit;
		
		if (!IsSuffServerRes(resReq, GetServer(i, 0)))
			continue;
		for (j = 0; j < limit; j++) {
			Server *sInstance = GetServer(i, j);
			
			if (sInstance->state == SS_Inactive) {
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
		// &g_systemInfo.servers[i][0] to get the first server of a type to check the init capacity
		if (!IsSuffServerRes(resReq, GetServer(i, 0)))
			continue;
		for (j = 0; j < limit && !done; j++) {
			numMsgSent += SendResInfoByServer(i, j, resReq, oldRESC);
			if (lastInactive && i == lastInactiveSType && j == lastInactiveSID)
				done = TRUE;
		}
	}
		
	return numMsgSent;
}

void FinalizeBatchMsg()
{
	if (strlen(g_bufferedMsg)) {	// last buffered messages
		g_batchMsg = ConcatBatchMsg(g_batchMsg, g_bufferedMsg);
		g_bufferedMsg[0] = '\0';
	}

	if (g_batchMsg)
		g_batchMsg[strlen(g_batchMsg) - 1] = '\0';	// to remove the last newline character
}

int SendBatchMsg(char *cmd, int numMsgs)
{
	char msgRcvd[LARGE_BUF_SIZE];
	int ret;

	ret = SendDataHeader(cmd, numMsgs);
	if (ret != INTACT_QUIT && numMsgs) {

		FinalizeBatchMsg();

		SendMsg(g_batchMsg);
		while ((ret = RecvMsg(g_batchMsg, msgRcvd)) != INTACT_QUIT && strcmp(msgRcvd, "OK"))
			fprintf(stderr, "The message is supposed to be \"OK\", but received \"%s\"\n", msgRcvd);
		/*
		ret = RecvMsg(g_batchMsg, msgRcvd);
		if (ret != INTACT_QUIT && strcmp(msgRcvd, "OK"))
			fprintf(stderr, "The message is supposed to be \"OK\", but received \"%s\"\n", msgRcvd);*/

		free(g_batchMsg);
		g_batchMsg = NULL;
	}
	
	return ret;
}

/*
GETS (formerly RESC) filter_cmd

filter_cmd can be one of the following:
- All
- Type server_type
- One server_type server_id
- Avail core_requirement memory_requirement disk_requirement
- Capable core_requirement memory_requirement disk_requirement
*/
int SendResInfo(char *msgRcvd)
{
	char *str, optStr[LARGE_BUF_SIZE];
	int numMsgSent = 0;
	int ret;
	int oldRESC = !(strncmp(msgRcvd, "RESC", CMD_LENGTH));
	
	for (str = &msgRcvd[CMD_LENGTH]; !isalpha(*str) && *str != '\0'; str++); // skip spaces between "GETS" and the next search term
	if (*str != '\0') {
		if (!strncmp(str, getsOptions[GO_All].optstr, getsOptions[GO_All].optlen)) {
			if (oldRESC && (ret = SendDataHeader("RESC", 1)) == INTACT_QUIT) return ret;
			if ((numMsgSent = SendResInfoAll(oldRESC)) == INTACT_QUIT) return INTACT_QUIT;
		}
		else
		if (!strncmp(str, getsOptions[GO_Type].optstr, getsOptions[GO_Type].optlen)) {
			int numFields, servType;
			char servTypeName[LARGE_BUF_SIZE] = "";
			
			numFields = sscanf(str, "%s %s", optStr, servTypeName);
			if (numFields != 2) return UNDEFINED;
			servType = FindResTypeByName(servTypeName);
			if (servType == UNDEFINED) return UNDEFINED;
			if (oldRESC && (ret = SendDataHeader("RESC", 1)) == INTACT_QUIT) return ret;
			if ((numMsgSent = SendResInfoByType(servType, NULL, oldRESC)) == INTACT_QUIT) return INTACT_QUIT;
		}
		else
		if (!strncmp(str, getsOptions[GO_One].optstr, getsOptions[GO_One].optlen)) {
			int numFields, servType, sID;
			char servTypeName[LARGE_BUF_SIZE] = "";
			
			numFields = sscanf(str, "%s %s %d", optStr, servTypeName, &sID);
			if (numFields != 3) return UNDEFINED;
			servType = FindResTypeByName(servTypeName);
			if (servType == UNDEFINED) 
			if (servType == UNDEFINED || sID < 0 || sID >= GetServerLimit(servType))
				return UNDEFINED;
			if (oldRESC && (ret = SendDataHeader("RESC", 1)) == INTACT_QUIT) return ret;
			if ((numMsgSent = SendResInfoByServer(servType, sID, NULL, oldRESC)) == INTACT_QUIT) return INTACT_QUIT;
		}
		else
		if (!strncmp(str, getsOptions[GO_Avail].optstr, getsOptions[GO_Avail].optlen)) {
			int numFields;
			ServerRes resReq;
			ServerRes *maxCapa = &g_systemInfo.sTypes[g_systemInfo.maxServType].capacity;
			
			numFields = sscanf(str, "%s %d %d %d", optStr, &resReq.cores, &resReq.mem, &resReq.disk);
			if (numFields != 4 || 
				IsOutOfBound(resReq.cores, 1, maxCapa->cores, limits[CCnt_Limit].name) ||
				IsOutOfBound(resReq.mem, MIN_MEM_PER_JOB_CORE, maxCapa->mem, limits[Mem_Limit].name) ||
				IsOutOfBound(resReq.disk, MIN_DISK_PER_JOB_CORE, maxCapa->disk, limits[Disk_Limit].name))
				return UNDEFINED;
			if (oldRESC && (ret = SendDataHeader("RESC", 1)) == INTACT_QUIT) return ret;
			if ((numMsgSent = SendResInfoByAvail(&resReq, oldRESC)) == INTACT_QUIT) return INTACT_QUIT;
		}
		else
		if (!strncmp(str, getsOptions[GO_Capable].optstr, getsOptions[GO_Capable].optlen)) {
			int numFields;
			ServerRes resReq;
			ServerRes *maxCapa = &g_systemInfo.sTypes[g_systemInfo.maxServType].capacity;
			
			numFields = sscanf(str, "%s %d %d %d", optStr, &resReq.cores, &resReq.mem, &resReq.disk);
			if (numFields != 4 || 
				IsOutOfBound(resReq.cores, 1, maxCapa->cores, limits[CCnt_Limit].name) ||
				IsOutOfBound(resReq.mem, MIN_MEM_PER_JOB_CORE, maxCapa->mem, limits[Mem_Limit].name) ||
				IsOutOfBound(resReq.disk, MIN_DISK_PER_JOB_CORE, maxCapa->disk, limits[Disk_Limit].name))
				return UNDEFINED;
			if (oldRESC && (ret = SendDataHeader("RESC", 1)) == INTACT_QUIT) return ret;
			if ((numMsgSent = SendResInfoByCapacity(&resReq, oldRESC)) == INTACT_QUIT) return INTACT_QUIT;
		}
		else
		if (!strncmp(str, getsOptions[GO_Bounded].optstr, getsOptions[GO_Bounded].optlen)) {
			int numFields;
			ServerRes resReq;
			ServerRes *maxCapa = &g_systemInfo.sTypes[g_systemInfo.maxServType].capacity;

			numFields = sscanf(str, "%s %d %d %d", optStr, &resReq.cores, &resReq.mem, &resReq.disk);
			if (numFields != 4 || 
				IsOutOfBound(resReq.cores, 1, maxCapa->cores, limits[CCnt_Limit].name) ||
				IsOutOfBound(resReq.mem, MIN_MEM_PER_JOB_CORE, maxCapa->mem, limits[Mem_Limit].name) ||
				IsOutOfBound(resReq.disk, MIN_DISK_PER_JOB_CORE, maxCapa->disk, limits[Disk_Limit].name))
				return UNDEFINED;
			if (oldRESC && (ret = SendDataHeader("RESC", 1)) == INTACT_QUIT) return ret;
			if ((numMsgSent = SendResInfoByBounded(&resReq, oldRESC)) == INTACT_QUIT) return INTACT_QUIT;
		}
		else
			return UNDEFINED;
	}
	else
		return UNDEFINED;

	if (!oldRESC)
		ret = SendBatchMsg("GETS", numMsgSent);

	return (ret != INTACT_QUIT ? numMsgSent : ret);
}

int SendJobsPerStateOnServer(SchedJob *sJob)
{
	char msgToSend[LARGE_BUF_SIZE];
	char msgRcvd[LARGE_BUF_SIZE];
	char curMsg[LARGE_BUF_SIZE];
	int numMsgs = 0;
	
	for (; sJob; sJob = sJob->next) {
		Job *job = sJob->job;
		ServerRes *resReq = &job->resReq;
		
		sprintf(curMsg, "%d %d %d %d %d %d %d %d\n", job->id, sJob->state, sJob->job->submitTime, sJob->startTime, job->estRunTime, 
			resReq->cores, resReq->mem, resReq->disk);

		if (strlen(g_bufferedMsg) + strlen(curMsg) > XLARGE_BUF_SIZE) {
			g_batchMsg = ConcatBatchMsg(g_batchMsg, g_bufferedMsg);
			g_batchMsg = ConcatBatchMsg(g_batchMsg, curMsg);
			g_bufferedMsg[0] = '\0';
		}
		else
			strcat(g_bufferedMsg, curMsg);

		numMsgs++;
	}
	
	return numMsgs;
}

int SendJobsOnServer(char *msgRcvd)
{
	char servTypeName[LARGE_BUF_SIZE];
	int sID, servType;
	int numFields, numMsgs = 0;
	int ret;
	Server *server;

	numFields = sscanf(msgRcvd, "LSTJ %s %d", servTypeName, &sID);
	if (numFields < 2 || 
		(servType = FindResTypeByName(servTypeName)) == UNDEFINED ||
		IsOutOfBound(sID, 0, g_systemInfo.sTypes[servType].limit - 1, "Server ID"))
		return UNDEFINED;

	server = GetServer(servType, sID);
	numMsgs += SendJobsPerStateOnServer(server->running); // Running jobs
	numMsgs += SendJobsPerStateOnServer(server->waiting); // Waiting jobs	
	ret = SendBatchMsg("LSTJ", numMsgs);

	return (ret != INTACT_QUIT ? numMsgs : ret);
}


inline int CountJobs(SchedJob *sJob)
{
	int cnt;
	
	for (cnt = 0; sJob; sJob = sJob->next, cnt++);
	
	return cnt;
}


int SendJobCountOfServer(char *msgRcvd)
{
	char servTypeName[LARGE_BUF_SIZE];
	int sID, servType, jState;
	int numFields;
	Server *server;
	SchedJob **sJobs;

	numFields = sscanf(msgRcvd, "CNTJ %s %d %d", servTypeName, &sID, &jState);
	if (numFields < 3 || 
		(servType = FindResTypeByName(servTypeName)) == UNDEFINED ||
		IsOutOfBound(sID, 0, g_systemInfo.sTypes[servType].limit - 1, "Server ID") ||
		IsOutOfBound(jState, 0, END_JOB_STATE, "Job state"))
		return UNDEFINED;

	server = GetServer(servType, sID);
	
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


int SendEstWTOfServer(char *msgRcvd)
{
	char servTypeName[LARGE_BUF_SIZE];
	int sID, servType;
	int numFields;
	Server *server;
	SchedJob **sJobs;

	numFields = sscanf(msgRcvd, "EJWT %s %d", servTypeName, &sID);
	if (numFields < 2 || 
		(servType = FindResTypeByName(servTypeName)) == UNDEFINED ||
		IsOutOfBound(sID, 0, g_systemInfo.sTypes[servType].limit - 1, "Server ID"))
		return UNDEFINED;

	server = GetServer(servType, sID);
	
	sJobs = GetSchedJobList(JS_Waiting, server);

	return GetTotalEstRT(*sJobs);
}


void SetServerToInactive(Server *server)
{
	ServerRes* capacity = &g_systemInfo.sTypes[server->type].capacity;
	
	server->state = SS_Inactive;
	server->startTime = UNKNOWN;
	SetServerRes(&server->availCapa, capacity);
}


int TerminateServer(char *cmd)
{
	char servTypeName[LARGE_BUF_SIZE];
	int servID, servType;
	int numFields, numJobsKilled = 0;
	int killedTime = g_ss.curSimTime;
	Server *server;

	numFields = sscanf(cmd, "TERM %s %d", servTypeName, &servID);
	if (numFields < 2 || 
		(servType = FindResTypeByName(servTypeName)) == UNDEFINED ||
		IsOutOfBound(servID, 0, g_systemInfo.sTypes[servType].limit - 1, "Server ID"))
		return UNDEFINED;
		
	server = GetServer(servType, servID);
	
	if (server->state == SS_Unavailable || server->state == SS_Inactive)
		return UNDEFINED;

	numJobsKilled += HandleKilledJobs(server->running, server, killedTime);
	server->running = NULL;
	
	numJobsKilled += HandleKilledJobs(server->waiting, server, killedTime);
	server->waiting = NULL;
	
	// job suspension is not supported at this stage!!!
	numJobsKilled += HandleKilledJobs(server->suspended, server, killedTime);
	server->suspended = NULL;

	if (server->state != SS_Booting) 
		server->totalUsage += killedTime - server->startTime;
	
	SetServerToInactive(server);
	
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
	
	if ((sJob = FindSchedJobByState(jID, JS_Running, server)))
		return sJob;
	if ((sJob = FindSchedJobByState(jID, JS_Waiting, server)))
		return sJob;
	if ((sJob = FindSchedJobByState(jID, JS_Suspended, server)))
		return sJob;
	
	return sJob;
}

void PreemptJob(SchedJob *sJob, int eventType, int eventTime)
{
	Server *server = GetServer(sJob->sType, sJob->sID);
	SchedJob **sJobList;
	
	DisconnectSchedJob(sJob, server);
	if (sJob->state == JS_Running)
		UpdateServerCapacity(server, &sJob->job->resReq, INCREASE);
	HandlePreemptedJob(sJob, eventType, eventTime);
	sJobList = GetSchedJobList(eventType, server);
	AddSchedJobToEnd(sJob, sJobList);
}


int KillJob(char *cmd)
{
	char servTypeName[LARGE_BUF_SIZE];
	int servID, servType, jID;
	int numFields;
	int killedTime = g_ss.curSimTime;
	Server *server;
	SchedJob *sJob;

	numFields = sscanf(cmd, "KILJ %s %d %d", servTypeName, &servID, &jID);
	if (numFields < 3 || 
		(servType = FindResTypeByName(servTypeName)) == UNDEFINED ||
		IsOutOfBound(servID, 0, g_systemInfo.sTypes[servType].limit - 1, "Server ID"))
		return UNDEFINED;
		
	server = GetServer(servType, servID);
	
	if (!(sJob = FindSchedJob(jID, server)))	// no such job found
		return UNDEFINED;
	
	PreemptJob(sJob, JS_Killed, killedTime);

	return jID;
}


int BackFillJob(Job *job)
{
	WaitingJob *wJob, *newWJob;
	Job *nextJob;
	int simEndTime = g_sConfig.termination.simEndTime * 1.5;	// 50% more time for now
	
	assert(g_ss.waitJQ);
	
	wJob = GetFirstWaitingJob();
	// the job sending back to the next job must be the first waiting job 
	assert(wJob->job->id == job->id);
	
	wJob = DisconnectWaitingJob(job->id);	
	newWJob = GetFirstWaitingJob();
	if (newWJob) {
		/* set the submit time of wJob to one time unit larger than that of newWJob;
			this is to avoid two jobs keep swapping positions when they both cannot be scheduled 
			(i.e., all servers capable of running them are unavailable/failed) without
			progressing simulation (time)
		*/
		wJob->submitTime = newWJob->submitTime + 1;
		// if the submit time of next job to schedule for the first time is less than that of wJob
		// then add the next job to g_ss.waitJQ
		//if (g_ss.curJobID < g_workloadInfo.numJobs) {
		while (g_ss.curJobID < g_workloadInfo.numJobs) {	// there might be more than one jobs with the same submission time
			nextJob = &g_workloadInfo.jobs[g_ss.curJobID];
			if (nextJob->submitTime < wJob->submitTime) {
				AddToWaitingJobList(nextJob, nextJob->submitTime);
				g_ss.curJobID++;
			}
			else
				break;
		}
	}
	else {
		// increase submit time of wJob by 1 to progress simulation
		wJob->submitTime++;
		// no more jobs to schedule other than wJob
		if (g_ss.curJobID >= g_workloadInfo.numJobs && wJob->submitTime > simEndTime)
			return UNDEFINED;
		else 
		if (g_ss.curJobID < g_workloadInfo.numJobs) {
			nextJob = &g_workloadInfo.jobs[g_ss.curJobID];
			AddToWaitingJobList(nextJob, nextJob->submitTime);
			g_ss.curJobID++;
		}
	}
	
	InsertWaitingJob(wJob);
	
	return TRUE;
}


int IsSchdInvalid(int jID, char *stName, int sID)
{
	int sType;
	
	if (!GetWaitingJob(jID))
		return SCHD_InvalidJob;
	if ((sType = FindResTypeByName(stName)) == UNDEFINED)
		return SCHD_InvalidServType;
	if (sID >= g_systemInfo.sTypes[sType].limit)
		return SCHD_InvalidServID;
		
	return SCHD_Valid;
}

SchedJob **GetSchedJobList(int jState, Server *server)
{
	switch (jState) {
		case JS_Waiting:
			return &server->waiting;
		case JS_Running:
			return &server->running;
		case JS_Failed:
			return &server->failed;
		case JS_Suspended:
			return &server->suspended;
		case JS_Completed:
			return &server->completed;
		case JS_Preempted:
			return &server->preempted;
		case JS_Killed:
			return &server->killed;
		default:
			return NULL;
	}
}

int IsSuffAvailRes(ServerRes *resReq, ServerRes *availCapa)
{
	return (availCapa->cores >= resReq->cores &&
		availCapa->mem >= resReq->mem &&
		availCapa->disk >= resReq->disk);
}

int IsSuffServerRes(ServerRes *resReq, Server *server)
{
	ServerRes *capacity = &g_systemInfo.sTypes[server->type].capacity;
	
	return (capacity->cores >= resReq->cores &&
				capacity->mem >= resReq->mem &&
				capacity->disk >= resReq->disk);
}

int AssignToServer(SchedJob *sJob, Server *server, char *msg)
{
	SchedJob **sJobList;
	
	sJobList = GetSchedJobList(sJob->state, server);
	if (sJob->state == JS_Running)
		InsertSJobByEndTime(sJob, sJobList);
	else
		AddSchedJobToEnd(sJob, sJobList);
	
	if (simOpts[SO_Verbose].used != VERBOSE_STATS)
		printf("t: %10d job %5d (%s) on #%2d of server %s (%s) %s\n", 
			g_ss.curSimTime, sJob->job->id, jobStates[sJob->state].state, server->id, 
			g_systemInfo.sTypes[server->type].name, servStates[server->state].state, msg);
		//* for debugging
		/*printf("t: %10d job %5d (#cores: %2d, mem: %6d & disk: %6d, state: %d) on #%2d of server %s (state: %s) starting at %d - %d SCHEDULED\n", 
			g_ss.curSimTime, sJob->job->id, sJob->job->resReq.cores, sJob->job->resReq.mem, sJob->job->resReq.disk, sJob->state,
			server->id, g_systemInfo.sTypes[server->type].name, servStates[server->state].stateStr, sJob->startTime, sJob->endTime);*/
	
	// either running immediately or the first job to run after booting complete
	if (sJob->state == JS_Running || (server->state == SS_Booting && sJob->startTime != UNKNOWN)) {
		UpdateServerCapacity(server, &sJob->job->resReq, DECREASE);
		if (sJob->state == JS_Running) {
			if (simOpts[SO_Verbose].used != VERBOSE_STATS)
				printf("t: %10d job %5d on #%2d of server %s RUNNING\n", 
					sJob->startTime, sJob->job->id, server->id, g_systemInfo.sTypes[server->type].name);
#ifdef DEBUG
	fprintf(stdout, "JOB: %d -- Finish Time: %d\n", sJob->job->id, sJob->endTime);
#endif
		}
	}
	
	return 0;
}


// check tha availablity of server including state and resource capacities.
// if valid, create SchedJob and add it to server, and remove the job from g_ss.waitJQ
int ScheduleJob(int jID, int sType, int sID, SchedJob *mSJob)
{
	Server *server = GetServer(sType, sID);
	ServerRes *resReq = &g_workloadInfo.jobs[jID].resReq;
	SchedJob *sJob;
	WaitingJob *wJob;
	Job *job = NULL;
	int actWJob = TRUE;
	char msg[LARGE_BUF_SIZE];
	
	if (server->state == SS_Unavailable)
		return SCHD_ServUnavailable;
	if (!IsSuffServerRes(resReq, server))
		return SCHD_ServIncapable;

	if (!mSJob) {
		wJob = GetWaitingJob(jID);
		job = wJob->job;
		sJob = (SchedJob *)malloc(sizeof(SchedJob));
		sJob->sType = sType;
		sJob->sID = sID;
		sJob->job = job;
		sJob->xState = XJS_None;
		sJob->next = NULL;
	}
	else {
		sJob = mSJob;
		job = sJob->job;
	}

	if (server->state == SS_Idle) {	// can start immediately
		sJob->startTime = g_ss.curSimTime;
		sJob->endTime = sJob->startTime + job->actRunTime;
		sJob->state = JS_Running;
		server->state = SS_Active;
	}
	else
	if (server->state == SS_Inactive) {
		sJob->startTime = g_ss.curSimTime + GetServerBootupTime(server->type);
		server->startTime = sJob->startTime;
		server->lastAccTime = sJob->startTime;
		sJob->endTime = sJob->startTime + job->actRunTime;
		sJob->state = JS_Waiting;
		server->state = SS_Booting;
	}
	else
	if (server->state == SS_Booting || server->state == SS_Active) {
		// in the case of server being booted, even the very first job scheduled is waiting; 
		// and thus, the start time of the last job has to be checked.
		// In other words, if the start time of the last job is set (but in the waiting state), 
		// the next job can also start when the booting completes 
		// as long as the remaining resources are sufficient for the job
		if (server->state == SS_Booting && server->waiting) {
			SchedJob *cur;
			
			for (cur = server->waiting; cur->next; cur = cur->next);	// find the last scheduled job
			if (cur->startTime != UNKNOWN)
				actWJob = FALSE;
		}
		else
		if (server->state == SS_Active && !server->waiting)
			actWJob = FALSE;
			
		// add the job to the end of the waiting job queue
		// For an Active server, if no jobs in the waiting queue and sufficient resources to run the job 
		// in parallel with whatever running at the moment
		// For a Booting server, there could be more than one waiting jobs 
		// since jobs can be only scheduled if there are sufficient resources
		if (!actWJob && IsSuffAvailRes(resReq, &server->availCapa)) { 
			sJob->startTime = server->state == SS_Booting ? server->startTime : g_ss.curSimTime;
			sJob->endTime = sJob->startTime + job->actRunTime;
			sJob->state = sJob->startTime > g_ss.curSimTime ? JS_Waiting : JS_Running;
		}
		else
		{
			sJob->startTime = UNKNOWN;
			sJob->endTime = UNKNOWN;
			sJob->state = JS_Waiting;
		}
	}

	if (!mSJob)
		strcpy(msg, "SCHEDULED");
	else
		strcpy(msg, "MIGRATED");

	AssignToServer(sJob, server, msg);
	if (!mSJob)
		FreeWaitingJob(job->id);

	return SCHD_Valid;
}

int CountWJobs()
{
	int i;
	WaitingJob *cur;
	
	for (i = 0, cur = g_ss.waitJQ; cur; cur = cur->next, i++);
	
	return i;
}

void PrintUnscheduledJobs()
{
	WaitingJob *wJob;
	Job *job;
	int i;
	
	printf("# -----------[ Jobs in the waiting queue ]-----------\n");
	for (wJob = g_ss.waitJQ; wJob && wJob; wJob = wJob->next)
		if (wJob->submitTime != wJob->job->submitTime)	// resubmitted due perhaps to resource failure
			printf("# Job %d initially submitted at %d and resubmitted at %d...\n", wJob->job->id, wJob->job->submitTime, wJob->submitTime);
		else
			printf("# Job %d submitted at %d...\n", wJob->job->id, wJob->job->submitTime);
	printf("# ---------------------------------------------------\n");
	printf("# --------[ Jobs created and yet, submitted ]--------\n");
	
	for (i = g_ss.curJobID, job = &g_workloadInfo.jobs[i]; i < g_workloadInfo.numJobs; i++, job = &g_workloadInfo.jobs[i])
		printf("# Job %d created to be submitted at %d...\n", job->id, job->submitTime);
	printf("# ---------------------------------------------------\n");
}

inline int ServerUsed(Server *server)
{
	return (server->waiting || server->running || server->failed || 
		server->suspended || server->completed || server->preempted || server->killed);
}


int GetLastServerTime(SchedJob *sJobs, int prevLTime)
{
	SchedJob *sJob;
	int curLTime = INT_MIN;
	
	for (sJob = sJobs; sJob; sJob = sJob->next)
		if (sJob->endTime > curLTime)
			curLTime = sJob->endTime;
		
	return (prevLTime > curLTime ? prevLTime : curLTime);
}


int GetLatestTimeServerUsed(Server *server)
{
	int lTime = INT_MIN;

	lTime = GetLastServerTime(server->running, lTime);
	lTime = GetLastServerTime(server->completed, lTime);
	lTime = GetLastServerTime(server->failed, lTime);
	lTime = GetLastServerTime(server->preempted, lTime);
	lTime = GetLastServerTime(server->suspended, lTime);
	
	// GetLatestTimeServerUsed is called only when server start time is not UNKNOWN, i.e., used
	assert(lTime != INT_MIN);

	return (lTime > g_ss.actSimEndTime ? g_ss.actSimEndTime : lTime);
}

// calculate effective server usage only accounting for (successfully) completed jobs
int CalcEfftvServerUsage(Server *server)
{
	int usage = 0, lat = INT_MAX;
	SchedJob *cSJob;
	
	if (!server->completed) return usage;
	// find the earliest start time among completed jobs
	for (cSJob = server->completed; cSJob; cSJob = cSJob->next) {
		int st = cSJob->startTime;
		if (st < lat)
			lat = st;
	}
	
	for (cSJob = server->completed; cSJob; cSJob = cSJob->next) {
		SchedJob *sJob;
		
		if (cSJob->startTime > lat) {
			int est = cSJob->startTime;

			for (sJob = cSJob->next; sJob; sJob = sJob->next) {
				int st = sJob->startTime;
				if (st < est)
					est = st;
			}	
			lat =  est > lat ? est : lat;
		}
		usage += (cSJob->endTime - lat);
		lat = cSJob->endTime;
	}

	return usage;
}

int PrintSchedJobs(Server *server, int jobState)
{
	SchedJob *sJob, **sJobList = GetSchedJobList(jobState, server);
	int numSJobs = 0;
	
	if (!*sJobList) return 0;
	printf("# ----- Job %s\n", jobStates[jobState].state);
	for (sJob = *sJobList; sJob; sJob = sJob->next) {
		printf("# ----- job %d -- submit: %d, start: %d, end: %d, core: %d, mem: %d, disk: %d\n", 
			sJob->job->id, sJob->job->submitTime, sJob->startTime, sJob->endTime, 
			sJob->job->resReq.cores, sJob->job->resReq.mem, sJob->job->resReq.disk);
	}
	
	return numSJobs;
}

void PrintStats()
{
	int i, nsTypes = g_systemInfo.numServTypes;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	Server **servers = g_systemInfo.servers;
	StatsPerServType *stats = (StatsPerServType *)calloc(nsTypes, sizeof(StatsPerServType));
	int totalServCnt = 0;
	long int totalActUsage = 0;
	long int totalUsage = 0;
	long int totalEffUsage = 0;
	float totalUtil = 0;
	float grandTotal = 0;
	float avgUtil = 0, avgEffUsage = 0;
	long totalWT = 0;
	long totalRT = 0;
	long avgWT = 0, avgET = 0, avgTT = 0;
	int numSJobs = 0;
	int numFJobs = 0;
	
	printf("# -------------------------------------------------------------------------------------\n");
	for (i = 0; i < nsTypes; i++) {
		int j, limit = sTypes[i].limit;
		float tTypeUtil = 0, totalCost = 0;
		
		stats[i].usage = (float *)calloc(limit, sizeof(int));
		stats[i].rentalCost = (float *)calloc(limit, sizeof(double));
		
				if (g_perServerStats)
			printf("# --- [%s]\n", FindResTypeNameByType(i));
		
		for (j = 0; j < limit; j++) {
			SchedJob *sJob;
			Server *server = &servers[i][j];
			float servUtil = 0, servCost = 0;
			int effUsage;
			
			if (!ServerUsed(server)) {
				if (g_perServerStats)
					printf("# --- server %d NEVER USED!\n", j);
				continue;
			}
			numFJobs += CountJobs(server->failed);
			// server may have started, but never used for valid work (e.g., failed while booting)
			if (server->startTime == UNKNOWN && !server->totalUsage) {
				if (g_perServerStats)
					printf("# --- server %d NOT USED for valid work!\n", j);
				continue;
			}
			if (server->startTime != UNKNOWN)
				server->totalUsage += (GetLatestTimeServerUsed(server) - server->startTime);
			stats[i].numServUsed++;
			for (sJob = server->completed; sJob; sJob = sJob->next, numSJobs++) {
				// Accumulate waiting times, execution times and turnaround times
				totalWT += (sJob->startTime - sJob->job->submitTime);
				totalRT += sJob->job->actRunTime;
			}
			servUtil = (float)server->actUsage / server->totalUsage;
			servCost = server->totalUsage * (sTypes[i].rate / HOUR_IN_SECONDS);
			tTypeUtil += servUtil;
			totalCost += servCost;
			totalActUsage += server->actUsage;
			effUsage = CalcEfftvServerUsage(server);
			totalEffUsage += effUsage;
			totalUsage += server->totalUsage;
			if (g_perServerStats) {
				int tFJobs = CountJobs(server->failed);
				int tCJobs = CountJobs(server->completed);
				int tPJobs = CountJobs(server->preempted);
				int tKJobs = CountJobs(server->killed);
				int tSJobs = tFJobs + tCJobs + tPJobs + tKJobs;
				
				printf("# --- server %d -- util: %.2f%%, cost: $%.2f, %d times failed (%d), ",
						j, servUtil * 100, servCost, server->failInfo.numFails, server->failInfo.totalFailTime);
				printf("act. usage: %d, total usage: %d , ef. usage: %d (%.2f%%); ",
						server->actUsage, server->totalUsage, effUsage, (float)effUsage / server->totalUsage * 100);
				printf("jobs: %d scheduled, %d completed, %d failed, %d preempted, %d killed\n", 
						tSJobs, tCJobs, tFJobs, tPJobs, tKJobs);
				PrintSchedJobs(server, JS_Completed);
				PrintSchedJobs(server, JS_Failed);
				PrintSchedJobs(server, JS_Preempted);
				PrintSchedJobs(server, JS_Killed);
			}
		}
		printf("# %d %s servers used with a utilisation of %.2f at the cost of $%.2f\n", 
			stats[i].numServUsed, FindResTypeNameByType(i), stats[i].numServUsed > 0 ? 
			tTypeUtil / stats[i].numServUsed * 100 : stats[i].numServUsed, totalCost);
		totalServCnt += stats[i].numServUsed;
		totalUtil += tTypeUtil;
		grandTotal += totalCost;
	}
	printf("# ==================================== [ Summary ] ====================================\n");
	printf("# actual simulation end time: %u, #jobs: %d (failed %d times)\n", 
		g_ss.actSimEndTime, numSJobs, numFJobs);
	
	//printf("# total #servers used: %d, avg util: %.2f%% (ef. usage: %.2f%%), total cost: $%.2f\n", 
	//	totalServCnt, totalUtil / totalServCnt * 100, (float)totalEffUsage / totalUsage * 100, grandTotal);
	
	avgUtil = totalServCnt ? totalUtil / totalServCnt * 100 : 0;
	avgEffUsage = totalUsage? (float)totalEffUsage / totalUsage * 100 : 0;
	printf("# total #servers used: %d, avg util: %.2f%% (ef. usage: %.2f%%), total cost: $%.2f\n", 
		totalServCnt, avgUtil, avgEffUsage, grandTotal);
	if (numSJobs) {
		avgWT = totalWT / numSJobs;
		avgET = totalRT / numSJobs;
		avgTT = avgWT + avgET;
	}
	printf("# avg waiting time: %ld, avg exec time: %ld, avg turnaround time: %ld\n",
		avgWT, avgET, avgTT);
	
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
	int i, nsTypes = g_systemInfo.numServTypes;
	ServerTypeProp *sTypes = g_systemInfo.sTypes;
	Server **servers = g_systemInfo.servers;

	if (!sTypes) return;	// nothing to free
	if (servers) {
		for (i = 0; i < nsTypes; i++) {
			int j, limit = sTypes[i].limit;
			Server *server = servers[i];
			if (server) {
				for (j = 0; j < limit; j++) {
					Server *sInstance = &server[j];
					
					FreeSchedJobs(sInstance->waiting);
					FreeSchedJobs(sInstance->running);
					FreeSchedJobs(sInstance->failed);
					FreeSchedJobs(sInstance->suspended);
					FreeSchedJobs(sInstance->completed);
					FreeSchedJobs(sInstance->preempted);
					FreeSchedJobs(sInstance->killed);
				}
				if (server->failInfo.fIndices)
					free(server->failInfo.fIndices);
				if (server->failInfo.rIndices)
					free(server->failInfo.rIndices);

				free(servers[i]);
			}
		}
		free(servers);
		g_systemInfo.servers = NULL;
	}

	for (i = 0; i < nsTypes; i++)
		if (!sTypes[i].name)
			free(sTypes[i].name);
	free(sTypes);
	g_systemInfo.sTypes = NULL;
	if (g_systemInfo.rFailT) {
		free(g_systemInfo.rFailT->resFailures);
		free(g_systemInfo.rFailT);
	}
}

void FreeWorkloadInfo()
{
	int i;

	if (!g_workloadInfo.jobTypes) return;	// nothing to free
	if (g_workloadInfo.name)
		free(g_workloadInfo.name);

	for (i = 0; i < g_workloadInfo.numJobTypes; i++) {
		JobTypeProp *jType = &g_workloadInfo.jobTypes[i];
		free(jType->name);
	}
	free(g_workloadInfo.jobTypes);

	if (g_workloadInfo.jobGr)
		free(g_workloadInfo.jobGr);
	
	if (g_workloadInfo.jobs)
		free(g_workloadInfo.jobs);
}

void FreeAll()
{
	FreeSystemInfo();
	FreeWorkloadInfo();
	xmlCleanupParser();
}

