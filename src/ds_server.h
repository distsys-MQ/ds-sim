#ifndef DS_SIM_FUNC_H
#define DS_SIM_FUNC_H
// function prototypes
void PrintWelcomeMsg(int argc, char **argv);
void InitSim();
int Config(int argc, char **argv);
void EstablishConnection(int argc, char **argv);
int HandleHandshaking(char *prevMsg, char *msgRcvd, char *correctMsg, int arg);
void CreateResFailTrace();
inline void GracefulExit();
void CompleteRecvMsg(char *msg, int length);
void CompleteSendMsg(char *msg, int length);
int SendMsg(char *msg);
int RecvMsg(char *msgSent, char *msgRcvd);
inline int IsValidNum(char *buf);
int BreakPointMode(char *msgToSend, int nextEvent);
int IsCmd(char *cmdStr);
int InteractiveMode(char *msgSent, char *msgRcvd);
int GetIntValue(char *str, char *desc);
float GetFloatValue(char *str, char *desc);
inline int IsOutOfBound(int value, long int min, long int max, char *message);
int HandleCmdArgs(int argc, char **argv);
void Help(int opt);
void ShowUsage();
void ShowCmds();
void ShowLimits();
void ShowStates();
void SetSimTermCond();
void CreateResFailureLists();
void CompleteConfig();
void CreateServerTypes(int nsTypes);
int StoreServerType(xmlNode *node);
void ResizeServerTypes();
void SetServerRes(ServerRes *tgtSRes, ServerRes *srcSRes);
void InitServers(Server *servers, ServerTypeProp *sType);
void CreateServers();
void ResizeJobTypes();
void CreateJobTypes(int njTypes);
int StoreJobType(xmlNode *node);
int StoreWorkloadInfo(xmlNode *node);
int LoadSimConfig(xmlNode *node);
int ValidateJobRates();
int ReadSimConfig(char *filename);
void GenerateSystemInfo();
inline long int CalcTotalCoreCount();
int CmpCoreCnt(const void *p1, const void *p2);
long int CalcTotalCoreUsage(int submitTime, Job *jobs, int jID);
int CalcLocalCurLoad(int submitTime, Job *jobs, int jID);
inline int GetNextLoadDuration(enum LoadState lState);
inline int GetJobType();
int ReadJobs(char *jobFileName);
int LoadJobs(xmlNode *node);
int GetJobTypeByName(char *);
int FindPrevSmallerCoreCnt(int curCoreCnt, int startSTInx);
int ComputeJobCoreReq(ServerRes *maxCapa, int loadTooLow);
void GenerateJobResReq(Job *job, ServerRes *maxCapa, int curLoad, int targetLoad, int loadTooLow);
void GenerateJobRuntimes(Job *job, int jType, JobTypeProp *jobTypes);
void CalcJobGranu();
Job *GenerateJobs();
inline int GetNumLoadTransitions();
void GenerateWorkload();
int ValidateSystemInfo();
int ValidateWorkloadInfo();
int CompareCoreCnt(const void *p1, const void *p2);
void SortServersByResCapacity();
void WriteSystemInfo();
void WriteJobs();
void WriteResFailures();
void PrintMsg(int time);
int HandleREDY(char *msgRcvd, char *msgToSend);
int HandlePSHJ(char *msgRcvd, char *msgToSend);
int HandleGETS(char *msgRcvd, char *msgToSend);
int HandleSCHD(char *msgRcvd, char *msgToSend);
int HandleJCPL(char *msgRcvd, char *msgToSend);
int HandleLSTJ(char *msgRcvd, char *msgToSend);
int HandleLSTQ(char *msgRcvd, char *msgToSend);
int HandleCNTJ(char *msgRcvd, char *msgToSend);
int HandleMIGJ(char *msgRcvd, char *msgToSend);
int HandleEJWT(char *msgRcvd, char *msgToSend);
int HandleTERM(char *msgRcvd, char *msgToSend);
int HandleKILJ(char *msgRcvd, char *msgToSend);
int HandleENQJ(char *msgRcvd, char *msgToSend);
int HandleDEQJ(char *msgRcvd, char *msgToSend);
inline Server *GetServer(int type, int id);
int HandleKilledJobs(SchedJob *sJob, Server *server, int killedTime);
int HandleFailedJobs(SchedJob *sJob, Server *server, int failedTime);
int HandlePreemptedJobs(SchedJob *sJob, Server *server, int eventType, int eventTime);
void HandlePreemptedJob(SchedJob *sJob, int eventType, int eventTime);
inline int GetESTimeJob(SchedJob *sJob);
int CalcFailureMAD(unsigned short servType, unsigned short servID, ServerFailInfo *failInfo);
inline void InitServerRes(ServerRes *sRes);
void FailRes(ResFailure *resFail, int fInx);
void RecoverRes(ResFailure *resRecover, int rInx);
inline int GetNextResFailEventTime();
int CheckForFailures(int nextFET);
int RemoveWaitingJob(int jID);
WaitingJob *DisconnectWaitingJob(int jID);
WaitingJob *GetWaitingJob(int jID);
WaitingJob *GetFirstWaitingJob();
void InsertWaitingJob(WaitingJob *wJob);
void AddToWaitingJobList(Job *job, int submitTime);
void AddToHeadWaitingJobQ(Job *job, int submitTime);
int EnqueueJob(Job *job, int submitTime);
QueuedJob *DequeueJob(int qID);
int GetNextJobSubmitTime();
Job *GetNextJobToSched(int *submitTime);
SchedJob *DisconnectSchedJob(SchedJob *sJob, Server *server);
void AddSchedJobToEnd(SchedJob *sJob, SchedJob **sJobList);
void InsertSJobByEndTime(SchedJob *sJob, SchedJob **sJobList);
int UpdateServerUsage(SchedJob *sJob, Server *server);
void MoveSchedJobState(SchedJob *sJob, int jStateNew, Server *server);
void UpdateServerRes(ServerRes *sRes1, ServerRes *sRes2, int op);
void UpdateServerCapacity(Server *server, ServerRes *resReq, int op);
inline void SetSJobTimes(SchedJob *sJob, int startTime);
SchedJob *GetNextJobToRun(Server *server, int prevJobET);
int CmpLog(const void *p1, const void *p2);
int PrintLog();
void MergeSort(SchedJob **head, int (*lessThan)(const void *item1, const void *item2));
SchedJob *Divide(SchedJob *head);
SchedJob *Merge(SchedJob *head1, SchedJob *head2, int (*lessThan)(const void *item1, const void *item2));
long int GetEFTime();
SchedJobListNode *CreateSJLNode(SchedJob *sJob);
SchedJobListNode *AddSJobToList(SchedJob *sJob, SchedJobListNode *head);
SchedJobListNode *RemoveFJobFromList(SchedJobListNode *head);
void FreeSJList(SchedJobListNode *head);
SchedJobListNode *GetNextCmpltJobs(int eft);
int UpdateBootingServerState(Server *server);
int UpdateBootingServerStates();
void CompleteJob(SchedJob *sJob);
int RunReadyJobs(Server *server);
int MigrateJob(char *msg);
void UpdateRunningJobs(Server *server);
unsigned int UpdateServerStates();
int FindResTypeByName(char *name);
inline char *FindResTypeNameByType(int type);
inline int GetServerLimit(int type);
int GetServerAvailTime(int sType, int sID, ServerRes *resReq);
char *ConcatBatchMsg(char *dest, char *src);
int SendDataHeader(char *cmd, int nRecs);
inline int GetServerBootupTime(int type);
int SendResInfoAll(int oldRESC);
int SendResInfoByType(int type, ServerRes *resReq, int oldRESC);
int SendResInfoByServer(int type, int id, ServerRes *resReq, int oldRESC);
int SendResInfoByAvail(ServerRes *resReq, int oldRESC);
int SendResInfoByCapacity(ServerRes *resReq, int oldRESC);
int SendResInfoByBounded(ServerRes *resReq, int oldRESC);
void FinalizeBatchMsg();
int SendBatchMsg(char *cmd, int numMsgs);
int GetArgCount(const char *str);
int SendResInfo(char *msgRcvd);
int SendJobsPerStateOnServer(SchedJob *sJob);
int SendJobsOnServer(char *msgRcvd);
int GetQueuedJob(QueuedJob *qJob, int qID, char *msgToSend);
int GetQueuedJobs(QueuedJob *qJob);
int ListQueuedJobs(char *msgRcvd, char*msgToSend);
inline int CountJobs(SchedJob *sJob);
int SendJobCountOfServer(char *msgRcvd);
inline int GetTotalEstRT(SchedJob *sJob);
int SendEstWTOfServer(char *msgRcvd);
int BackFillJob(Job *job);
void SetServerToInactive(Server *server);
int TerminateServer(char *cmd);
SchedJob *FindSchedJobByState(int jID, enum JobState state, Server *server);
SchedJob *FindSchedJob(int jID, Server *server);
void PreemptJob(SchedJob *sJob, int eventType, int eventTime);
int KillJob(char *cmd);
int IsSchdInvalid(int jID, char *stName, int sID);
SchedJob **GetSchedJobList(int jState, Server *server);
int AssignToServer(SchedJob *sJob, Server *server, char *msg);
int IsSuffAvailRes(ServerRes *resReq, ServerRes *availCapa);
int IsSuffServerRes(ServerRes *resReq, Server *server);
int ScheduleJob(int jID, int sType, int sID, SchedJob *mSJob);
int CountWJobs();
void PrintUnscheduledJobs();
inline int ServerUsed(Server *server);
int GetLastServerTime(SchedJob *sJobs, int prevLTime);
int GetLatestTimeServerUsed(Server *server);
int CalcEfftvServerUsage(Server *server);
int PrintSchedJobs(Server *server, int jobState);
void PrintStats();
void FreeSystemInfo();
void FreeWorkloadInfo();
void FreeSchedJobs();
void FreeAll();

#endif
