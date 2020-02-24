
## Overview
ds-sim is a discrete-event simulator that has been developed primarily for leveraging scheduling algorithm design. It adopts a minimalist design explicitly taking into account modularity in that it uses the client-server model. The client-side simulator acts as a job scheduler while the server-side simulator simulates everything else including users (job submissions) and servers (job execution).

---
## How to run a simulation
1. run server `$ ds-server [-c configuration] [-v brief|all]`
2. run client `$ ds-client [-a algorithm]`



---
## Simulation protocol
ds-sim protocol is designed to be simple like SMTP.
![ds-sim flow chart.](images/ds-sim-protocol.png)

1. C(lient): sends "HELO" to S(erver)
2. S: replies with "OK" (e.g., 250 in SMTP)
3. C: sends “AUTH” with authentication information "AUTH xxx" to S
4. S: replies with “OK” after printing out some welcome messages and writing system info (system.xml)
* client reads system.xml
5. C: sends "REDY" to S
6. S: sends “JOBN” for scheduling or “NONE” when there are no more jobs to schedule
7. C: sends one of the following scheduling actions for “JOBN” or go to Step 12 for “NONE”
- RESC: the request for resource (availability) information - SCHD: the actual scheduling decision , or - LSTJ: the request for running and waiting jobs on a particular server
8. S: sends one of the following corresponding messages to C
- DATA (for RESC and LSTJ): the indicator for the request information will be sent in the successive messages
- OK (for SCHD): scheduling is successfully done,
- ERR (for any of scheduling actions): invalid request; invalid scheduling decision (in SCHD message) or invalid resource/job information request (in RESC/LSTJ message); for example, an invalid server ID or an invalid job ID has been used, or
9. C: takes one of the following actions upon receiving a message from S
- send “OK” for “DATA”
- go to Step 5 for "OK", or
- go to Step 7 or Step 10 for "ERR"
10. S: sends one of the following
- resource information
- information of jobs on a server, or
- “.” if no more information to send
11. C: takes one of the following actions
- reads the resource/jobs information, sends “OK” to S and go to Step 10, or
- go to Step 7 for “.”
12. C: sends “QUIT” to S
13. S: sends “QUIT” to C and exits
14. C: exits

**Command list**

| Cmd     				| Description   					| Format|
| ------------- 		|:-------------:					| -----:|
| HELO		      		| Initial message from client		| HELO |
| OK		     		| Response to a valid command 		|   OK |
| AUTH 					| Authentication information		| AUTH username|
| REDY 					| Client signals server for a job	| REDY|
|JOBN					| Job submission information		| JOBN submit_time (int) job_ID (int) estimated_runtime (int) #CPU_cores (int) memory (int) disk (int)
|JOBF					| Job resubmission after failure	| JOBF submit_time (int) job_ID (int) estimated_runtime (int) #CPU_cores (int) memory (int) disk (int)
|RESF					| Resource failure notification		| RESF server_type server_ID failure_start_time
|RESR					| Resource failure recovery 		| RESR server_type server_ID recovery_time
|RESC					| Resource information request: - RESC All: the information of all servers, in the system, regardless of their state. - RESC Type: the information of all servers of a particular server type. - RESC Avail: the information of servers that are available (i.e., inactive, booting and idle) for the job with sufficient resources.| RESC All Type server_type (char *) Avail #CPU_cores (int) memory (int) disk (int)
|LSTJ 					|Job list of a server				| LSTJ server_type (char *) server_ID (int)
|DATA |The indicator for the actual information to be sent |DATA
|ERR |Invalid message received |ERR 
|SCHD |Scheduling decision |SCHD job_ID (int) server_type (char *) server_ID (int) 
|QUIT |Simulation termination |QUIT

* The unit of time is second.
* The unit of memory/disk is MB.
* Data types (e.g., int and char *) are of C programming language and based on x64.
* Commands in blue are used by client, those in green by server and those in orange by both. 
* When the client-side simulator sends a RESC command, the format of actual data (server information) after the DATA message is as follows: server_type server_id_of_that_type server_state available_time #CPU_cores memory disk 

* A server state value of 0 indicates the server is ‘Inactive’ (e.g., switched off); see below for all state values. 
* Server available time: if the server is currently ‘Inactive’ and it is to be used, it has to be on (‘Idle’ once it's on) and the booting takes 60 seconds as specified in the configuration file (“bootupTime=60” in ds-config1.xml); hence, its available time is t(ime) 270 as the current simulation time is 210 based on the submission time of job 0 (JOBN 210 0 385 1 100 800). If -1 is returned, either the availability of server is unknown (due primarily to some running jobs) or the server is unavailable (e.g., failed).
* To use an ‘Inactive’ server, it has to boot; There is no specific command to boot a server. Simply, you schedule a job to an Inactive server (SCHD); the server is then available after a certain amount of booting time, e.g., 60 seconds. 
* The format of message sent in response to LSTJ is as follows: job_id job_state job_start_time job_estimated_runtime #cores memeory disk

---
## Simulation options and predefined literals and limits
**Server-side simulator options (e.g., server -c ds-config1.xml)**

* h: show usage
- v [all]: verbose
- j #jobs
- d duration_in_seconds
- s random_seed
- n: newline added to the end of message
- r resource_limit
- c config_file
* If the "-v all" option is used, all detailed simulation messages will be printed out

**Server state**

Inactive = 0, Booting = 1, Idle = 2, Active = 3, Unavailable = 4
* Active state means the server is on and currently running one or more jobs; it can be interpreted as ‘Busy’ state.

**Job state**

Submitted = 0, Waiting = 1, Running = 2, Suspended = 3, Completed = 4, Failed = 5, Killed = 6

**Limits (based on x64)**

* Job count: [1, SHRT_MAX]
- Job run time: [1, 604800] // 7 days = 60 seconds * 60 minutes * 24 hours * 7 days
- Job population rate: [1, 100]
- Simulation end time” [1, 2592000] // 30 days in seconds
- The number of server types: [1, 100]
- Resource limit (#servers/type): [1, 1000]
- Server bootup time: [0, 600]
- Random seed limit: [1, UINT_MAX]
- Core count limit: [1, 256]
- Memory limit: [1000, 8192000]
- Disk limit: [1000, 8192000]
- Min load limit: [1, 99]
- Max load limit: [1, 100]
- Time limit of load state: [3600, 43200] for alternating loads (i.e., [1 hour, 12 hours])

** The unit of time is second.

** The unit of memory/disk is MB.

** The unit of job population rate, load (min and max) is percentage.


