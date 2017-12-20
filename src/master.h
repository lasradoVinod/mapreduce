#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"
#include "masterworker.grpc.pb.h"
#include <memory>
#include <list>

using grpc::ServerCompletionQueue;
using grpc::ServerAsyncResponseWriter;
using grpc::CompletionQueue;
using grpc::ClientAsyncResponseReader;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using masterworker::MapReduceComm;
using masterworker::Reply;
using masterworker::File;
using masterworker::WorkDesc;

#define MAP     false
#define REDUCE  true

typedef struct __WorkTime
{
  bool mapRed;
  FileShard fShard;
  uint32_t numOpFiles;
  std::string user_id;
  std::string out_dir;
  timeval timeStarted;
}WorkTime;


// struct for keeping state and data information
struct AsyncClientCall {
  // Container for the data we expect from the server.
  Reply reply;

  // Context for the client. It could be used to convey extra information to
  // the server and/or tweak certain RPC behaviors.
  ClientContext context;

  // Storage for the status of the RPC upon completion.
  Status status;

  std::unique_ptr<ClientAsyncResponseReader<Reply>> response_reader;
};

typedef enum __WorkerClass
{
	enWorkMap,
	enWorkReduce,
	enWorkNone,
}WorkerClass;

typedef enum __WorkerState
{
  enStateReachable,
  enStateNotReachable
}WorkerState;

typedef struct __WorkerInfo
{
  WorkerState state;
  WorkerClass wClass;
	std::string ipAddress;
	std::vector<FileShard> AssignedWork;
}WorkerInfo;

class WorkerClient
{
public:
 WorkerClient(std::string ipAddress);
 void getWorkDone(bool mapRED_, FileShard fShard_, uint32_t numOpFiles, uint32_t worker_id, std::string user_id,std::string out_dir);
 bool getStatus(std::vector<std::string> & fileOutput);
 bool busy;
private:
 std::string ipAddress;
 std::shared_ptr<Channel> channel;
 std::unique_ptr<MapReduceComm::Stub> stub_;
 CompletionQueue cq;
 AsyncClientCall * call;
};

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature*/
		bool run();
		void AssignWork(bool mapRED_, FileShard fShard_, uint32_t numOpFiles, std::string user_id,std::string out_dir);
		bool completedWork(std::vector<std::string> &);
	private:
		std::list<WorkTime> unAssigned;
		std::list <uint32_t> freeWorkers;
		uint32_t workerId;
		std::map <uint32_t,WorkTime> workerTaskMap;
		const MapReduceSpec& mapReduceSpec;
		std::vector<FileShard> fileShards;
		std::vector<WorkerClient*> workerCommInterface;
		std::vector<WorkerInfo> workerInfo;
		uint32_t numWorkersBusy;
};


