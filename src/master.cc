/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
#include "master.h"
#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include <chrono>
#include <sys/time.h>

 WorkerClient::WorkerClient(std::string ipAddress_)
 {
   call = NULL;
   ipAddress = ipAddress_;
   channel = grpc::CreateChannel(ipAddress , grpc::InsecureChannelCredentials());
   stub_ = MapReduceComm::NewStub(channel);
   busy = false;
 }

 void WorkerClient::getWorkDone(bool mapRED_,
                               FileShard fShard_,
                               uint32_t numOpFiles,
                               uint32_t worker_id,
                               std::string user_id,
                               std::string out_dir
                               )
 {
   call = new AsyncClientCall;
   WorkDesc wDesc;
   wDesc.set_mapred(mapRED_);
   wDesc.set_worker_id(worker_id);
   wDesc.set_user_id(user_id);
   wDesc.set_numopfiles(numOpFiles);
   wDesc.set_out_dir(out_dir);

   std::vector<PerFileInfo>::iterator it;

   for (it = fShard_.fileInfo.begin(); it != fShard_.fileInfo.end() ; it++)
   {
     File * file = wDesc.add_file();
     file->set_filename((*it).fileName);
     file->set_size((*it).length);
     file->set_startpos((*it).startPos);
   }
   call->response_reader = stub_->AsyncdoWork(&(call->context), wDesc, &cq);
   call->response_reader->Finish(&(call->reply), &(call->status), (void*)1);
   busy = true;
}

 bool WorkerClient::getStatus(std::vector<std::string>& fileOutput)
 {
   uint32_t sizeVector;
   void* got_tag = NULL;
   bool ok = false;
   
   std::chrono::duration<int,std::ratio<1> > one_second (1);
   std::chrono::system_clock::time_point timeTo = std::chrono::system_clock::now() + one_second;

   GPR_ASSERT(cq.AsyncNext <std::chrono::system_clock::time_point>
               (&got_tag, &ok, timeTo));

  // Verify that the result from "cq" corresponds, by its tag, our previous
  // request.
  if (got_tag == NULL)
    return false;
  // ... and that the request was completed successfully. Note that "ok"
  // corresponds solely to the request for updates introduced by Finish().
  GPR_ASSERT(ok);

  // Act upon the status of the actual RPC.
  if (call->status.ok())
  {
    sizeVector = call->reply.file_size();
    for (uint32_t i = 0;i< sizeVector;i++)
    {
      fileOutput.push_back(call->reply.file(i));
    }
  }

  if (!call->status.ok())
  {
    std::cout << call->status.error_code() << ": " << call->status.error_message() << std::endl;
    delete call;
    return false;
  }

  delete call;
  busy = false;
  return true;
 }

Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards)
: mapReduceSpec(mr_spec) , fileShards(file_shards)
{
	uint32_t iter;
	workerId = 0;

	for (iter = 0;iter<mapReduceSpec.numWorkers;iter++)
	{
	  freeWorkers.push_back(iter);
		WorkerInfo tempInfo = {
		  .state = enStateReachable,
			.wClass = enWorkNone,
			.ipAddress = mapReduceSpec.ipAddresses[iter]
		};
		workerInfo.push_back(tempInfo);
		workerCommInterface.push_back(new WorkerClient(mapReduceSpec.ipAddresses[iter]));
	}
	numWorkersBusy = 0;
}

void Master::AssignWork(bool mapRED_,
    FileShard fShard_,
    uint32_t numOpFiles,
    std::string user_id,
    std::string out_dir)
{

  timeval time;
  gettimeofday(&time,0);
  WorkTime work = { mapRED_,fShard_,numOpFiles, user_id,out_dir,time};

  if (freeWorkers.size())
  {
    uint32_t worker = freeWorkers.front();
    freeWorkers.pop_front();
    workerTaskMap[worker] = work;
    workerCommInterface[worker]->getWorkDone(work.mapRed,work.fShard,work.numOpFiles,workerId++,work.user_id,work.out_dir);
  }
  else
  {
    unAssigned.push_front(work);
    //std::cout << unAssigned.size() << std::endl;
  }
}

bool Master::completedWork(std::vector<std::string> & output)
{

  //std::cout << unAssigned.size() << std::endl;
  for (uint32_t i=0;i<mapReduceSpec.numWorkers;i++)
  {
    if (workerTaskMap.find(i) == workerTaskMap.end() || workerInfo[i].state == enStateNotReachable)
      continue;

    bool ret = workerCommInterface[i]->getStatus(output);
    if (ret == true)
    {
      workerTaskMap.erase(i);
      if (unAssigned.size())
      {
        WorkTime work = unAssigned.front();
        unAssigned.pop_front();
        workerTaskMap[i] = work;
        workerCommInterface[i]->getWorkDone(work.mapRed,work.fShard,work.numOpFiles,workerId++,work.user_id,work.out_dir);
      }
      else
      {
        freeWorkers.push_back(i);
      }
    }
    else
    {
      timeval currTime;
      timeval res;
      gettimeofday(&currTime,0);
      timersub(&currTime,&(workerTaskMap[i].timeStarted),&res);
      if (res.tv_sec > 2)
      {
        WorkTime work = workerTaskMap[i];
        workerTaskMap.erase(i);
        workerInfo[i].state = enStateNotReachable;
        AssignWork(work.mapRed,work.fShard,work.numOpFiles,work.user_id,work.out_dir);
      }
    }
  }

  if (workerTaskMap.size())
    return false;
  workerId = 0;
  return true;
}
/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
  std::vector<std::string> MapOutput;
  std::vector<std::string> ReduceOutput;
  std::vector<FileShard> ReduceInput;
  std::map <uint32_t,WorkTime> workerTaskMap;
  uint32_t iter;
  uint32_t numWorkers = mapReduceSpec.numWorkers;

  for (iter = 0; iter < fileShards.size();iter++ )
  {
      AssignWork(MAP, fileShards[iter], mapReduceSpec.numOutputFiles, mapReduceSpec.user_id,mapReduceSpec.outputDirName);
  }

  while(!completedWork(MapOutput));


  /*Creating work for reducers*/
  ReduceInput.resize(mapReduceSpec.numOutputFiles);
  for (iter = 0; iter<MapOutput.size(); iter++ )
  {
    PerFileInfo tempFileInfo;
    tempFileInfo.fileName = MapOutput[iter];
    ReduceInput[iter%(mapReduceSpec.numOutputFiles)].fileInfo.push_back(tempFileInfo);
  }

  for (iter = 0; iter < ReduceInput.size(); iter++)
  {
    AssignWork(REDUCE,ReduceInput[iter],1, mapReduceSpec.user_id,mapReduceSpec.outputDirName);
  }

  while(!completedWork(ReduceOutput));

  return true;
}
