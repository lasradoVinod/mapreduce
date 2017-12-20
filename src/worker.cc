/*
 * worker.cc
 *
 *  Created on: Nov 14, 2017
 *      Author: doniv
 */


#include <grpc++/grpc++.h>
#include <mr_task_factory.h>
#include "mr_tasks.h"
#include <grpc/support/log.h>
#include "mapreduce_spec.h"
#include "file_shard.h"
#include "masterworker.grpc.pb.h"
#include "worker.h"
#include <unistd.h>
#include <istream>

using namespace std;
/* CS6210_TASK: ip_addr_port is the only information you get when started.
  You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port)
{
  ServerBuilder builder;
  builder.AddListeningPort(ip_addr_port, grpc::InsecureServerCredentials());
  builder.RegisterService(&service_);
  cq_ = builder.AddCompletionQueue();
  server_ = builder.BuildAndStart();
  std::cout << "Server listening on " << ip_addr_port << std::endl;
}

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks
  from Master, complete when given one and again keep looking for the next one.
  Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and
  BaseReduer's member BaseReducerInternal impl_ directly,
  so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
  /*  Below 5 lines are just examples of how you will call map and reduce
    Remove them once you start writing your own logic */
/*  std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
  auto mapper = get_mapper_from_task_factory("cs6210");
  mapper->map("I m just a 'dummy', a \"dummy line\"");
  auto reducer = get_reducer_from_task_factory("cs6210");
  reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
  return true;
*/

      new CallData(&service_, cq_.get());
      void* tag;  // uniquely identifies a request.
      bool ok;
      while (true) {
        // Block waiting to read the next event from the completion queue. The
        // event is uniquely identified by its tag, which in this case is the
        // memory address of a CallData instance.
        // The return value of Next should always be checked. This return value
        // tells us whether there is any kind of event or cq_ is shutting down.
        GPR_ASSERT(cq_->Next(&tag, &ok));
        GPR_ASSERT(ok);
        static_cast<CallData*>(tag)->Proceed();
      //  tpool.schedule_work(tag);
      }
}

void Worker::CallData::Proceed() {
  if (status_ == CREATE) {
    status_ = PROCESS;
    //std::cout << "Proceed:: CREATE" << std::endl;
    service_->RequestdoWork(&ctx_, &request_, &responder_, cq_, cq_,this);

  } else if (status_ == PROCESS) {
    // Spawn a new CallData instance to serve new clients while we process
    // the one for this CallData. The instance will deallocate itself as
    // part of its FINISH state.
    new CallData(service_, cq_);
    /******* Handling Requests ********/
    // Get fields from request
    auto is_map_request = !request_.mapred();
    auto num_out_files = request_.numopfiles();
    const std::string& user_id = request_.user_id(); // TODO Add this to proto
    auto worker_id = request_.worker_id(); // TODO Add this to proto
   
    // Get the base mapper object for the requesting user
    std::shared_ptr<BaseMapper> mapper = get_mapper_from_task_factory(user_id);
    std::shared_ptr<BaseReducer> reducer = get_reducer_from_task_factory(user_id);
    auto dir_name = request_.out_dir();
    // Handle Mapping requests
    if (is_map_request){
      for (int j=0; j < request_.file_size(); j++) {
         auto _file_ = request_.file(j);
         auto fileName = _file_.filename();
         auto startPos = _file_.startpos();
         auto fileSize = _file_.size();

         unsigned long read_count = 0;
         // Open file for reading
         ifstream f (fileName);

         if (f.is_open()){
            std::string line;
            f.seekg(startPos, ios::beg);
            std::getline(f, line);
            read_count += line.size();
            while (f.tellg() <= startPos + fileSize  ){
            ///  std::cout << f.tellg() << " " <<  read_count << std::endl;
              // setup emit to produce the correct intermediate file
               mapper->impl_->set_out_file_prefix("./" + dir_name + "/" + std::string("map_inter_") + std::to_string(worker_id));
               mapper->impl_->set_max_out_files(num_out_files); // TODO FIXME
               mapper->map(line); 
               if (f.tellg() == -1)
                 break;
               std::getline(f, line);
               read_count += line.size();
            }
            f.close();
         } else {
            printf("Worker.cc: Error while Mapping. Can't open file %s. Aborting!\n", fileName.c_str());
            exit(0);
         }
      }
      std::vector<std::string> list = mapper->impl_->get_out_file_list();
      for (auto it = list.begin(); it != list.end(); it++){
         char* cwd;
         char buff[PATH_MAX + 1];
         cwd = getcwd( buff, PATH_MAX + 1 );
         reply_.add_file(*it);
      }
    }
    if (!is_map_request){
      std::map<std::string, std::vector<std::string> > reducer_input_map;
      for (int j=0; j < request_.file_size(); j++){
         auto _file_ = request_.file(j);
         auto fileName = _file_.filename();
         auto startPos = _file_.startpos();
         auto fileSize = _file_.size();
         unsigned long read_count = 0;
         // Open file for reading
         ifstream f (fileName);
         if (f.is_open()){
            std::string line;
            while (std::getline(f, line)){
               // setup emit to produce the output
               reducer->impl_->set_out_file_prefix("./" + dir_name + "/" +std::string("out_") + std::to_string(worker_id));
               reducer->impl_->set_max_out_files(num_out_files); // TODO FIXME
               int jj = 0;
               for(jj=0; jj<line.size(); jj++){
                  if (line[jj] == ' ') break;
               }
               std::string reducer_key = line.substr(0, jj);
               std::string reducer_value = line.substr(jj+1, line.size()-jj-1);
               reducer_input_map[reducer_key].push_back(reducer_value);
               //reducer->reduce(line); 
               read_count += line.size();
            }
            f.close();
         } else {
            printf("Worker.cc: Error while Mapping. Can't open file %s. Aborting!\n", fileName.c_str());
            exit(0);
         }
      }
      // Invoke reducer for every key
      for(auto itr = reducer_input_map.begin(); itr != reducer_input_map.end(); itr++){
         reducer->reduce(itr->first, itr->second);
      }
      std::vector<std::string> list = reducer->impl_->get_out_file_list();
      for (auto it = list.begin(); it != list.end(); it++){
         reply_.add_file(*it);
      }
     // std::cout << "Reduce Done" << std::endl;
    }

    //reply_.add_file("TestFile");
    /*******/


    status_ = FINISH;
    responder_.Finish(reply_, Status::OK, this);
  } else {
    //std::cout << "Proceed:: FINISH" << std::endl;
    GPR_ASSERT(status_ == FINISH);
    // Once in the FINISH state, deallocate ourselves (CallData).
    delete this;
  }
}
