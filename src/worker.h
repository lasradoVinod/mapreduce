#pragma once
#include <grpc++/grpc++.h>
#include <mr_task_factory.h>
#include "mr_tasks.h"
#include <grpc/support/log.h>
#include "mapreduce_spec.h"
#include "file_shard.h"
#include "masterworker.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerCompletionQueue;
using grpc::ServerAsyncResponseWriter;
using grpc::CompletionQueue;
using grpc::ClientAsyncResponseReader;
using masterworker::MapReduceComm;
using masterworker::Reply;
using masterworker::File;
using masterworker::WorkDesc;

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker{

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

	private:
	  class CallData{
	   public:
	    // Take in the "service" instance (in this case representing an asynchronous
	    // server) and the completion queue "cq" used for asynchronous communication
	    // with the gRPC runtime.
	    CallData(MapReduceComm::AsyncService* service, ServerCompletionQueue* cq)
	        :service_(service), cq_(cq), responder_(&ctx_), status_(CREATE)
      {
	      // Invoke the serving logic right away.
	      Proceed();
	    }

	    void Proceed();

	   private:
	    // The means of communication with the gRPC runtime for an asynchronous
	    // server.
	    MapReduceComm::AsyncService* service_;
	    // The producer-consumer queue where for asynchronous server notifications.
	    ServerCompletionQueue* cq_;
	    // Context for the rpc, allowing to tweak aspects of it such as the use
	    // of compression, authentication, as well as to send metadata back to the
	    // client.
	    ServerContext ctx_;

	    // What we get from the client.
	    WorkDesc request_;
	    // What we send back to the client.
	    Reply reply_;

	    // The means to get back to the client.
	    ServerAsyncResponseWriter<Reply> responder_;

	    // Let's implement a tiny state machine with the following states.
	    enum CallStatus { CREATE, PROCESS, FINISH };
	    CallStatus status_;  // The current serving state.
	  };
    void HandleRequests();
    std::vector<std::string> ip_addrresses_;
    MapReduceComm::AsyncService service_;
    std::unique_ptr<ServerCompletionQueue> cq_;
    std::unique_ptr<Server> server_;
    ServerContext ctx_;
};

