#include "main_node.h"

#ifdef BRPC_WITH_RDMA

//main 8003 data_node 8002
DEFINE_int32(port, 8003, "TCP Port of main server");
DEFINE_bool(use_rdma, true, "Use RDMA or not");

std::vector<std::string> data_node_ip_list;
std::vector<std::string> data_dir_path_list;


namespace nserver {
class MainnodeServiceImpl : public MainnodeService {
public:
    MainnodeServiceImpl() {}
    ~MainnodeServiceImpl() {}

    void RegisterDatanodeIp(google::protobuf::RpcController* cntl_base,
              const node_regRequest* request,
              node_regResponse* response,
              google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl =
                static_cast<brpc::Controller*>(cntl_base);

        butil::EndPoint mainserver_ep = cntl->local_side();
        butil::EndPoint datanode_ep = cntl->remote_side();

        std::string mainserver_ip(butil::ip2str(mainserver_ep.ip).c_str());
        std::string datanode_ip(butil::ip2str(datanode_ep.ip).c_str());


        LOG(INFO) << "mainserver ip: " << mainserver_ip
                  << " port: "         << mainserver_ep.port;
        LOG(INFO) << "datanode ip: " << datanode_ip
                  << " port: "       << datanode_ep.port;
        
        data_node_ip_list.emplace_back(datanode_ip);

        response->set_reg_succ(true);
        response->set_node_ip(datanode_ip);

    }
    void AddDirpath(google::protobuf::RpcController* cntl_base,
              const dir_pathRequest* request,
              dir_pathResponse* response,
              google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);        
        
        if(main_node::add_dir_path_to_redis(request->dir_path()) < 0)
        {
            response->set_add_succ(false);
        }
        else
        {
            data_dir_path_list.emplace_back(request->dir_path());
            response->set_add_succ(true);
        }
    }   
};
}


int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    // Initialize RDMA environment in advance.
    if (FLAGS_use_rdma) 
    {
        brpc::rdma::GlobalRdmaInitializeOrDie();
    }

    
    brpc::Server server;
    nserver::MainnodeServiceImpl datanode_service_impl;

    if (server.AddService(&datanode_service_impl, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service in main node server";
        return 0;
    }

    brpc::ServerOptions options;
    options.use_rdma = FLAGS_use_rdma;
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start transfileServer";
        return -1;
    }

    server.RunUntilAskedToQuit();
    return 0;
}

#else


int main(int argc, char* argv[]) {
    LOG(ERROR) << " brpc is not compiled with rdma. To enable it, please refer to https://github.com/apache/brpc/blob/master/docs/en/rdma.md";
    return 0;
}

#endif
