#include "data_node.h"

#ifdef BRPC_WITH_RDMA


//main 8003 data_node 8002
DEFINE_int32(port, 8002, "TCP Port of this data node server");
DEFINE_bool(use_rdma, true, "Use RDMA or not");
DEFINE_string(main_server, "172.31.50.187:8003", "IP Address of server");

std::unordered_map<std::string, npcbuf_data> data_map;
std::unordered_map<std::string, std::vector<std::string>> path_date_list;

static void MockFree(void* buf) { }

namespace nserver {
class DatanodeServiceImpl : public DatanodeService {
public:
    DatanodeServiceImpl() {}
    ~DatanodeServiceImpl() {}

    void TransFile(google::protobuf::RpcController* cntl_base,
              const fileRequest* request,
              fileResponse* response,
              google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl =
                static_cast<brpc::Controller*>(cntl_base);
        
        if(data_map.find(request->path_date()) != data_map.end())
        {
            response->set_trans_succ(true);
            npcbuf_data &resp_data = data_map[request->path_date()];
            cntl->response_attachment().append_user_data_with_meta(
                resp_data.buf,
                resp_data.data_size,
                MockFree,
                resp_data.data_lkey
            );
            LOG(INFO) << "Received request path_date:" <<request->path_date() 
                      << " and find success";
        }
        else
        {
            response->set_trans_succ(false);
            LOG(INFO) << "Received request path_date:" <<request->path_date() 
                      << " and find failed";
        }
        
    }
    void LoadDate(google::protobuf::RpcController* cntl_base,
              const loadDateRequest* request,
              loadDateResponse* response,
              google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl =
                static_cast<brpc::Controller*>(cntl_base);
        
        if(request->load_data())
        {
            if(data_node::read_data(request->dir_path(), data_map, 
                path_date_list[request->dir_path()]) < 0) 
            {
                LOG(ERROR) << "Fail to read data";
                return;
            }
            for(const auto &path_date: path_date_list[request->dir_path()])
            {
                response->add_path_date_list(path_date);
            }
            LOG(INFO) << "datanode server add path_date succ ,total: " << response->path_date_list_size();
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
    
    if(data_node::reg_to_main_server(FLAGS_main_server) < 0)
    {
        LOG(ERROR) << "Fail to register data node ip to main server";
    }

    brpc::Server server;
    nserver::DatanodeServiceImpl datanode_service_impl;

    if (server.AddService(&datanode_service_impl, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service in data node server";
        return 0;
    }

    brpc::ServerOptions options;
    options.use_rdma = FLAGS_use_rdma;
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start transfileServer";
        return -1;
    }

    server.RunUntilAskedToQuit();

    data_node::clear_data(data_map);
    return 0;
}

#else


int main(int argc, char* argv[]) {
    LOG(ERROR) << " brpc is not compiled with rdma. To enable it, please refer to https://github.com/apache/brpc/blob/master/docs/en/rdma.md";
    return 0;
}

#endif
