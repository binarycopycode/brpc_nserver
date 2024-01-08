#pragma once

#include <fstream>
#include <filesystem>
#include <algorithm>

#include <gflags/gflags.h>
#include "butil/atomicops.h"
#include "butil/logging.h"
#include "butil/time.h"
#include "butil/endpoint.h" 
#include "butil/iobuf.h"
#include "brpc/server.h"
#include "bvar/variable.h"
#include "brpc/rdma/block_pool.h"
#include "brpc/rdma/rdma_endpoint.h"
#include "brpc/rdma/rdma_helper.h"
#include "hiredis/hiredis.h"
#include "nserver.pb.h"

namespace main_node
{
    static std::atomic<bool> stopRequested(false);

    int is_data_node_online_in_redis(int node_index);
    
    int set_data_node_state_in_redis(int node_index ,int node_state);
    //path_date : string(path+date)
    int add_path_date_to_redis(const std::string &ip_addr, const std::vector<std::string> &path_date_list);
    
    int add_dir_path_to_redis(const std::string &dir_path);
    
    int del_ip_in_redis(const std::string &ip_addr);

    //recover data infomation from redis when restart mainserver 
    int recover_data_info(std::vector<std::string> &data_node_ip_list,
                          std::vector<std::string> &data_dir_path_list);

}