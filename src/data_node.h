#pragma once

#include <fstream>
#include <filesystem>
#include <algorithm>

#include <gflags/gflags.h>
#include "brpc/channel.h"
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
#include "nserver.pb.h"

struct npcbuf_data
{
    char* buf;
    int data_size;
    uint32_t data_lkey;
    npcbuf_data() : buf(nullptr), data_size(0), data_lkey(0) {}
};

namespace data_node
{
    //register the datanode ip to the main server
    int reg_to_main_server(const std::string main_server_addr);

    //read data from the local file system
    int read_data(const std::string &dir_path, 
                  std::unordered_map<std::string, npcbuf_data> &data_map,
                  std::vector<std::string> &path_datetime_list);
    //Recycling data
    void clear_data(std::unordered_map<std::string, npcbuf_data> &data_map);
}