#include "data_node.h"

int data_node::reg_to_main_server(const std::string main_server_addr)
{
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.use_rdma = true;
    options.protocol = "baidu_std";
    options.connection_type = "single";
    options.timeout_ms = 2000;
    options.max_retry = 0;

    if (channel.Init(main_server_addr.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }

    nserver::MainnodeService_Stub stub(&channel);

    nserver::node_regRequest req;
    nserver::node_regResponse resp;
    brpc::Controller cntl;

    stub.RegisterDatanodeIp(&cntl, &req, &resp, nullptr);

    if (!cntl.Failed())
    {
        LOG(INFO) << "reg_to_main_server succ, local ip: " << resp.node_ip();
    }
    else
    {
        LOG(WARNING) << cntl.ErrorText();
        return -1;
    }

    return 0;
}

//read data from the local file system
int data_node::read_data(const std::string &dir_path, 
                  std::unordered_map<std::string, npcbuf_data> &data_map,
                  std::vector<std::string> &path_datetime_list)
{

    namespace fs = std::filesystem;

    int file_count = 0;

    if (fs::is_directory(dir_path))
    {
        for (const auto &entry : fs::directory_iterator(dir_path))
        {
            if (entry.is_regular_file())
            {
                std::string filename = entry.path().filename().string();
                if (fs::path(filename).extension() == ".npcbuf")
                {
                    size_t dot_index = filename.rfind(".");
                    if (dot_index != std::string::npos)
                    {
                        std::string datetime = filename.substr(0, dot_index);
                        path_datetime_list.emplace_back(datetime);
                        file_count++;
                    }
                }
            }
        }
    }
    else
    {
        std::cerr << "dir_path is not a directory" << std::endl;
        return -1;
    }

    // vector pre reserve reduce the expand move time,
    
    for (const std::string &datetime : path_datetime_list)
    {
        std::string path_date = dir_path + "+" + datetime;
        npcbuf_data &data_item = data_map[path_date];

        // read npcbuf file
        std::string npcbuf_file_path = dir_path + "/" + datetime + ".npcbuf";
        std::ifstream npcbuf_file(npcbuf_file_path, std::ios::binary);
        if (!npcbuf_file)
        {
            std::cerr << "can not open npcbuf file path:" << npcbuf_file_path << std::endl;
            return -1;
        }
        
        // Get the length of the file
        npcbuf_file.seekg(0, std::ios::end);
        int file_size = (int)npcbuf_file.tellg();
        npcbuf_file.seekg(0, std::ios::beg);

        // Allocate memory
        //char* npcbuf_data = (char*)malloc(file_size);
        data_item.buf = (char*)malloc(file_size);
        data_item.data_size = file_size;

        // Read file content into allocated memory
        npcbuf_file.read(data_item.buf, file_size);
        if (!npcbuf_file) 
        {
            std::cerr << "Error reading npcbuf file." << std::endl;
            free(data_item.buf); // Release allocated memory in case of an error
            return -1;
        }

        uint32_t lkey = brpc::rdma::RegisterMemoryForRdma(data_item.buf, file_size);
        if(lkey == 0)
        {
            std::cerr << "rdma::registerMemoryForRdma" << std::endl;
            free(data_item.buf); 
            return -1;
        }

        // npcbuf_data_list.emplace_back(npcbuf_data);
        // npcbuf_data_size_list.emplace_back(file_size);
        // data_lkey_list.emplace_back(lkey);

        npcbuf_file.close();
    }
    return 0;
}

void data_node::clear_data(std::unordered_map<std::string, npcbuf_data> &data_map)
{
    //deregister for rdma and free malloced memory
    for(auto &pr: data_map)
    {
        npcbuf_data &data = pr.second;
        if(!data.buf)
        {
            brpc::rdma::DeregisterMemoryForRdma(data.buf);
            free(data.buf);
        }
    }

    //recycle stl memory
    std::unordered_map<std::string, npcbuf_data> empty_data_map;
    std::swap(empty_data_map, data_map);
}


