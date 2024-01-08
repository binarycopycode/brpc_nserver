#include "main_node.h"

//------------------------redis part ---------------------------------------------

//default redisConnect("127.0.0.1", 6379)
redisContext* get_redis_context()
{
    redisContext *redis_ctx_ptr = redisConnect("127.0.0.1", 6379);
    return redis_ctx_ptr;
}

int main_node::is_data_node_online_in_redis(int node_index)
{
    const char *key = "data_node_status";
    redisContext *redis_ctx_ptr = get_redis_context();
    if (redis_ctx_ptr == nullptr || redis_ctx_ptr->err)
    {
        if (redis_ctx_ptr)
        {
            std::cerr << "redis connection error" << redis_ctx_ptr->err << std::endl;
            redisFree(redis_ctx_ptr);
        }
        else
        {
            std::cerr << "Connection error: can't allocate redis context" << std::endl;
        }
        return -1;
    }
    redisReply *reply = (redisReply *)redisCommand(redis_ctx_ptr, "GETBIT %s %d", key, node_index);
    int redis_status = reply->integer;
    freeReplyObject(reply);
    redisFree(redis_ctx_ptr);
    return redis_status;
}

int main_node::set_data_node_state_in_redis(int node_index, int node_state)
{
    const char *key = "data_node_status";
    redisContext *redis_ctx_ptr = get_redis_context();
    if (redis_ctx_ptr == nullptr || redis_ctx_ptr->err)
    {
        if (redis_ctx_ptr)
        {
            std::cerr << "redis connection error" << redis_ctx_ptr->err << std::endl;
            redisFree(redis_ctx_ptr);
        }
        else
        {
            std::cerr << "Connection error: can't allocate redis context" << std::endl;
        }
        return -1;
    }

    redisReply *reply = (redisReply *)redisCommand(redis_ctx_ptr, "SETBIT %s %d %d", key, node_index, node_state);
    freeReplyObject(reply);
    redisFree(redis_ctx_ptr);
    return 0;
}

int main_node::add_path_date_to_redis(const std::string &ip_addr, const std::vector<std::string> &path_date_list)
{
    redisContext *redis_ctx_ptr = get_redis_context();
    if (redis_ctx_ptr == nullptr || redis_ctx_ptr->err)
    {
        if (redis_ctx_ptr)
        {
            std::cerr << "redis connection error" << redis_ctx_ptr->err << std::endl;
            redisFree(redis_ctx_ptr);
        }
        else
        {
            std::cerr << "Connection error: can't allocate redis context" << std::endl;
        }
        return -1;
    }

    redisReply *reply;
    for (auto &path_date : path_date_list)
    {
        reply = (redisReply *)redisCommand(redis_ctx_ptr, "SADD %s %s", ip_addr.c_str(), path_date.c_str());
        freeReplyObject(reply);
        reply = (redisReply *)redisCommand(redis_ctx_ptr, "SADD %s %s", path_date.c_str(), ip_addr.c_str());
        freeReplyObject(reply);
    }
    redisFree(redis_ctx_ptr);
    std::cout << "success add all date from ip:" << ip_addr << " in redis" << std::endl;
    return 0;
}

int main_node::add_dir_path_to_redis(const std::string &dir_path)
{
    redisContext *redis_ctx_ptr = get_redis_context();
    if (redis_ctx_ptr == nullptr || redis_ctx_ptr->err)
    {
        if (redis_ctx_ptr)
        {
            std::cerr << "redis connection error" << redis_ctx_ptr->err << std::endl;
            redisFree(redis_ctx_ptr);
        }
        else
        {
            std::cerr << "Connection error: can't allocate redis context" << std::endl;
        }
        return -1;
    } 

    redisReply *reply;
    reply = (redisReply *)redisCommand(redis_ctx_ptr, "SADD dir_path_list %s", dir_path.c_str());
    freeReplyObject(reply);
    redisFree(redis_ctx_ptr);
    std::cout << "success add dir path" << dir_path << " to redis" << std::endl;
    return 0;
}

int main_node::del_ip_in_redis(const std::string &ip_addr)
{
    redisContext *redis_ctx_ptr = get_redis_context();
    if (redis_ctx_ptr == nullptr || redis_ctx_ptr->err)
    {
        if (redis_ctx_ptr)
        {
            std::cerr << "redis connection error" << redis_ctx_ptr->err << std::endl;
            redisFree(redis_ctx_ptr);
        }
        else
        {
            std::cerr << "Connection error: can't allocate redis context" << std::endl;
        }
        return -1;
    }

    std::vector<std::string> ip2date_list;

    redisReply *reply = (redisReply *)redisCommand(redis_ctx_ptr, "SMEMBERS %s", ip_addr.c_str());
    if (reply != nullptr)
    {
        for (size_t i = 0; i < reply->elements; ++i)
        {
            ip2date_list.emplace_back(reply->element[i]->str);
        }
    }
    else
    {
        std::cerr << "can not find ip2date set: " << ip_addr << "in redis" << std::endl;
    }
    freeReplyObject(reply);

    for(auto &date : ip2date_list)
    {
        reply = (redisReply *)redisCommand(redis_ctx_ptr, "SREM %s %s", date.c_str(), ip_addr.c_str());
        freeReplyObject(reply);
    }

    reply = (redisReply *)redisCommand(redis_ctx_ptr, "DEL %s", ip_addr.c_str());
    freeReplyObject(reply);
    
    redisFree(redis_ctx_ptr);
    std::cout << "success del ip:" << ip_addr << " in redis" << std::endl;
    return 0;
}

int main_node::recover_data_info(std::vector<std::string> &data_node_ip_list,
                                 std::vector<std::string> &data_dir_path_list)
{
    redisContext *redis_ctx_ptr = get_redis_context();
    if (redis_ctx_ptr == nullptr || redis_ctx_ptr->err)
    {
        if (redis_ctx_ptr)
        {
            std::cerr << "redis connection error" << redis_ctx_ptr->err << std::endl;
            redisFree(redis_ctx_ptr);
        }
        else
        {
            std::cerr << "Connection error: can't allocate redis context" << std::endl;
        }
        return -1;
    }

    redisReply *reply = (redisReply *)redisCommand(redis_ctx_ptr, "SMEMBERS dir_path_list");
    if (reply != nullptr)
    {
        for (size_t i = 0; i < reply->elements; ++i)
        {
            data_dir_path_list.emplace_back(reply->element[i]->str);
        }
    }
    else
    {
        std::cerr << "Fail to SMEMBERS dir_path_list" << std::endl;
    }
    freeReplyObject(reply);

    redisFree(redis_ctx_ptr);
    std::cout << "success recover_data_info from redis" << std::endl;
    return 0;
}

//---------------------------------redis part end---------------------------------------------