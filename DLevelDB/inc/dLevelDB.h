//
// Created by Dilpreet on 2021-06-20.
//

#pragma once

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <fstream>
#include <ifaddrs.h>
#include <iostream>
#include <leveldb/db.h>
#include <map>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <utility>
#include <vector>

typedef std::chrono::high_resolution_clock high_resolution_clock;

constexpr long outgoing_connection_timeout_ms = 5000;

// JSON functions
bool isJSONValid(rapidjson::Document& doc);
std::string getStringFromJSON(rapidjson::Value const& doc);

class dLevelDB {
    std::map<std::string, std::unique_ptr<leveldb::DB> > dbs;
    std::map<std::string, leveldb::Options> options;

    leveldb::Status newLevelDB(std::string name);

    std::future<int> nm_future;  // Async network manager

    // Networking
    bool running;

    // Network identity of this device
    std::string network_name;
    std::string network_ip, network_netmask;

    rapidjson::Document network_json;

    std::chrono::time_point<high_resolution_clock> vitals_pulse_timer;

    std::vector<std::future<int> > incoming_connections;
    std::vector<std::tuple<std::future<std::string>, int, std::chrono::time_point<high_resolution_clock>, std::shared_ptr<bool> > > outgoing_connections;  // Connection future, socket, timeout timer, needs to exists

    std::map<std::string, std::shared_ptr<bool> > vitals_connections;  // Map devices to their vitals check pulse's "needs to exist" boolean

    std::mutex incomming_connection_mutex;
    std::mutex outgoing_connection_mutex;
    std::mutex data_recv_mutex;

    int sock;
    struct sockaddr_in dst{};
    struct sockaddr_in serv{};
    socklen_t sock_size = sizeof(struct sockaddr_in);

    std::string db_name;
    unsigned int port;

    void saveNetworkConfig();

    std::string createConnection(std::string const& msg, std::string const& dst_ip);
    int handleConnection();
    std::string parseMessage(std::string const& msg);

public:
    dLevelDB(std::string const& config_path = "./.config/ddb.config");
    ~dLevelDB() = default;

    // Operations
    std::string get(std::string key);
    bool put(std::string key, std::string value);
    bool remove(std::string key);
    std::string operator[](std::string key);

    // Networking
    int start();
    int stop();
};
