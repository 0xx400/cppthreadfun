#include <iostream>
#include <thread>
#include <vector>
#include <assert.h>
#include <mutex>
#include <chrono>
#include <random>
#include "queueipc.h"

#define ASSERT(x) \
            if (! (x)) \
            { \
            std::cerr << "ERROR!! Assert " << #x << " failed\n"; \
            std::cerr << " on line " << __LINE__  << "\n"; \
            std::cerr << " in file " << __FILE__ << "\n";  \
            return -1; \
            } else \
            {      \
            std::cerr << "ASSERT PASSED " << #x << std::endl; \
            }


int test() {
    trade::QueueIPC holder;

    typename trade::QueueIPC<>::ServerT server(100, holder);

    server.add_instrument(1);
    server.add_instrument(2);

    typename trade::QueueIPC<>::ClientT client(1, holder);

    client.subscribe(1, "AS_IS");
    client.subscribe(2, "AS_IS");

    server.publish_data(1, trade::QuotesSnapshot{
            1, 
            {
                {10., 1}, {9., 100}
            }, 
            { 
                {11., 100}, {12., 555}
            }
        });

    auto msg = client.async_get_msg();
    if (msg != nullptr) {
        std::cout << "got msg with instrument_id = " << msg->instrument_id << "\n";
        for (auto &bid: msg->bids) {
            std::cout << " bid " << bid.price << " x " << bid.size << "\n";
        }
        for (auto &ask: msg->asks) {
            std::cout << " ask" << ask.price << " x " << ask.size << "\n";
        }
    }



    return 0;
}


int mt_test(
    size_t NUM_OF_PUB = 32,
    size_t NUM_OF_CLIENTS = 32,
    size_t NUM_OF_INSTRUMENTS_PER_PUB = 5) {
    size_t TOTAL_INSTRUMENTS = NUM_OF_INSTRUMENTS_PER_PUB * NUM_OF_PUB;
    std::vector<std::thread> threads;
    std::atomic<bool> running = true;
    
    trade::QueueIPC holder;
    std::mutex printm;

    for (size_t idx = 0; idx < NUM_OF_PUB; ++idx) {
        threads.emplace_back([&](int module_id, uint64_t first_iid, size_t num_of_instr) {
            typename trade::QueueIPC<>::ServerT server(module_id, holder);
            std::this_thread::sleep_for(std::chrono::seconds(1));
            {
                std::lock_guard lock(printm);
                std::cout << "started SERVER " << module_id << " iid: "
                    << first_iid << " - " << first_iid + num_of_instr << "\n";
            }
            for (uint64_t iid = first_iid; iid < first_iid + num_of_instr; ++iid) {
                server.add_instrument(iid);
            }
            while(running) {
                for (uint64_t iid = first_iid; iid < first_iid + num_of_instr; ++iid) {
                    server.publish_data(iid, trade::QuotesSnapshot{
                        iid, 
                        {
                            {10., 1000 + static_cast<int>(iid)}, {9., 2000 + static_cast<int>(iid)}
                        }, 
                        { 
                            {11., 1000 + static_cast<int>(iid)}, {12., 1000 + static_cast<int>(iid)}
                        }
                    });

                    // std::lock_guard lock(printm);
                    // std::cout << "server " << module_id << " pub " << iid << "\n";
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }, idx, idx * NUM_OF_INSTRUMENTS_PER_PUB, NUM_OF_INSTRUMENTS_PER_PUB);
    }

    for (size_t idx = 0; idx < NUM_OF_CLIENTS; ++idx) {
        threads.emplace_back([&](int module_id, size_t subscr_instr, size_t total_num_of_instr) {
            typename trade::QueueIPC<>::ClientT client(module_id, holder);
            std::map<uint64_t, int> stat;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            {
                std::lock_guard lock(printm);
                std::cout << "started CLIENT " << module_id << "\n";
            }
            std::random_device rd;
            std::mt19937 gen(rd()); 
            std::bernoulli_distribution dis(.4);
                      
            for (size_t iid = 0; iid < total_num_of_instr; ++iid) {
                if (dis(gen)) {
                    std::lock_guard lock(printm);
                    std::cout << "subscr CLIENT " << module_id << " to " << iid << "\n";
                    client.subscribe(iid, "AS_IS");
                    stat[iid] = 0;
                }

            }
            while(running) {
                auto msg = client.sync_get_msg();

                if (msg != nullptr) {
                    // std::lock_guard lock(printm);
                    // std::cout << "[" << module_id << "] got msg with instrument_id = " << msg->instrument_id << "\n";
                    // for (auto &bid: msg->bids) {
                    //     std::cout << " bid " << bid.price << " x " << bid.size << "\n";
                    // }
                    // for (auto &ask: msg->asks) {
                    //     std::cout << " ask" << ask.price << " x " << ask.size << "\n";
                    // }
                    stat[msg->instrument_id] += 1;
                }
            }
            {
                std::lock_guard lock(printm);
                std::cout << "stat CLIENT " << module_id << "\n";
                for (auto &ss: stat) {
                    std::cout << "iid: " << ss.first << " msg = " << ss.second << "\n";
                }
            }

        }, 1000+idx, NUM_OF_INSTRUMENTS_PER_PUB, TOTAL_INSTRUMENTS);
    }

    std::this_thread::sleep_for(std::chrono::seconds(10));
    running = false;

    for (auto &th: threads) {
        th.join();
    }

    return 0;
}

int main() {
    ASSERT(!test());

    ASSERT(!mt_test(30,30,30));
    return 0;
}
