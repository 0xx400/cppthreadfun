#include <memory>
#include <condition_variable>
#include <mutex>
#include <atomic>
#include <assert.h>
#include <map>
#include <set>
#include <deque>
#include <functional>
#include <chrono>

namespace trade {


using Filter = std::string;

struct PriceLevel {
    double price;
    int size;
};

struct QuotesSnapshot {
    uint64_t instrument_id;
    std::vector<PriceLevel> bids;
    std::vector<PriceLevel> asks;
};

template <
    typename MutexT, 
    typename CondvarT,
    template <typename T> typename QueueT
>
class ClientMailbox {
public:
    MutexT mutex;
    CondvarT cv;
    QueueT<std::shared_ptr<const QuotesSnapshot>> mailbox_;

    void put_to_queue(std::shared_ptr<const QuotesSnapshot> newdata) {
        std::unique_lock<MutexT> lock(mutex);
        mailbox_.push_back(newdata);
        lock.unlock();
        cv.notify_one();
    }

    std::shared_ptr<const QuotesSnapshot> sync_get_msg(std::chrono::milliseconds waitt = std::chrono::milliseconds{100}) {
        std::unique_lock<MutexT> lock(mutex);

        if(mailbox_.size() == 0) {
            cv.wait_for(lock, waitt);
        }
        if (mailbox_.size() == 0) {
            return {};
        }
        auto ptr = mailbox_.front();
        mailbox_.pop_front();
        return ptr;
    }

    std::shared_ptr<const QuotesSnapshot> async_get_msg() {
        std::lock_guard<MutexT> lock(mutex);

        if(mailbox_.size() == 0) {
            return {};
        }
        auto ptr = mailbox_.front();
        mailbox_.pop_front();
        return ptr;
    }
};

template <
    typename HolderT_, 
    typename MutexT, 
    typename CondvarT,
    template <typename T> typename QueueT
>
class ClientIPC {
public:
    using MailBoxT = ClientMailbox<MutexT, CondvarT, QueueT>;
    using HolderT = HolderT_;
    std::map<uint64_t, std::set<Filter>> subscr;
    
    uint64_t client_id_;
    std::shared_ptr<MailBoxT> mailbox;
    HolderT &holder_;


    ClientIPC(uint64_t client_id, HolderT &holder)
        : client_id_(client_id)
        , mailbox(std::make_shared<MailBoxT>())
        , holder_(holder){
    }

    ~ClientIPC() {
        //unsubscribe here from everything.
        auto copys = subscr;
        for(auto &ss: copys) {
            for(auto &filter: ss.second) {
                unsubscribe(ss.first, filter);
            }
        }
    }

    int subscribe(uint64_t instrument_id, Filter filter) {
        auto ret = holder_.subscribe(client_id_, instrument_id, filter, mailbox);
        if (!ret) {
            subscr[instrument_id].insert(filter);
        }
        return ret;
    }

    int unsubscribe(uint64_t instrument_id, Filter filter) {
        auto ret = holder_.unsubscribe(client_id_, instrument_id, filter);
        if (!ret) {
            subscr[instrument_id].erase(filter);
            if (subscr[instrument_id].size() == 0) {
                subscr.erase(instrument_id);
            }
        }
        return ret;
    }
    std::shared_ptr<const QuotesSnapshot> sync_get_msg(std::chrono::milliseconds dt = std::chrono::milliseconds{100}) {
        return mailbox->sync_get_msg(dt);
    }

    std::shared_ptr<const QuotesSnapshot> async_get_msg() {
        return mailbox->async_get_msg();
    }
};


template <
    typename HolderT_, 
    typename MutexT, 
    typename CondvarT,
    template <typename T> typename QueueT
>
class SourceIPC {
public:
    // instrument_id -> subscr_type_id -> client_module_ids
    using HolderT = HolderT_;
    using MailBoxT = typename HolderT::MailBoxT;
    const static size_t POOL_SIZE = 100;
    const static size_t RESERVED_SIZE_IN_POOL = 20;
private:
    MutexT subscr_mutex;
    std::map<uint64_t, std::weak_ptr<MailBoxT>> clients;
    std::map<uint64_t, std::map<Filter, std::set<uint64_t>>> subscriptions;  
    HolderT &holder_;
    struct PoolT {
        std::vector<std::unique_ptr<QuotesSnapshot>> pool;
        MutexT mutex;
    };
    std::shared_ptr<PoolT> free_pool_;

public:
    SourceIPC(HolderT &holder)
        : holder_(holder) {
        free_pool_ = std::make_shared<PoolT>();
        generate_newpool();
    }

    ~SourceIPC() {
    }

    void generate_newpool() {
        for (size_t idx = 0; idx < POOL_SIZE; ++idx) {
            auto ptr = std::make_unique<QuotesSnapshot>();
            ptr->bids.reserve(RESERVED_SIZE_IN_POOL);
            ptr->asks.reserve(RESERVED_SIZE_IN_POOL);
            free_pool_->pool.push_back(std::move(ptr));
        }
    }

    int subscribe(uint64_t client_id, uint64_t instrument_id, Filter filter, std::weak_ptr<MailBoxT> mailbox) {
        std::lock_guard<MutexT> lock(subscr_mutex);
        if (subscriptions.find(instrument_id) == subscriptions.end()) {
            subscriptions[instrument_id] = {};
        }
        auto &sf = subscriptions[instrument_id];
        if (sf.find(filter) == sf.end()) {
            sf[filter] = {};
        }
        auto &soi = sf[filter];
        if (soi.find(client_id) != soi.end()) {
            return -1; // already subscribed;
        }
        soi.insert(client_id);
        if (clients.find(client_id) == clients.end()) {
            clients[client_id] = mailbox;
        }
        return 0;
    }
    
    int unsubscribe(uint64_t client_id, uint64_t instrument_id, Filter filter) {
        std::lock_guard<MutexT> lock(subscr_mutex);
        if (subscriptions.find(instrument_id) == subscriptions.end()) {
            return -1;
        }
        auto &sf = subscriptions[instrument_id];
        if (sf.find(filter) == sf.end()) {
            return -1;
        }
        auto &soi = sf[filter];
        if (soi.find(client_id) == soi.end()) {
            return -1; 
        }
        soi.erase(soi.find(client_id));
        if (soi.size() == 0) {
            sf.erase(sf.find(filter));
        }
        //TODO: scan all subscritption for remove from clients
        return 0;
    }

    std::unique_ptr<QuotesSnapshot> get_dataplace() {
        std::lock_guard<MutexT> lock_poll(free_pool_->mutex);
        auto &pool = free_pool_->pool;
        if (pool.size() == 0) {
            generate_newpool();
        }
        assert(pool.size() > 0);
        auto snapshot = std::move(pool.back());
        pool.pop_back();
        return std::move(snapshot);
    }

    int publish_data(uint64_t instrument_id, const QuotesSnapshot &data) {
        std::lock_guard<MutexT> lock(subscr_mutex);

        // some flag for users, who forgot to unsubscribe.
        bool need_cleanup = false; 

        if (subscriptions.find(instrument_id) == subscriptions.end()) {
            return 0;
        }
        auto &dests = subscriptions[instrument_id];

        uint64_t newlastts = 0; // current time

        for(auto &subscr: dests) {
            auto &filter_id = subscr.first;
            auto &clientids  = subscr.second;
            // add some map filter->std::function here
            //newdata = some_filter(data);
            auto dataplace = get_dataplace();
            auto &filter = holder_.get_filter(filter_id);

            bool needupdate = filter(*dataplace, data);
            if (!needupdate) {
                continue;
            }
//            auto sdata = std::shared_ptr<QuotesSnapshot>(dataplace.release());

            std::weak_ptr<PoolT> wptr = free_pool_;
            auto sdata = std::shared_ptr<QuotesSnapshot>(dataplace.release(), [&, wptr](QuotesSnapshot *ptr){
                auto uptr = std::unique_ptr<QuotesSnapshot>(ptr);
                auto poolptr = wptr.lock();
                if (poolptr) {
                    std::lock_guard<MutexT> lock_poll(poolptr->mutex);
                    poolptr->pool.push_back(std::move(uptr));
                }
            });

            for (auto &clientid: clientids) {
                auto &client = clients[clientid];
                if (auto cptr = client.lock()) {
                   auto sp = std::shared_ptr<const QuotesSnapshot>(sdata, sdata.get());
                   cptr->put_to_queue(sp);
                } else {
                    need_cleanup = true;
                }
            }
        }

        // all data published, remove dead subscriptions

        for(auto it = dests.begin(); it != dests.end();) {
            auto &subscr = *it;
            auto &filter = subscr.first;
            auto &clientids  = subscr.second;
            // add some map filter->std::function here
            //newdata = some_filter(data);

            for (auto clit = clientids.begin(); clit != clientids.end();) {
                auto &clientid = *clit;
                auto &client = clients[clientid];
                if (auto ptr = client.lock()) {
                    ++clit;
                } else {
                    clit = clientids.erase(clit);
                }
            }
            if (clients.size() == 0) {
                it = dests.erase(it);
            } else {
                ++it;
            }
        }
        return 0;
    }

    

};

template <
    typename HolderT_, 
    typename MutexT, 
    typename CondvarT,
    template <typename T> typename QueueT
>
class ServerIPC {
public:
    using SourceT = SourceIPC<HolderT_, MutexT, CondvarT, QueueT>;
    using HolderT = HolderT_;
    std::set<uint64_t> subscr;
    
    std::shared_ptr<SourceT> source_;
    HolderT &holder_;
    uint64_t module_id_;


    ServerIPC(uint64_t module_id, HolderT &holder)
        : source_(std::make_shared<SourceT>(holder))
        , holder_(holder)
        , module_id_(module_id){
    }

    ~ServerIPC() {
        //unsubscribe here from everything.
        auto copys = subscr;
        for(auto &ss: copys) {
            remove_instrument(ss);
        }
    }

    int add_instrument(uint64_t instrument_id) {
        if (subscr.find(instrument_id) != subscr.end()) {
            return -1;
        }

        if(holder_.register_source(instrument_id, source_)) {
            return -1;
        }
        subscr.insert(instrument_id);
        return 0;
    }

    int remove_instrument(uint64_t instrument_id) {
        if (subscr.find(instrument_id) == subscr.end()) {
            return -1;
        }
        subscr.erase(instrument_id);
        return holder_.unregister_source(instrument_id);
    }


    template <typename DataT>
    int publish_data(uint64_t instrument_id, DataT &&data) {
        return source_->publish_data(instrument_id, std::forward<DataT>(data));
    }
};

template <
    typename MutexT = std::mutex, 
    typename CondvarT = std::condition_variable,
    template <typename T> typename QueueT = std::deque
>
class QueueIPC {
public:
    using MailBoxT = ClientMailbox<MutexT, CondvarT, QueueT>;
    using OwnT = QueueIPC<MutexT, CondvarT, QueueT>;
    using SourceT = SourceIPC<OwnT, MutexT, CondvarT, QueueT>;
    using ClientT = ClientIPC<OwnT, MutexT, CondvarT, QueueT>;
    using ServerT = ServerIPC<OwnT, MutexT, CondvarT, QueueT>;
private:

    // client_module_id -> ClientMailbox
    std::map<uint64_t, std::unique_ptr<MailBoxT>> mailboxes_;

    // instrument -> source_module_id
    std::map<uint64_t, int> sources_intstuments;

    std::map<uint64_t, std::weak_ptr<SourceT>> source_instrument;
    
    MutexT subscr_mutex;

    using FilterFuncT = std::function<bool(QuotesSnapshot&, const QuotesSnapshot&)>;

    // we can replace Filter with std::string and add another filter in runtime from usercode.
    std::map<Filter, FilterFuncT> filters = {
        {
            "AS_IS", 
            [](QuotesSnapshot &out, const QuotesSnapshot &data) {
                out.instrument_id = data.instrument_id;
                out.bids.clear();
                out.asks.clear();
                // here we can add filter for amount and others.
                for (auto &bid: data.bids) {
                    out.bids.push_back(bid);
                }
                for (auto &ask: data.asks) {
                    out.asks.push_back(ask);
                }
                return true; // return false, if no updates
            }
        }
    };

public:
    MailBoxT& add_client(uint64_t client_module_id) {
        std::lock_guard<MutexT> lock(subscr_mutex);
        if (mailboxes_.find(client_module_id) != mailboxes_.end()) {
            return mailboxes_.at(client_module_id).get();
        }
        auto mb = std::make_unique<MailBoxT>();
        mailboxes_[client_module_id] = mb;
        return mailboxes_[client_module_id].get();
    }

    // void add_filter(Filter filter, FilterFuncT&) {
    //}

    const FilterFuncT& get_filter(Filter filter) {
        return filters[filter];
    }

    void remove_client(uint64_t client_module_id) {
        std::lock_guard<MutexT> lock(subscr_mutex);
        mailboxes_.erase(client_module_id);
    }

    int register_source(uint64_t instrument_id, std::weak_ptr<SourceT> src_ptr) {
        std::lock_guard<MutexT> lock(subscr_mutex);
        if (source_instrument.find(instrument_id) != source_instrument.end()) {
            return -1; // already busy
        }
        source_instrument[instrument_id] = src_ptr;
        return 0;
    }

    int unregister_source(uint64_t instrument_id) {
        std::lock_guard<MutexT> lock(subscr_mutex);
        if (source_instrument.find(instrument_id) == source_instrument.end()) {
            return -1; // already removed
        }
        source_instrument.erase(instrument_id);
        return 0;
    }
    int subscribe(uint64_t client_id, uint64_t instrument_id, Filter filter, std::weak_ptr<MailBoxT> mailbox) {
        std::lock_guard<MutexT> lock(subscr_mutex);
        if (source_instrument.find(instrument_id) == source_instrument.end()) {
            return -1;
        }
        auto &wp = source_instrument[instrument_id];
        if (auto sp = wp.lock()) {
            return sp->subscribe(client_id, instrument_id, filter, mailbox);
        } else {
            // warn
            return -1;
        }
    }

    int unsubscribe(uint64_t client_id, uint64_t instrument_id, Filter filter) {
        std::lock_guard<MutexT> lock(subscr_mutex);
        if (source_instrument.find(instrument_id) == source_instrument.end()) {
            return -1;
        }
        auto &wp = source_instrument[instrument_id];
        if (auto sp = wp.lock()) {
            return sp->unsubscribe(client_id, instrument_id, filter);
        } else {
            // warn
            return -1;
        }
    }
};

} //namespace trade

