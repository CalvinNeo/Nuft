#include <thread>
#include <condition_variable>
#include <mutex>
#include <vector>
#include <functional>
#include <atomic>
#include <map>
#include <string>

namespace Nuke{
struct ThreadExecutor{
    // `ThreadExecutor` works as a Task Queue
    typedef std::function<void()> Task;
    typedef std::pair<std::string, std::function<void()>> CapTask;
    size_t capacity;

    bool check_empty_unguard(){
        return tasks.size() == 0;
    }

    ThreadExecutor(size_t _capacity) : capacity(_capacity){
        close_flag.store(true);
        auto inner_loop = [this](){
            while(close_flag.load()){
                std::unique_lock<std::mutex> lk(mut);
                while(check_empty_unguard()){
                    // Wait until there is work to do.
                    cv_empty.wait(lk);
                    if(!close_flag.load()){
                        lk.unlock();
                        return;
                    }
                }
                CapTask t = tasks.back();
                tasks.pop_back();
                lk.unlock();
                // printf("Run %s\n", t.first.c_str());
                t.second();
                // printf("Finish %s\n", t.first.c_str());
            }
        };
        ths = new std::thread[capacity](inner_loop);
    }

    ~ThreadExecutor(){
        wait();
        delete [] ths;
    }

    void add_task(Task && t){
        std::unique_lock<std::mutex> lk(mut);
        tasks.push_back(std::make_pair(std::string(""), t));
        cv_empty.notify_one();
    }
    void add_task(const std::string & name, Task && t){
        std::unique_lock<std::mutex> lk(mut);
        tasks.push_back(std::make_pair(name, t));
        cv_empty.notify_one();
    }

    void stop(){
        close_flag.store(false);
        cv_empty.notify_all();
    }

    void wait(){
        stop();
        for (size_t i = 0; i < capacity; i++){
            if(ths[i].joinable()){
                ths[i].join();
            }
        }
    }

    void detach(){
        for (size_t i = 0; i < capacity; i++){
            if(ths[i].joinable()){
                ths[i].detach();
            }
        }
    }
    std::thread * ths;
    std::vector<CapTask> tasks;
    std::mutex mut;
    std::condition_variable cv_empty;
    std::atomic<bool> close_flag;
};
}
