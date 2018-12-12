#include <thread>
#include <condition_variable>
#include <mutex>
#include <vector>
#include <functional>
#include <atomic>

namespace Nuke{
struct ThreadExecutor{
    // `ThreadExecutor` works as a Task Queue
    typedef std::function<void()> Task;
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
                    cv_empty.wait(lk);
                    if(!close_flag.load()){
                        lk.unlock();
                        return;
                    }
                }
                Task t = tasks.back();
                tasks.pop_back();
                lk.unlock();
                t();
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
        tasks.push_back(t);
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
    std::vector<Task> tasks;
    std::mutex mut;
    std::condition_variable cv_empty;
    std::atomic<bool> close_flag;
};
}
