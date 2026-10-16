// C++11 线程池 —— 精简版（业务代码够用版）。
//
// 由原版 thread_pool.hpp 精简而来：删去 InplaceFunction 类型擦除、每 worker 独立
// 队列 + 偷取、缓存行填充、CPU 绑核等性能优化，换成"单一队列 + 一把锁 + 一个
// 条件变量"的经典结构。
//
// 适用场景：业务代码 —— 任务由外部线程提交、提交频率中等、调用方用 future 回收
// 结果。此量级下与原版无实质性能差距；若将来出现每秒百万级小任务或多生产者高
// 争用，再换回原版即可。
//
// 与原版语义一致：
//   - submit() 返回 future，任务抛出的异常在 future.get() 处重放；
//   - 析构会先排空队列里的剩余任务、join 全部线程后再退出；
//   - 已知限制相同：任务内部不要对同一个池 submit 出的 future 调 get()，
//     所有 worker 可能都卡在等队列里的任务，直接死锁。要等结果就回调用方线程等。
//
// API 差异：删掉了 pin_to_cores 构造参数与 tasks_completed()，其余签名不变，
// 换掉 include 即可直接替换使用。
#ifndef DEV_THREAD_POOL_SIMPLE_HPP
#define DEV_THREAD_POOL_SIMPLE_HPP

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace dev {

class ThreadPool {
public:
    // thread_count 传 0 表示按硬件并发数自动决定（探测不出时兜底 4）。
    explicit ThreadPool(std::size_t thread_count = 0) : stop_(false) {
        if (thread_count == 0) {
            thread_count = std::thread::hardware_concurrency();
            if (thread_count == 0) thread_count = 4;
        }
        try {
            threads_.reserve(thread_count);
            for (std::size_t i = 0; i < thread_count; ++i)
                threads_.emplace_back(&ThreadPool::run, this);
        } catch (...) {
            // 线程起了一半就抛异常（如 pids 限额触顶）时，vector 析构会对可 join
            // 的 std::thread 直接 std::terminate —— 复用关停握手把已起的收回来再重抛。
            shutdown_and_join();
            throw;
        }
    }

    ~ThreadPool() { shutdown_and_join(); }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // 提交任务，返回结果的 future。实参按值拷贝进任务（std::bind 语义）。
    template <class F, class... Args>
    std::future<typename std::result_of<
        typename std::decay<F>::type(typename std::decay<Args>::type...)>::type>
    submit(F&& f, Args&&... args) {
        typedef typename std::result_of<
            typename std::decay<F>::type(typename std::decay<Args>::type...)>::type return_type;

        // shared_ptr 包一层：packaged_task 是 move-only 不便入队，且池析构后任务
        // 未跑完时 future 也不悬空（最多不 ready）。make_shared 合并两次分配。
        std::shared_ptr<std::packaged_task<return_type()>> task =
            std::make_shared<std::packaged_task<return_type()>>(
                std::bind(std::forward<F>(f), std::forward<Args>(args)...));
        std::future<return_type> result = task->get_future();

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stop_) throw std::runtime_error("submit on stopped ThreadPool");
            queue_.push([task]() { (*task)(); });
        }
        // 锁外 notify 安全：等待条件（stop_/队列）的检查与 wait() 都在同一把
        // mutex_ 下，不存在"通知打在尚无 waiter 上"的丢唤醒窗口。
        cv_.notify_one();
        return result;
    }

    std::size_t thread_count() const noexcept { return threads_.size(); }

    // 尚在队列、未被取走的任务数（不含正在执行的）。
    std::size_t queued_tasks() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

private:
    // 置位 stop_、唤醒全部 worker、join。析构与构造失败的回滚共用这一份。
    void shutdown_and_join() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_ = true;
        }
        cv_.notify_all();
        for (std::size_t i = 0; i < threads_.size(); ++i) {
            if (threads_[i].joinable()) threads_[i].join();
        }
    }

    // worker 主循环：等任务 -> 出队（锁内）-> 执行（锁外）。
    // wait 的谓词含"队列非空"，所以 stop_ 置位后仍会先排空队列再退出。
    void run() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return stop_ || !queue_.empty(); });
                if (queue_.empty()) return;  // 只可能是 stop_ 且已排空
                task = std::move(queue_.front());
                queue_.pop();
            }
            task();  // 任务异常已被 packaged_task 吞进共享状态，不会击穿线程
        }
    }

    std::vector<std::thread> threads_;
    std::queue<std::function<void()>> queue_;
    mutable std::mutex mutex_;    // 保护 queue_ 与 stop_
    std::condition_variable cv_;  // 只在队列空的冷路径上使用
    bool stop_;                   // 只在 mutex_ 内读写
};

}  // namespace dev

#endif
