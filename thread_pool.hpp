// C++11 线程池。
//
// 三条贯穿全文件的核心不变式（改动时务必守住）：
//   A. submit() 先把 active_tasks_ 加一，再往队列里放任务。
//      因此 active_tasks_ == 0 可以推出"所有队列都是空的"，反之不成立
//      （可能正在 push 途中）。空闲 worker 靠这条跳过全部加锁扫描。
//      配套要求：入队抛异常（bad_alloc / lock 失败）必须把这一减退回去，见 submit 的
//      catch。虚高是永久性的，会让 stop_ 之后的 worker 既睡不成也退不出。
//   B. 改动"等待条件"的一方，必须在 waiter 检查条件时所持有的那把锁内完成状态变更，
//      或者至少把 notify 放进同一把锁里。
//      worker 侧：持 sleep_mutex_ 完成 idle_workers_++ 、复查 active_tasks_ 和调用 wait()。
//      生产者侧：push 在队列锁内，notify 在 sleep_mutex_ 内。
//      只在锁外 notify 会丢唤醒 —— 通知可能打在"还没有 waiter"上被静默丢弃。
//   C. worker 只在持有 sleep_mutex_ 的时候改 idle_workers_ 并调用 wait()，
//      析构函数把 stop_ 的置位也放在 sleep_mutex_ 内（同上，属于改动等待条件）。
//
// 已知限制：任务函数内部不要对同一个池产生的 future 调用 get()。
// 所有 worker 可能都卡在"等队列里的任务"，而没有 worker 去执行它 —— 直接死锁。
// 要并发就只在调用方线程回收 future，或者换用会"帮等的人干活"的 fork-join 框架。
#ifndef DEV_THREAD_POOL_HPP
#define DEV_THREAD_POOL_HPP

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <new>  // placement new
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace dev {

// 栈上（准确说：对象内联存储）的类型擦除可调用对象。
//
// 存在的唯一理由：std::function<void()> 超过内联缓冲（libstdc++ 是 16 字节）就要
// new 一块堆内存，而这正是高频小任务场景的热点。这里给 BufferSize 字节缓冲 +
// 三个函数指针，代价是超限时编译期直接报错，以及必须是 move-only。
//
// 类型擦除原理：真正的 callable 被 placement new 进 buffer_，之后它的具体类型就不
// 可逆了，所以把"对它做什么"记成三个不捕获的 lambda（可转成函数指针）：
//   invoke_  调用
//   destroy_ 析构
//   move_    搬到另一块 buffer 并析构源头
// move_ 是必须的：没有它就无法在只认识 buffer_ 的情况下移动一个未知类型的对象，
// 而 std::deque<InplaceFunction> 的 push_back 正需要这个能力。
template <std::size_t BufferSize = 64>
class InplaceFunction {
    typedef void (*invoke_fn)(void*);
    typedef void (*destroy_fn)(void*);
    typedef void (*move_fn)(void*, void*);

public:
    // 默认构造出一个"空"对象。take() 找不到任务时就是返回它，
    // 调用方用 if (task) 判断有没有活，比额外返回 bool 干净。
    InplaceFunction() noexcept : invoke_(nullptr), destroy_(nullptr), move_(nullptr), empty_(true) {}

    // 从任意可调用对象构造。F 走万能引用，把 lambda 的类型原样保留给 construct()，
    // 这样才能拿到精确的 sizeof/alignof 去做编译期校验。
    template <class F>
    InplaceFunction(F&& f) : invoke_(nullptr), destroy_(nullptr), move_(nullptr), empty_(true) {
        construct(std::forward<F>(f));
    }

    // 移动构造：先继承对方的三个函数指针（它们描述的是同一个具体类型），
    // 再用对方留下的 move_ 把对象本体搬进自己的 buffer_。
    // 注意成员初始化列表里 buffer_ 是未初始化的裸内存，这正是 move_ 做
    // placement new 的前提。
    InplaceFunction(InplaceFunction&& other) noexcept
        : invoke_(other.invoke_), destroy_(other.destroy_), move_(other.move_), empty_(other.empty_) {
        if (!empty_) move_(buffer_, other.buffer_);
        other.clear();  // 源头已被 move_ 析构，这里只是把对方标记成空，防止二次析构
    }

    InplaceFunction& operator=(InplaceFunction&& other) noexcept {
        if (this != &other) {
            destroy_self();  // 先结束自己当前持有的对象
            invoke_ = other.invoke_;
            destroy_ = other.destroy_;
            move_ = other.move_;
            empty_ = other.empty_;
            if (!empty_) move_(buffer_, other.buffer_);
            other.clear();
        }
        return *this;
    }

    // 禁止拷贝：内联存储里放的可能是 packaged_task、unique_ptr 之类本身不可拷贝的对象，
    // 而且两个对象共享同一份"活"对象语义上是错的。
    InplaceFunction(const InplaceFunction&) = delete;
    InplaceFunction& operator=(const InplaceFunction&) = delete;

    ~InplaceFunction() { destroy_self(); }

    // 允许直接给一个空对象赋新任务：task = []{ ... };
    template <class F>
    InplaceFunction& operator=(F&& f) {
        destroy_self();
        construct(std::forward<F>(f));
        return *this;
    }

    // 唯一的多态调用点：把 buffer_ 交给类型擦除时记下的 invoke_。
    void operator()() { invoke_(buffer_); }

    // explicit 是有意的：只允许 if (task) / static_cast<bool>(task) 这种上下文中出现，
    // 避免意外参与整数转换。
    explicit operator bool() const noexcept { return !empty_; }

private:
    template <class F>
    void construct(F&& f) {
        // decay 掉引用和 const，得到真正存放的类型。
        typedef typename std::decay<F>::type stored_type;
        // 超限宁可编译失败也不退回堆分配：这条静态断言就是"零堆分配"承诺的落地方式。
        // 因为条件依赖 stored_type，它只在真正实例化这个构造函数时才触发。
        static_assert(sizeof(stored_type) <= BufferSize, "callable too large for InplaceFunction buffer");
        static_assert(alignof(stored_type) <= alignof(std::max_align_t),
                      "over-aligned callable does not fit InplaceFunction buffer");
        new (buffer_) stored_type(std::forward<F>(f));
        // 三个无捕获 lambda 转成函数指针；参数是 void*，指向持有对象的那块 buffer。
        invoke_ = [](void* p) { (*static_cast<stored_type*>(p))(); };
        destroy_ = [](void* p) { static_cast<stored_type*>(p)->~stored_type(); };
        move_ = [](void* dst, void* src) {
            new (dst) stored_type(std::move(*static_cast<stored_type*>(src)));
            static_cast<stored_type*>(src)->~stored_type();
        };
        empty_ = false;
    }

    void destroy_self() {
        if (!empty_) destroy_(buffer_);
        clear();
    }

    void clear() noexcept {
        invoke_ = nullptr;
        destroy_ = nullptr;
        move_ = nullptr;
        empty_ = true;
    }

    // buffer_ 的对齐按 std::max_align_t 保证，placement new 任意普通 callable 都合法。
    alignas(std::max_align_t) unsigned char buffer_[BufferSize];
    invoke_fn invoke_;
    destroy_fn destroy_;
    move_fn move_;
    bool empty_;
};

namespace detail {

// C++11 写法的 std::invoke_result_t（C++17 才有那个别名）。
// 参数统一 decay：F 在 submit(F&&) 里推导出来可能带引用，直接用会让 result_of 的
// 函数签名写法出现 cv-ref 歧义。
template <class F, class... Args>
struct task_result {
    typedef typename std::result_of<typename std::decay<F>::type(typename std::decay<Args>::type...)>::type type;
};

// 纯占位，用来把相邻成员隔开 64 字节以上，不参与任何逻辑。
// 特意写一个用户提供的构造函数：它让这个类型"非平凡"，否则 Clang 会把从不读写的
// 私有成员报成 -Wunused-private-field。
struct cache_pad {
    unsigned char bytes[64];
    cache_pad() {}
};

}  // namespace detail

class ThreadPool {
    typedef InplaceFunction<64> task_type;

    // 直接写 64 而不是 std::hardware_destructive_interference_size：后者是 C++17 的，
    // 且 Clang 拒绝实现（理由是缓存行大小属于运行时属性，编译期常量不靠谱）。
    static const std::size_t kCacheLine = 64;

    // 每个 worker 的全部私有状态，独占缓存行。
    //
    // 没有 alignas(64) 时，workers_[i] 和 workers_[i+1] 会挤在同一条 64 字节缓存行里：
    // 线程 A 每完成一个任务就 fetch_add 自己的 completed，这条 store 会让 CPU 把该行
    // 在 B 的缓存里标成失效，于是 B 下一次访问自己那半行也要重新从 A 拿 —— 两个毫无
    // 关系的数据互相拖累，这就是 false sharing。
    //
    // 结构内部也要分区：queue/mutex 会被别的线程（生产者、小偷）读写，而 completed 只有
    // 本 worker 每完成一个任务就写一次。如果两者共用一条缓存行（libstdc++ 下 deque 80 字节 +
    // mutex 40 字节，completed 放在最前面就会和 deque 头部同行），小偷每次扫描都会把
    // 这条行抢走，worker 的 completed 自增就要重新拿回独占权 —— 结构间隔离了，结构内部却
    // 又互相拖累。所以 completed 放在最后并单独对齐到一条缓存行，结构总大小也因此被撑到
    // 缓存行整数倍（sizeof 会随 stdlib 变，下面构造函数按 sizeof 算 stride，不受影响）。
    //
    // 禁拷贝/禁移动：里面有 mutex 和 std::atomic，本来就不可拷贝；显式写出来还能
    // 防止有人误把它塞进 std::vector（移动会直接编译失败）。
    struct alignas(64) worker_state {
        std::deque<task_type> queue;
        std::mutex mutex;
        alignas(64) std::atomic<std::uint64_t> completed;

        // std::atomic 的默认构造在 C++11 里不初始化值，必须显式给 0。
        worker_state() : completed(0) {}
        worker_state(const worker_state&) = delete;
        worker_state& operator=(const worker_state&) = delete;
    };

public:
    // thread_count 传 0 表示按硬件并发数自动决定。
    // pin_to_cores 把 worker 钉到当前进程**被授权**的核上，避免跨核迁移导致的缓存失效；
    // 授权集合由 sched_getaffinity 现场取，不信 hardware_concurrency()（见 bind_to_core）。
    // 只有 Linux 生效，其它平台静默忽略；探测失败时也静默不绑。
    explicit ThreadPool(std::size_t thread_count = 0, bool pin_to_cores = false)
        : stop_(false), pin_to_cores_(pin_to_cores), next_worker_(0), active_tasks_(0), idle_workers_(0) {
        // hardware_concurrency() 允许返回 0 表示"探测不出来"，兜一个保守值。
        // 这是本文件唯一一处使用它的地方：绑核已改为只看 sched_getaffinity，
        // 不再需要"两个调用点各自兜底"，也就不存在漏兜一个的窗口。
        if (thread_count == 0) {
            thread_count = std::thread::hardware_concurrency();
            if (thread_count == 0) thread_count = 4;
        }

        // worker_state 要求 64 字节对齐，但 C++11 的 operator new[] 没有对齐参数
        // （aligned new 是 C++17），GCC 在这里会给 -Waligned-new 警告且真的可能不对齐。
        // 所以：整池只申请一块原始内存，手动把起点向上取整到 64，再按 stride 等距
        // placement-construct 每个 worker_state。
        // 多申请的一行是给"起点对不齐"预留的滑动余量。
        const std::size_t stride = (sizeof(worker_state) + kCacheLine - 1) / kCacheLine * kCacheLine;
        storage_.reset(new unsigned char[stride * thread_count + kCacheLine]);
        const std::uintptr_t raw = reinterpret_cast<std::uintptr_t>(storage_.get());
        worker_state* slot = reinterpret_cast<worker_state*>((raw + kCacheLine - 1) / kCacheLine * kCacheLine);

        // 构造函数一旦抛出，析构函数不会运行，成员只会被逐个销毁：
        //   - threads_ 里若已有可 join 的 std::thread，vector 析构会直接 std::terminate；
        //     而那些线程还在跑、还引用着正在被销毁的 this。
        //   - 已 placement 构造的 worker_state 不会被析构（deque 的内存会泄漏）。
        // 触发条件并不玄乎：容器 pids limit 触顶时 std::thread 构造就会抛 system_error，
        // worker_state() 里 deque 的首次分配也可能 bad_alloc。
        // 所以这一段自己收拾：复用析构函数的关停握手让已起的线程退出并 join，再析构已构造的元素。
        // 此时没有任何任务入队（对象还没交到调用方手里），active_tasks_ == 0，
        // 已起的 worker 在 stop_ 置位后会正常走到退出分支。
        try {
            workers_.reserve(thread_count);
            for (std::size_t i = 0; i < thread_count; ++i) {
                workers_.push_back(new (reinterpret_cast<unsigned char*>(slot) + stride * i) worker_state());
            }

            // 状态区先就位，再起线程：run() 一进去就会读 workers_，顺序不能颠倒。
            threads_.reserve(thread_count);
            for (std::size_t i = 0; i < thread_count; ++i) {
                threads_.push_back(std::thread(&ThreadPool::run, this, i));
            }
        } catch (...) {
            shutdown_and_join();
            for (std::size_t i = 0; i < workers_.size(); ++i) workers_[i]->~worker_state();
            throw;
        }
    }

    ~ThreadPool() {
        shutdown_and_join();

        // 线程全部退出之后才能动 placement 出来的对象：
        // 反过来就会边跑边析构队列和 mutex，是未定义行为。
        // storage_ 本身交给 unique_ptr<unsigned char[]> 释放，这里只负责析构构造过的元素。
        // 注意：关停时排空剩余任务是 worker 自己的事 —— 它每轮先 take() 再判断
        // (stop_ && active_tasks_ == 0)，所以 stop_ 置位后仍会把队列里的活干完才 return。
        for (std::size_t i = 0; i < workers_.size(); ++i) workers_[i]->~worker_state();
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // 提交一个带任意实参的任务，返回结果的 future。
    // 任务抛出的异常会被 packaged_task 捕获并存进共享状态，在 future.get() 处重放。
    template <class F, class... Args>
    std::future<typename detail::task_result<F, Args...>::type> submit(F&& f, Args&&... args) {
        typedef typename detail::task_result<F, Args...>::type return_type;

        // 用 shared_ptr 包一层 packaged_task 有两个原因：
        //   1. packaged_task 是 move-only，塞进需要可拷贝语义的类型擦除存储很别扭；
        //      而 shared_ptr 指针 16 字节，稳稳放进 InplaceFunction 的 64 字节缓冲。
        //   2. 即使池被销毁、任务还没跑完，packaged_task 也仍然存活，
        //      调用方的 future 不会悬空（最多永远不 ready，这是调用方自己的问题）。
        // std::bind 在这里只负责把实参绑成零元可调用对象，实参按值拷贝进 bind 对象。
        // 用 make_shared 而不是 shared_ptr(new ...)：packaged_task 是 move-only，
        // 但 make_shared 只需要它的移动构造（在控制块里原地构造，bind 对象作为实参喂进
        // packaged_task 的模板构造），所以能合并"对象 + 控制块"两次分配为一次。
        // 实测每个任务的提交-回收往返：4.2 次分配 -> 3.2 次。
        std::shared_ptr<std::packaged_task<return_type()>> task =
            std::make_shared<std::packaged_task<return_type()>>(
                std::bind(std::forward<F>(f), std::forward<Args>(args)...));
        std::future<return_type> result = task->get_future();

        // 真正入队的只是这个薄壳 lambda，它只捕获一个 shared_ptr（16 字节），
        // 所以永远不会因为任务体大小而落到堆上 —— 这条是"类型擦除不额外分配"的关键。
        // 注意别读成"提交路径零堆分配"：packaged_task 自己的共享状态仍然要分配，
        // 上面那 3.2 次/任务就是实测总数，去掉这一次已经是极限，再往下得手写共享状态。
        task_type wrapped([task]() { (*task)(); });

        // 顺序不能换：先计数、后入队（不变式 A）。
        // 反过来（先入队后计数）会让 active_tasks_ == 0 失去"队列必空"的推断力，
        // 空闲 worker 的快速返回路径就会漏任务。
        //
        // 但正因为先加了计数，入队一旦抛异常就必须原路退回去：
        // 残留的虚高 1 会让 stop_ 之后的 worker 走进死角 —— take() 的快速返回失效
        // （开始每轮加锁扫全队列），退出条件 stop_ && active_tasks_ == 0 永不成立，
        // 入睡条件 !stop_ && ... 也因为 stop_ 已真而不成立。于是线程既睡不成也退不出，
        // 满核空转、析构的 join() 永挂。失败模式从"崩"变成"挂且不可恢复"。
        active_tasks_.fetch_add(1, std::memory_order_seq_cst);
        // try 的范围刻意只到入队为止，不要把下面的 notify 段括进来：
        // 那一段执行时任务已经在队列里，若在它的 catch 里再减一次计数，
        // 就是反向破坏不变式 A（计数低于真实已入队数），后果是关停时带着
        // 未执行的任务提前退出、future 永远拿不到值却不报错。
        // 用 catch (...) 而不是 catch (bad_alloc)：lock_guard 构造也能抛 system_error，
        // 这里不区分异常类型，只做计数回滚。
        try {
            // 轮转选队列：外部单点提交时天然把负载摊平到各 worker，
            // 之后的不均由 take() 里的偷取兜底。
            // 锁的粒度只在一条队列上，不再是全池共用的那把。
            std::size_t index = next_worker_.fetch_add(1, std::memory_order_relaxed) % threads_.size();
            std::lock_guard<std::mutex> lock(workers_[index]->mutex);
            workers_[index]->queue.push_back(std::move(wrapped));
        } catch (...) {
            active_tasks_.fetch_sub(1, std::memory_order_seq_cst);
            throw;  // 原样抛出：submit 的语义是"要么入队，要么不入队并告知调用方"
        }
        // 唤醒必须在 sleep_mutex_ 内发出。
        //
        // 只在队列锁内 push、然后在锁外 notify_one() 是有 bug 的：worker 可以在
        // sleep_mutex_ 内读完 active_tasks_（那时任务还没入队），然后在真正调用 wait()
        // 之前被抢占；等它回来时通知早已打在"还没有 waiter"上被丢弃，于是它会睡下去，
        // 而队列里有活 —— future 永不 ready。
        // 规则：改动"等待条件"的那一方，必须用 waiter 检查条件时持有的那把锁来串行化。
        //
        // 抢到这把锁之后两种交错都成立：
        //   1) 生产者先拿到锁 -> worker 随后进入检查区时，通过 release/acquire 必然
        //      看得到已递增的 active_tasks_，于是不睡，直接回到循环去取；
        //   2) worker 先拿着锁 -> 生产者被挡在锁外，直到 worker 已经注册进 condvar
        //      并释放锁，此时 notify_one() 打在一个真实存在的 waiter 上。
        //
        // 代价只落在冷路径：池子忙时 idle_workers_ == 0，这把锁一次都不碰，
        // 提交流程仍然是"只碰自己那条队列的锁"。
        //
        // 刻意不加保险的一点：这里的 lock_guard 同样可能抛 system_error，那时任务已入队、
        // 计数也对，只是没人被唤醒 —— 若全池都在睡，一样会拖到关停时挂住。不处理它是有意的：
        // std::mutex 加锁失败意味着进程已经在 OS 层故障，而这个池没有错误上报通道，
        // 为它兜底只是假装有救。上面那个 catch 管的是"我们自己算错了计数"，性质不同。
        if (idle_workers_.load(std::memory_order_seq_cst) > 0) {
            std::lock_guard<std::mutex> lock(sleep_mutex_);
            sleep_cv_.notify_one();
        }
        return result;
    }

    std::size_t thread_count() const noexcept { return threads_.size(); }

    // 还在队列里、尚未被任何 worker 取走的任务数（含正在 push 途中的）。
    // 由于不变式 A，读到的值可能大于真实已入队数，但绝不会小于。
    //
    // 它**不是**"还有多少活没干完"：任务一被取出队列计数就减一，正在执行的任务不计入。
    // 所以读到 0 只说明队列空了，不说明任务做完了 —— 仍可能有 worker 在跑，
    // 而且那个任务还可以再 submit 新任务。relaxed 读也不构成对任务副作用的 happens-before。
    // 要等"全部做完"请回收 submit 返回的 future，future 的就绪才携带同步语义。
    // （旧名 pending_tasks 容易被读成"未完成数"，已改名。）
    std::size_t queued_tasks() const noexcept { return active_tasks_.load(std::memory_order_relaxed); }

    // 每个 worker 完成的任务数。全_relaxed：这只是观测数据，不参与任何同步决策。
    // 用途是看负载是否摊开 —— 某个 worker 长期为 0 说明偷取链断了。
    std::vector<std::uint64_t> tasks_completed() const {
        std::vector<std::uint64_t> counts;
        counts.reserve(threads_.size());
        for (std::size_t i = 0; i < threads_.size(); ++i) {
            counts.push_back(workers_[i]->completed.load(std::memory_order_relaxed));
        }
        return counts;
    }

private:
    // 置位 stop_、唤醒所有 worker、等它们退出。析构函数和构造函数的失败回滚共用这一份，
    // 免得两处各写一遍握手而日后改歪。
    //
    // stop_ 必须在持有 sleep_mutex_ 的情况下置位（不变式 C）。
    // 这样任何一个 worker 要么还没拿到锁、醒来时必然看到 true；
    // 要么已经持锁在 wait() 里 —— 而 wait() 会释放该锁，本函数就能拿到它。
    // 于是"置位"和"检查后入睡"这两个动作被完全串行化，不存在置位后还有人睡着的窗口。
    void shutdown_and_join() {
        {
            std::lock_guard<std::mutex> lock(sleep_mutex_);
            stop_ = true;
        }
        // notify 放在解锁之后：否则被唤醒的 worker 会立刻阻塞在刚被本线程持有的锁上。
        sleep_cv_.notify_all();
        for (std::size_t i = 0; i < threads_.size(); ++i) threads_[i].join();
    }

    // worker 主循环：拿活 -> 干活 -> 拿不到就睡。
    void run(std::size_t index) {
        if (pin_to_cores_) bind_to_core(index);
        while (true) {
            task_type task = take(index);
            if (task) {
                task();  // 任务里的异常已被 packaged_task 吞进共享状态，不会击穿线程
                workers_[index]->completed.fetch_add(1, std::memory_order_relaxed);
                continue;  // 有活就立刻再来一轮，不做任何睡眠判断
            }

            // 拿不到活。先看一眼是否可以收工：stop_ 已置位且队列确实全空。
            // 这一遍不加锁，是给"关停已经发生"这个常见情况一条快速退出路径。
            if (stop_.load(std::memory_order_acquire) &&
                active_tasks_.load(std::memory_order_acquire) == 0) return;

            // 准备入睡。从这里开始必须在 sleep_mutex_ 内完成，见不变式 C。
            std::unique_lock<std::mutex> lock(sleep_mutex_);

            // 拿到锁之后再查一次 —— 上一查到这一次之间，析构函数可能已经置位 stop_。
            if (stop_.load(std::memory_order_seq_cst) &&
                active_tasks_.load(std::memory_order_seq_cst) == 0) return;

            // 登记"我要睡了"，再复查有没有活。这一句和 wait() 之间不能有任何锁外空隙，
            // 否则就是丢唤醒的窗口，见下面的 notify 侧注释。
            idle_workers_.fetch_add(1, std::memory_order_seq_cst);
            if (!stop_.load(std::memory_order_seq_cst) &&
                active_tasks_.load(std::memory_order_seq_cst) == 0) {
                // wait() 原子地"注册为 waiter + 释放 sleep_mutex_"。
                // 在它返回时通知可能已经打在别的 worker 身上，那条任务未必是我们的，
                // 所以不假定一定有活，回到循环顶部重新 take()。
                sleep_cv_.wait(lock);
            }
            idle_workers_.fetch_sub(1, std::memory_order_seq_cst);
            // 从这儿回到循环顶部重新 take()：被唤醒只是"可能有活了"，不是确定有。
            // 虚假唤醒、或活被别人抢走，都会让它自然再睡回去。
        }
    }

    // 取一个任务，取不到返回空对象（用 if (task) 判定）。
    //
    // 三级顺序，从最省锁到最贵：
    //   0) 全局计数为 0 -> 直接返回，一把锁都不加。高频空转场景的主要收益点。
    //   1) 自己的队列（加自己的锁，取头部 = 最早入队的）
    //   2) 别人的队列（轮转遍历，取尾部 = 最晚入队的）
    //
    // 注意这**不是** Chase-Lev 那种无锁 work-stealing，别拿它的理由来读这段代码：
    //   - 两端取并不能减少撞锁。队列是 mutex + deque，主人和小偷抢的是同一把锁，
    //     取哪一端都一样。（无锁双端队列里"两端互不干扰、只在剩 1 个元素时才竞争"
    //     才成立，这里不是。）
    //   - 也没有主人的缓存局部性可言：submit() 永远轮转选队列，任务是外部线程压进来的，
    //     不是本 worker 自己压入的；任务里再 submit 也不会优先进自己的队列。
    //   - 本地取头部，保证本队列内大致 FIFO；偷取取尾部，拿到的是最新入队的任务，
    //     会让这个任务插到更早的任务前面执行。这没有性能上的理由，只是历史写法；
    //     若在意公平/有序，把 pop_back 改成 pop_front 即可，不影响任何不变式。
    task_type take(std::size_t index) {
        // submit() 先加 active_tasks_ 再入队，所以这里读到 0 就能证明所有队列都空，
        // 于是空闲 worker 既不用碰本地锁，也不用做 O(n) 的偷取扫描。
        if (active_tasks_.load(std::memory_order_acquire) == 0) return task_type();
        {
            std::lock_guard<std::mutex> lock(workers_[index]->mutex);
            if (!workers_[index]->queue.empty()) return pop_front(*workers_[index]);
        }
        const std::size_t count = threads_.size();
        // offset 从 1 起：跳过自己（上面查过了）。固定起点会让所有空闲 worker
        // 都去骚扰同一个受害者，规模大时可以改成按 index 错开起点。
        for (std::size_t offset = 1; offset < count; ++offset) {
            worker_state& victim = *workers_[(index + offset) % count];
            std::lock_guard<std::mutex> lock(victim.mutex);
            if (!victim.queue.empty()) return pop_back(victim);
        }
        // 走到这里说明计数 > 0 但队列全空：任务正卡在 submit 的"已计数、未入队"窗口里。
        // 交回调用方，由它走睡眠握手再等一次，而不是在这里空转烧 CPU。
        // 注意这条推理依赖"该窗口是短暂的"，而它由 submit 的 catch 回滚保证：
        // 入队抛异常却不回退计数的话，这个"暂时"会变成永久，本函数也就退化成满核空转。
        return task_type();
    }

    // 以下两个函数都要求调用方已经持有 state.mutex，因此不再自己加锁。
    // 递减 active_tasks_ 必须和出队在同一把锁内完成，否则计数会短暂低于真实队列长度，
    // 破坏不变式 A 的方向性（计数 >= 实际已入队数）。
    //
    // 这里的 seq_cst 是刻意保留的，不是必需：减计数不参与任何 Dekker 配对（那对在
    // submit 的 fetch_add 与 idle_workers_ 之间），逻辑上 release 就够 —— 它只需要
    // "减计数不至于是先于出队被看到"，配 take() 里的 acquire load 即可。
    // 不降级的理由是本机实测零收益：x86-64 上 fetch_sub 的 seq_cst 与 release
    // 都编译成同一条 lock 前缀指令，而这条路径每个任务要付 3.2 次堆分配和一次互斥量，
    // 一条 fence 排不进优化清单。降 order 换来的是"三种内存序并存、每处都要单独论证跟谁配对"，
    // 而这个文件的正确性靠头上三条不变式集中推理。真要部署弱内存序平台时再连同
    // run() 里那两个 load(seq_cst) 一起重审。
    task_type pop_front(worker_state& state) {
        task_type task = std::move(state.queue.front());
        state.queue.pop_front();
        active_tasks_.fetch_sub(1, std::memory_order_seq_cst);
        return task;
    }

    task_type pop_back(worker_state& state) {
        task_type task = std::move(state.queue.back());
        state.queue.pop_back();
        active_tasks_.fetch_sub(1, std::memory_order_seq_cst);
        return task;
    }

    // 把当前线程绑到一个"确实授权给我们使用"的逻辑核上。跨核迁移会把线程私有的
    // 队列热度、分支预测、L1 数据全部丢在旧核上，实时性敏感时值得钉住。
    // 这是 POSIX 接口，非 Linux 平台编译期退化成一个空操作。
    //
    // 分母刻意不用 hardware_concurrency()，两个原因：
    //   1) 标准允许它返回 0（探测不出来，容器/沙箱里真实发生过），
    //      而 `index % 0` 是未定义行为；
    //   2) 更常见的是它返回**宿主机**核数 —— glibc 走 sysconf，不看 cgroup cpuset。
    //      8 核授权的容器里就能算出 CPU_SET(37)，pthread_setaffinity_np 直接 EINVAL，
    //      绑核静默失败：调用者以为钉住了，其实线程该漂还漂。
    // 唯一的真值是 sched_getaffinity 返回的授权集合，本函数完全以它为准。
    //
    // 降级策略：拿不到集合、集合为空、或内核拒绝设置 —— 一律不绑，继续正常跑。
    // 绑核是性能优化而非正确性前提，静默降级比让构造失败合理；
    // 但绝不构造一个指向非法核的 mask，那是把问题伪装成"优化已生效"。
    void bind_to_core(std::size_t index) {
#if defined(__linux__)
        cpu_set_t allowed;
        CPU_ZERO(&allowed);
        if (sched_getaffinity(0, sizeof(cpu_set_t), &allowed) != 0) return;

        // 不用 CPU_COUNT()：它是 glibc 扩展宏，musl（Alpine 镜像的常态）历史上不提供。
        // 这里本来就要遍历集合，自己数一遍，少一个依赖。
        std::size_t count = 0;
        for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
            if (CPU_ISSET(cpu, &allowed)) ++count;
        }
        if (count == 0) return;  // 兜住 `index % count` 的除零路径

        // 取授权集合里按核号升序的第 (index % count) 个**置位核**，
        // 而不是 CPU_SET(index % count)：cpuset 未必是 [0, count) 这段连续区间
        // （完全可以是 {2,3,6,7}），按位扫描才能命中真实存在的核号。
        const std::size_t target = index % count;
        std::size_t seen = 0;
        for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
            if (!CPU_ISSET(cpu, &allowed)) continue;
            if (seen != target) {
                ++seen;
                continue;
            }
            cpu_set_t one;
            CPU_ZERO(&one);
            CPU_SET(cpu, &one);
            // 忽略返回值 = 上面写明的静默降级，不是漏检。
            pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &one);
            return;
        }
#else
        (void)index;
#endif
    }

    // 成员声明顺序即构造顺序、且与析构顺序相反 —— 这里刻意排成：
    // 数据区（storage_/workers_）比线程（threads_）先构造、后析构，
    // 保证 join 之后才轮到 placement 对象和原始内存被回收。
    //
    // 池级的写热点用 cache_pad 隔开。worker_state 已经按缓存行隔离了，但池对象自己的
    // 成员原本全挤在一两条行里：每次 submit 的 next_worker_/active_tasks_ 自增、每次出队的
    // active_tasks_ 自减，都会让所有 worker 在 take() 里读 workers_/threads_ 时 miss ——
    // 这些只读指针根本没人写，却被邻居的写拖累。
    // 这里用填充字节而不用 alignas(64)：ThreadPool 对象常被 new/make_shared 放到堆上，
    // C++11 的 operator new 不保证 64 对齐（和 worker_state 要手工对齐是同一个问题）。
    // 填充只保证"两个热字段起点相距 >= 64 字节"，而这与对象起点对不对齐无关，
    // 所以任何起始地址下它们都不会落进同一条缓存行。
    //
    // 分三组：
    //   只读组   storage_/workers_/threads_/stop_/pin_to_cores_ —— 构造后基本不写
    //            （stop_ 只在关停时写一次）
    //   写热点   next_worker_（仅提交端写）、active_tasks_（提交端和 worker 都写）、
    //            idle_workers_（睡眠握手里写，提交端每次都读）
    //   睡眠组   sleep_mutex_/sleep_cv_ —— 只在冷路径碰
    std::unique_ptr<unsigned char[]> storage_;   // 整池 worker_state 的原始内存
    std::vector<worker_state*> workers_;         // 每个 worker 独占一条缓存行
    std::vector<std::thread> threads_;
    std::atomic<bool> stop_;
    bool pin_to_cores_;
    detail::cache_pad pad0_;
    std::atomic<std::size_t> next_worker_;       // 提交端的轮转游标
    detail::cache_pad pad1_;
    std::atomic<std::size_t> active_tasks_;      // 已入队 + 正在入队的任务总数
    detail::cache_pad pad2_;
    std::atomic<std::size_t> idle_workers_;      // 已进入睡眠握手流程的 worker 数
    detail::cache_pad pad3_;
    std::mutex sleep_mutex_;                     // 只用于睡眠/关停握手，不在任务主路径上
    std::condition_variable sleep_cv_;
};

}  // namespace dev

#endif
