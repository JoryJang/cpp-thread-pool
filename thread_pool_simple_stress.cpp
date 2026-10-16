// thread_pool_simple.cpp 的压力/并发测试。C++11。
//
// 与 thread_pool_stress.cpp（原版）的差异：
//   - 测试 4（嵌套提交）：原版的 submit() 不检查 stop_，"提交完根任务立刻析构池"
//     也能保证所有后代任务执行完；精简版的 submit() 在析构开始（stop_ 置位）后会
//     抛 std::runtime_error，所以这里改为在池存活期间等齐全部后代再析构。
//   - 测试 5（阻塞挤占）：精简版没有偷取，但单一共享队列天然可达同样效果——
//     N-1 个 worker 被阻塞任务占住时，剩下那个空闲 worker 仍会逐个取走队列里的任务。
//
// 构建（两种互斥的 sanitizer，要分开编）：
//   TSan（找数据竞争、丢唤醒导致的超时）：
//     g++ -std=c++11 -O1 -g -fsanitize=thread thread_pool_simple_stress.cpp -pthread -o stress_simple_tsan
//   ASan+UBSan（找泄漏、越界；加 -DTP_FAULT_INJECTION 打开"线程创建失败"用例）：
//     g++ -std=c++11 -O1 -g -fsanitize=address,undefined -DTP_FAULT_INJECTION
//         thread_pool_simple_stress.cpp -pthread -ldl -o stress_simple_asan   （两行并成一行执行）
//   故障注入用例靠在可执行文件里重定义 pthread_create 实现，会与 TSan 的拦截冲突，
//   所以只在 ASan 构建里开；用 g++（共享 libasan）以保证 dlsym(RTLD_NEXT) 取到真实实现。
//
// 环境变量 STRESS_SCALE（默认 1）按倍数放大迭代次数。
// 任何断言失败直接 abort，并打印位置；"丢唤醒"表现为 future 在超时内不就绪。

#include "thread_pool_simple.cpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <vector>

#if defined(TP_FAULT_INJECTION)
#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#endif

#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            std::fprintf(stderr, "CHECK failed at %s:%d: %s\n", __FILE__, __LINE__, #cond); \
            std::abort();                                                            \
        }                                                                            \
    } while (0)

#if defined(TP_FAULT_INJECTION)
// 故障注入：重定义 pthread_create，第 k 次成功之后的创建全部返回 EAGAIN。
// 必须在全局作用域（extern "C" 符号不受命名空间影响，但放进匿名空间读起来像内部链接）。
std::atomic<int> g_create_budget(-1);  // <0: 不注入；>=0: 还允许成功的次数，用完后全部失败

extern "C" int pthread_create(pthread_t* thread, const pthread_attr_t* attr,
                              void* (*start)(void*), void* arg) {
    typedef int (*real_fn)(pthread_t*, const pthread_attr_t*, void* (*)(void*), void*);
    static real_fn real = reinterpret_cast<real_fn>(dlsym(RTLD_NEXT, "pthread_create"));
    int budget = g_create_budget.load();
    if (budget == 0) return EAGAIN;
    if (budget > 0) g_create_budget.fetch_sub(1);
    return real(thread, attr, start, arg);
}
#endif

namespace {

int g_scale = 1;

// 等 future 就绪；超时按"丢唤醒/死锁"处理。
template <class T>
void wait_ready(std::future<T>& f) {
    CHECK(f.wait_for(std::chrono::seconds(30)) == std::future_status::ready);
}

const std::size_t kThreadCounts[] = {1, 2, 3, 8};

// 1. 关停时队列里还有积压：多个生产者灌任务，全部提交完立刻析构池。
//    要求：析构返回时每个任务都已执行（排空语义），且无竞争。
void test_shutdown_with_backlog() {
    for (int iter = 0; iter < 30 * g_scale; ++iter) {
        for (std::size_t t = 0; t < sizeof(kThreadCounts) / sizeof(kThreadCounts[0]); ++t) {
            const int producers = 4;
            const int per_producer = 1500;
            std::atomic<long> counter(0);  // 必须比池活得久
            {
                dev::ThreadPool pool(kThreadCounts[t]);
                std::vector<std::thread> prods;
                for (int p = 0; p < producers; ++p) {
                    prods.push_back(std::thread([&pool, &counter, per_producer]() {
                        for (int i = 0; i < per_producer; ++i) {
                            pool.submit([&counter]() { counter.fetch_add(1, std::memory_order_relaxed); });
                        }
                    }));
                }
                for (std::size_t p = 0; p < prods.size(); ++p) prods[p].join();
                // 不等任务做完，直接析构。
            }
            CHECK(counter.load() == static_cast<long>(producers) * per_producer);
        }
    }
}

// 2. 空池反复构造/析构：worker 刚起、还没睡下去这种交错最容易出事。
void test_construct_destroy_idle() {
    for (int iter = 0; iter < 400 * g_scale; ++iter) {
        dev::ThreadPool pool(kThreadCounts[iter % 4]);
        if (iter % 3 == 0) std::this_thread::yield();
        if (iter % 7 == 0) std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
}

// 3. 丢唤醒：多个生产者各自"提交一个 -> 等它完成"，中间夹杂让 worker 睡下去的停顿。
//    只要 cv_.notify_one() 丢一次，对应 future 就会超时。
void test_lost_wakeup() {
    dev::ThreadPool pool(4);
    const int producers = 4;
    const int cycles = 2500 * g_scale;
    std::atomic<long> done(0);
    std::vector<std::thread> prods;
    for (int p = 0; p < producers; ++p) {
        prods.push_back(std::thread([&pool, &done, p, cycles]() {
            for (int i = 0; i < cycles; ++i) {
                std::future<int> f = pool.submit([&done, i]() {
                    done.fetch_add(1, std::memory_order_relaxed);
                    return i;
                });
                wait_ready(f);
                CHECK(f.get() == i);
                // 偶尔停一下，让全池 worker 都进入睡眠，下一次提交必须靠 notify 叫醒。
                if ((i + p) % 31 == 0) std::this_thread::sleep_for(std::chrono::microseconds(300));
                else if ((i + p) % 7 == 0) std::this_thread::yield();
            }
        }));
    }
    for (std::size_t p = 0; p < prods.size(); ++p) prods[p].join();
    CHECK(done.load() == static_cast<long>(producers) * cycles);
}

// 4. 任务内再提交：二叉树式展开，要求所有后代任务都被执行。
//    注意与原版的差异（见文件头注释）：精简版关停后 submit() 会抛 runtime_error，
//    所以必须在池存活期间等齐全部后代，再析构池。
struct Tree {
    dev::ThreadPool* pool;
    std::atomic<long>* count;
    int depth;
    void operator()() const {
        count->fetch_add(1, std::memory_order_relaxed);
        if (depth > 0) {
            Tree child = {pool, count, depth - 1};
            pool->submit(child);
            pool->submit(child);
        }
    }
};

void test_nested_submit() {
    for (int iter = 0; iter < 20 * g_scale; ++iter) {
        for (std::size_t t = 0; t < sizeof(kThreadCounts) / sizeof(kThreadCounts[0]); ++t) {
            const int roots = 8;
            const int depth = 9;  // 每棵 2^(depth+1)-1 个任务
            const long expected = static_cast<long>(roots) * ((1L << (depth + 1)) - 1);
            std::atomic<long> count(0);
            {
                dev::ThreadPool pool(kThreadCounts[t]);
                Tree root = {&pool, &count, depth};
                for (int r = 0; r < roots; ++r) pool.submit(root);
                // 等全部后代执行完再析构；只数完成的任务数，不经 future（根任务提交完
                // 就返回了，其 future 不代表后代完成，也没有人能提前拿到后代的 future）。
                for (int spin = 0; count.load() != expected; ++spin) {
                    CHECK(spin < 5000000);
                    std::this_thread::yield();
                }
            }
            CHECK(count.load() == expected);
        }
    }
}

// 5. 阻塞挤占：N-1 个 worker 被阻塞任务占住，之后提交的任务只能由剩下那个空闲
//    worker 从共享队列逐个取走完成。精简版没有偷取，靠的正是单一队列的天然共享。
void test_blocked_workers_still_progress() {
    const std::size_t n = 4;
    dev::ThreadPool pool(n);
    std::atomic<int> started(0);
    std::atomic<bool> release(false);
    std::vector<std::future<void> > blockers;
    for (std::size_t i = 0; i + 1 < n; ++i) {
        blockers.push_back(pool.submit([&started, &release]() {
            started.fetch_add(1);
            while (!release.load()) std::this_thread::yield();
        }));
    }
    for (int spin = 0; started.load() < static_cast<int>(n - 1); ++spin) {
        CHECK(spin < 5000000);
        std::this_thread::yield();
    }

    std::atomic<int> ran(0);
    std::vector<std::future<void> > work;
    const int total = 200 * g_scale;
    for (int i = 0; i < total; ++i) {
        work.push_back(pool.submit([&ran]() { ran.fetch_add(1, std::memory_order_relaxed); }));
    }
    for (std::size_t i = 0; i < work.size(); ++i) wait_ready(work[i]);
    CHECK(ran.load() == total);

    release.store(true);
    for (std::size_t i = 0; i < blockers.size(); ++i) wait_ready(blockers[i]);
}

// 6. 任务抛异常：异常进 future，不击穿 worker，池照常可用。
void test_task_exception() {
    dev::ThreadPool pool(2);
    for (int i = 0; i < 200 * g_scale; ++i) {
        std::future<int> bad = pool.submit([]() -> int { throw std::runtime_error("boom"); });
        bool caught = false;
        try {
            bad.get();
        } catch (const std::runtime_error&) {
            caught = true;
        }
        CHECK(caught);
        std::future<int> ok = pool.submit([i]() { return i; });
        wait_ready(ok);
        CHECK(ok.get() == i);
    }
}

#if defined(TP_FAULT_INJECTION)
// 7. 构造时线程创建失败（注入点 pthread_create 的重定义在文件开头，全局作用域）。
//    std::thread 会因 EAGAIN 抛 system_error。
//    要求：异常原样传出；已起的线程被回收（否则 vector<thread> 析构 -> std::terminate）；
//    之后还能正常新建池。
void test_constructor_thread_create_failure() {
    const std::size_t n = 8;
    for (int ok_before_fail = 0; ok_before_fail < static_cast<int>(n); ++ok_before_fail) {
        for (int round = 0; round < 20 * g_scale; ++round) {
            g_create_budget.store(ok_before_fail);
            bool threw = false;
            try {
                dev::ThreadPool pool(n);
            } catch (const std::system_error&) {
                threw = true;
            }
            g_create_budget.store(-1);
            CHECK(threw);
        }
        // 失败不应留下任何后遗症。
        dev::ThreadPool pool(2);
        std::future<int> f = pool.submit([]() { return 42; });
        wait_ready(f);
        CHECK(f.get() == 42);
    }
}
#endif

}  // namespace

int main() {
    if (const char* s = std::getenv("STRESS_SCALE")) {
        g_scale = std::atoi(s);
        if (g_scale < 1) g_scale = 1;
    }

    struct Case {
        const char* name;
        void (*fn)();
    };
    const Case cases[] = {
        {"shutdown_with_backlog", test_shutdown_with_backlog},
        {"construct_destroy_idle", test_construct_destroy_idle},
        {"lost_wakeup", test_lost_wakeup},
        {"nested_submit", test_nested_submit},
        {"blocked_workers_still_progress", test_blocked_workers_still_progress},
        {"task_exception", test_task_exception},
#if defined(TP_FAULT_INJECTION)
        {"constructor_thread_create_failure", test_constructor_thread_create_failure},
#endif
    };
    for (std::size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        std::printf("[ RUN  ] %s\n", cases[i].name);
        std::fflush(stdout);
        cases[i].fn();
        std::printf("[  OK  ] %s\n", cases[i].name);
    }
    std::printf("ALL PASSED\n");
    return 0;
}
