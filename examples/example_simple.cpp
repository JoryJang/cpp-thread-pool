// thread_pool_simple.hpp 的完整使用示例。C++11，覆盖全部公开 API。
//
// 构建（在仓库根目录执行，-I. 用于找到头文件）：
//   g++ -std=c++11 -I. -O2 -pthread examples/example_simple.cpp -o example_simple
//
// 注意：thread_pool.hpp 与 thread_pool_simple.hpp 都定义 dev::ThreadPool，
// 同一编译单元只能包含其中一个，所以两个版本各有一个示例文件。
// 把下面的 include 换成 "thread_pool.hpp"，本文件即可用原版编译运行
// （10~12 节是原版独有的接口，用精简版时请注释掉）。

#include "thread_pool_simple.hpp"

#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

// ---------- 2. submit 的基本形态：lambda + 任意参数（按值拷贝，std::bind 语义） ----------
static void demo_basic_submit(dev::ThreadPool& pool) {
    // 返回值通过 future 取回；参数 a、b 被拷贝进任务，与提交后的实参不再相关
    auto sum = pool.submit([](int a, int b) { return a + b; }, 1, 2);
    std::cout << "[2] 1 + 2 = " << sum.get() << "\n";

    // 无参数、无返回值
    std::atomic<int> side_effect(0);
    auto fire = pool.submit([&side_effect] { side_effect.fetch_add(1); });
    fire.get();  // void 任务也有 future，get() 顺便等待完成
    std::cout << "[2] void task done, side_effect = " << side_effect.load() << "\n";
}

// ---------- 3. 三类可调用对象：普通函数 / 成员函数 / 函数对象 ----------
static std::string greet(const std::string& name, int times) {
    std::string out;
    for (int i = 0; i < times; ++i) out += "hi, " + name + " ";
    return out;
}

struct Calculator {
    int mul(int a, int b) { return a * b; }  // 成员函数：第一个实参传对象指针
};

struct Greeter {
    std::string operator()(const std::string& name) const { return "hello, " + name; }
};

static void demo_callables(dev::ThreadPool& pool) {
    auto g = pool.submit(&greet, std::string("world"), 2);  // 普通函数 + 参数
    std::cout << "[3] free fn   : " << g.get() << "\n";

    Calculator calc;
    auto m = pool.submit(&Calculator::mul, &calc, 6, 7);    // 成员函数 + 对象指针
    std::cout << "[3] member fn : 6 * 7 = " << m.get() << "\n";

    auto h = pool.submit(Greeter(), std::string("pool"));   // 函数对象
    std::cout << "[3] functor   : " << h.get() << "\n";
}

// ---------- 4. 参数传递语义：默认拷贝，std::ref 才是引用 ----------
static void add_to(int& out, int v) { out += v; }

static void demo_arg_semantics(dev::ThreadPool& pool) {
    int total = 0;
    pool.submit(add_to, std::ref(total), 41).get();  // std::ref 包一层才是真引用
    std::cout << "[4] std::ref result = " << total << "\n";

    int untouched = 0;
    pool.submit([](int* p) { *p = 100; }, &untouched).get();  // 传指针也可以
    std::cout << "[4] pointer result = " << untouched << "\n";
}

// ---------- 5. 异常重放：任务抛出的异常在 future.get() 处原样抛出 ----------
static void demo_exception(dev::ThreadPool& pool) {
    auto boom = pool.submit([]() -> int { throw std::runtime_error("boom"); });
    try {
        boom.get();
    } catch (const std::runtime_error& e) {
        std::cout << "[5] caught from future: " << e.what() << "\n";
    }
    // 抛过异常的池照常可用
    auto ok = pool.submit([] { return 42; });
    std::cout << "[5] pool still works: " << ok.get() << "\n";
}

// ---------- 6. 批量提交：等全部完成的正确姿势是回收所有 future ----------
static void demo_batch(dev::ThreadPool& pool) {
    std::vector<std::future<long long>> results;
    for (int i = 0; i < 8; ++i) {
        results.push_back(pool.submit([i] {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            return static_cast<long long>(i) * i;
        }));
    }
    long long total = 0;
    for (auto& f : results) total += f.get();  // 逐个 get()，阻塞至各自完成
    std::cout << "[6] sum of squares = " << total << "\n";  // 0^2+...+7^2 = 140
}

// ---------- 7. 观测接口：thread_count() / queued_tasks() ----------
static void demo_observation(dev::ThreadPool& pool) {
    std::cout << "[7] thread_count = " << pool.thread_count() << "\n";

    // queued_tasks() 是"尚在队列、未被取走"的任务数，正在执行的不算；
    // 读数随时间变化，这里只是演示调用方式
    std::atomic<bool> done(false);
    pool.submit([&done] { while (!done.load()) std::this_thread::sleep_for(std::chrono::milliseconds(5)); });
    for (int i = 0; i < 3; ++i)
        pool.submit([] { std::this_thread::sleep_for(std::chrono::milliseconds(20)); });
    std::cout << "[7] queued_tasks right after submit = " << pool.queued_tasks()
              << " (0~3 都正常，正在执行的不计入)\n";
    done.store(true);
}

// ---------- 8. 析构排空：不等任务做完直接析构，析构返回时剩余任务已全部执行 ----------
static void demo_destructor_drain() {
    std::atomic<int> counter(0);
    {
        dev::ThreadPool pool(2);
        for (int i = 0; i < 4; ++i) {
            pool.submit([&counter] {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                counter.fetch_add(1);
            });
        }
        // 故意不回收 future，直接析构
    }
    std::cout << "[8] after destruction, executed = " << counter.load() << " (应为 4)\n";
}

// ---------- 9. 已知限制（注释说明，不演示） ----------
// 不要在任务内部对同一个池 submit 出的 future 调用 get()：
// 所有 worker 可能都卡在"等队列里的任务"，没有 worker 去执行它 —— 直接死锁。
// 需要等待结果时，回到提交方的线程再 get()。

int main() {
    // ---------- 1. 构造 ----------
    // 传 0：按硬件并发数自动决定（探测失败兜底 4）；也可以显式指定线程数。
    dev::ThreadPool pool(0);
    std::cout << "[1] pool created, threads = " << pool.thread_count() << "\n\n";

    demo_basic_submit(pool);
    demo_callables(pool);
    demo_arg_semantics(pool);
    demo_exception(pool);
    demo_batch(pool);
    demo_observation(pool);
    demo_destructor_drain();

    std::cout << "\nALL DEMOS DONE\n";
    return 0;
}
