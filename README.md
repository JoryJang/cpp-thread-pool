# treadPool

一个仅头文件的 **C++11 线程池**，无任何第三方依赖，提供两个版本：

| 文件 | 定位 |
|---|---|
| `thread_pool.hpp` | 原版：面向高吞吐场景的性能优化实现 |
| `thread_pool_simple.hpp` | 精简版：业务代码够用版，结构经典、易读易维护 |

## 特性

- **仅头文件，零依赖**：只需 C++11 编译器，`#include` 即用
- **future 风格 API**：`submit()` 返回 `std::future`，任务抛出的异常会在 `future.get()` 处原样重放
- **优雅关停**：析构时先排空队列里的剩余任务，再 join 全部线程；构造中途失败（如线程数超限）会自动回滚已创建的线程
- **两个版本语义一致**：换掉 include 即可互相替换

原版额外包含的性能设计：

- **per-worker 队列 + work stealing**：提交端轮转写入各 worker 队列，空闲 worker 偷取他人队列尾部，锁粒度只在单条队列
- **`InplaceFunction<64>`**：类型擦除可调用对象内联存储在 64 字节缓冲中，任务壳零额外堆分配（超出缓冲则编译期报错，不静默退回堆）
- **缓存行对齐与填充**：worker 私有状态、池级写热点均按 64 字节隔离，消除 false sharing
- **CPU 绑核（可选，仅 Linux 生效）**：以 `sched_getaffinity` 返回的授权核集合为准，容器/cpuset 环境下依然正确；其它平台静默忽略

## 快速开始

```cpp
#include "thread_pool.hpp"   // 或 "thread_pool_simple.hpp"
#include <iostream>

int main() {
    dev::ThreadPool pool(4);  // 传 0 则按硬件并发数自动决定

    // 提交任务，实参按值拷贝进任务（std::bind 语义）
    auto sum = pool.submit([](int a, int b) { return a + b; }, 1, 2);
    std::cout << "1 + 2 = " << sum.get() << std::endl;

    // 任务抛出的异常会在 future.get() 处重放
    auto boom = pool.submit([]() -> int { throw std::runtime_error("boom"); });
    try {
        boom.get();
    } catch (const std::exception& e) {
        std::cout << "caught: " << e.what() << std::endl;
    }
    return 0;
}  // 析构：排空剩余任务并 join 所有线程
```

编译：

```bash
g++ -std=c++11 -O2 -pthread main.cpp -o main
```

## 使用示例

以下示例对两个版本均适用（标注「仅原版」的除外）。

### 提交带参数的任务

实参按值拷贝进任务（`std::bind` 语义），成员函数传对象指针：

```cpp
#include "thread_pool.hpp"
#include <string>
#include <iostream>

std::string greet(const std::string& name, int times) {
    std::string out;
    for (int i = 0; i < times; ++i) out += "hi, " + name + "\n";
    return out;
}

struct Calculator {
    int mul(int a, int b) { return a * b; }
};

int main() {
    dev::ThreadPool pool(2);

    // 普通函数 + 参数
    auto g = pool.submit(&greet, "world", 2);
    std::cout << g.get();

    // 成员函数：第一个参数是对象指针，其后是成员参数
    Calculator calc;
    auto m = pool.submit(&Calculator::mul, &calc, 6, 7);
    std::cout << "6 * 7 = " << m.get() << std::endl;  // 42
    return 0;
}
```

### 批量提交，等待全部完成

`queued_tasks()` 读到 0 只说明队列空了，不代表任务做完。**等待全部完成的正确姿势是回收所有 future**：

```cpp
#include "thread_pool.hpp"
#include <vector>
#include <chrono>
#include <thread>

int main() {
    dev::ThreadPool pool;

    std::vector<std::future<long long>> results;
    for (int i = 0; i < 100; ++i) {
        results.push_back(pool.submit([i] {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            return static_cast<long long>(i) * i;
        }));
    }

    long long total = 0;
    for (auto& f : results) total += f.get();  // 逐个回收，get() 阻塞至任务完成
    return total > 0 ? 0 : 1;
}
```

### 生产者-消费者：任务内不再向同一个池提交

任务里可以 `submit` 其它任务，但**不要对同一个池返回的 future 调用 `get()`**（会死锁，见下文「已知限制」）。需要任务间传递结果时，直接传递值或用回调：

```cpp
dev::ThreadPool pool(2);

// 安全：任务 A 把结果写到共享变量，任务 B 直接读，不经 future
auto shared = std::make_shared<std::atomic<int>>(0);

pool.submit([shared] { shared->store(42, std::memory_order_release); });
pool.submit([shared] {
    int v;
    while (!(v = shared->load(std::memory_order_acquire))) { /* 忙等仅为示例 */ }
    std::printf("got %d\n", v);
});
```

### 观测池状态（仅原版）

```cpp
#include "thread_pool.hpp"
#include <iostream>

int main() {
    dev::ThreadPool pool(4);

    for (int i = 0; i < 1000; ++i)
        pool.submit([i] { return i; });

    std::cout << "queued: " << pool.queued_tasks() << "\n";  // 尚在队列中的任务数

    // 每个 worker 各自完成的任务数，用于看负载是否被偷取摊开
    for (auto c : pool.tasks_completed())
        std::cout << c << ' ';
    std::cout << "\n";
    return 0;
}
```

### CPU 绑核（仅原版，仅 Linux 生效）

实时性敏感、不希望 worker 跨核漂移丢缓存时，构造时打开 `pin_to_cores`；其它平台该参数被静默忽略：

```cpp
dev::ThreadPool pool(0, /*pin_to_cores=*/true);  // 0 = 按硬件并发数，钉到授权核集合上
```

## 版本选择

| | `thread_pool.hpp`（原版） | `thread_pool_simple.hpp`（精简版） |
|---|---|---|
| 任务队列 | 每 worker 独立 deque + 偷取 | 单一队列 + 一把锁 + 一个条件变量 |
| 任务对象存储 | 64 字节内联缓冲，零额外堆分配 | `std::function`（超限时堆分配） |
| 缓存行优化 | 有 | 无 |
| CPU 绑核 | 可选（仅 Linux） | 无 |
| 观测接口 | `thread_count()` / `queued_tasks()` / `tasks_completed()` | `thread_count()` / `queued_tasks()` |
| 适用场景 | 每秒海量小任务、多生产者高争用 | 中低频提交的常规业务代码 |

> 精简版注释中明确：常规业务量级下两者无实质性能差距，先选精简版即可。

## API

```cpp
explicit ThreadPool(std::size_t thread_count = 0, bool pin_to_cores = false);
// thread_count == 0 时按硬件并发数自动决定（探测失败兜底 4）
// pin_to_cores 仅原版有，仅 Linux 生效，失败时静默不绑

template <class F, class... Args>
std::future<R> submit(F&& f, Args&&... args);
// R = f(args...) 的返回类型；提交失败会抛异常且保证不入队

std::size_t thread_count() const noexcept;

std::size_t queued_tasks() const;
// 尚在队列、未被取走的任务数。原版读数可能略高于真实值，但绝不会偏低；
// 注意它不是"未完成任务数"——正在执行的不计入，读到 0 不代表任务做完，
// 等待全部完成请回收 submit 返回的 future

std::vector<std::uint64_t> tasks_completed() const;  // 仅原版
// 每个 worker 各自完成的任务数，用于观察负载是否摊开
```

## 已知限制

**不要在任务内部对同一个池 `submit` 出的 future 调用 `get()`。**
所有 worker 可能都卡在"等队列里的任务"，而没有 worker 去执行它——直接死锁。需要并发等待结果时，只在调用方线程回收 future，或改用会"帮等待者干活"的 fork-join 框架。

另外注意：两个文件定义的都是 `dev::ThreadPool`，**同一编译单元只包含其中一个**。

## 平台要求

- 支持 C++11 的编译器（GCC / Clang / MSVC）
- 绑核功能依赖 POSIX `sched_getaffinity`，仅 Linux 编译生效，其它平台自动退化为空操作
