# 源码构建

第三方依赖由根目录 `vcpkg.json` 声明，配置阶段通过 vcpkg 清单模式安装到构建目录的 `vcpkg_installed/`。
vcpkg 与 Ruvia 一致跟踪最新端口，不固定 baseline；CI 每次克隆最新 vcpkg 提交。
Ruvia、ZLMediaKit、FAAC、nanopb 不在 vcpkg 中，仍由 CMake FetchContent 固定完整提交获取，
Ruvia 使用带 SHA-256 校验的归档，其依赖由本项目清单统一提供。
`ports/vcpkg/` 只保留需要调整的 overlay 端口：`usrsctp` 在上游端口基础上将内部 `timingsafe_bcmp` 重命名为 `usrsctp_timingsafe_bcmp`。

## 工具链

- CMake 3.28 或更新版本、Git、Bun 1.3.14，以及已 bootstrap 的 vcpkg，`VCPKG_ROOT` 指向其根目录。
- Linux：Clang 19、GCC 14 标准库、Ninja、NASM、pkg-config、Bison、Flex、Make、curl、zip、unzip、tar。
- Windows：MSVC C++ 工具链和 Windows SDK；vcpkg 自行获取 NASM、MSYS2 等端口构建工具。

具体工具安装步骤与 CI 保持一致，见 `.github/workflows/build.yml`。

本机使用 mise 管理工具时，可通过 `mise exec bun@1.3.14 -- <命令>` 为配置、构建和测试临时选择 CI 同版本的 Bun；
其他工具同样按上述工具链准备，不需要修改全局 mise 配置。CMake 首次配置须在该环境中运行，避免缓存其他版本的可执行文件路径。

## 配置、构建与测试

先安装前端依赖：

```text
bun install --frozen-lockfile
```

Linux：

```sh
CC=clang-19 CXX=clang++-19 cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_COMPILE_WARNING_AS_ERROR=ON
cmake --build build --config Release --parallel
ctest --test-dir build --build-config Release --output-on-failure
```

`collector-route` 回归测试会启动独立的内存 Redis，使用随机本地端口，验证连接租约到期、续期、重连保护、截止事件与遥测保留。
需将 `redis-server` 加入 `PATH`；测试临时目录与日志位于根 `build/`，不连接生产 Redis。
Linux CI 安装该工具并运行真实 Redis 回归；缺少工具的环境会明确标记该项为跳过，不能据此认定连接租约行为已验证。

Windows：

```powershell
cmake -S . -B build -DCMAKE_COMPILE_WARNING_AS_ERROR=ON
cmake --build build --config Release --parallel
ctest --test-dir build --build-config Release --output-on-failure
```

根 `CMakeLists.txt` 未显式指定 `CMAKE_TOOLCHAIN_FILE` 时从 `VCPKG_ROOT` 选择 vcpkg 工具链；
Windows 默认使用与静态 MSVC 运行库匹配的 `x64-windows-static` triplet。
首次配置会编译全部 vcpkg 端口，耗时较长；设置 `VCPKG_DEFAULT_BINARY_CACHE` 可复用二进制包。

固定的 Ruvia `38122bbe19e10817c3903555cbb66cb83b2492f4` 通过
`ports/cmake/patch-ruvia.cmake` 修正 Clang 对协程内前置调用 `auto wait()` 的编译错误：
直接等待同一个 worker signal，保持原有关闭与唤醒语义，不改变公开 API。
补丁核验原始或已修复源码的 SHA-256，支持重复配置并拒绝源码漂移；依赖升级时须重新核对并移除已被上游修复的补丁。

管理端、后台媒体代理及测试 Controller 使用无框架基类的 `final` 类，并通过
`RUVIA_ROUTES_BEGIN(Type)` 声明路由；Middleware 直接满足公开概念，不再继承框架类型。
既有路由、权限及字段校验顺序保持一致，入口通过公开 `ruvia::exception_text` 记录 UTF-8 失败原因。

该版本仅提供 HTTP/1.1，项目继续通过 Nginx 终止 TLS 并反代后端；不再安装已无调用的 `ngtcp2`。
独立 HTTP、PostgreSQL、Redis 客户端通过公开 `handle()` 获取并复用操作句柄；句柄及结果在所属客户端释放前销毁，
客户端在所属 `event_loop` 等待 `shutdown()` 后再关闭线程。

## 连接状态租约与升级

Collector 的连接身份保存在 `iot:v3:device:connection:<deviceId>`，TTL 为 15 秒。
所属 Worker 每 5 秒只为当前实际存活的连接续期，查询与命令路由读取均不续期。
续期和清理核对实例、Worker incarnation、连接及 session epoch；旧连接的迟到任务不能删除或复活新会话。
连接到期索引使用 Redis 时钟和最早截止调度，实际到期后发布一次状态变化，供现有 SSE 更新页面。

升级时自动回收已认领链路设备的旧 `iot:v2:runtime:device:<deviceId>` 连接身份字段，
最后上报时间、在线超时及最新指标继续保留，历史迁移 SQL 和数据库结构不变。
运行中的全部实例应使用同一版本；旧程序不读取新连接键，回滚后通过真实重连重新建立旧版本路由。
新 Worker、链路诊断快照同样有期限，历史无 TTL 的链路快照仅在确认链路已归属当前实例后回收。

第三方许可证随制品打包：

```text
cmake -DDEPENDENCY_BUILD_DIR=build -DDESTINATION=build/artifact -P ports/cmake/stage-third-party-notices.cmake
```
