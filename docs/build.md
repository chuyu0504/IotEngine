# 源码构建

第三方依赖由根目录 `vcpkg.json` 声明，配置阶段通过 vcpkg 清单模式安装到构建目录的 `vcpkg_installed/`。
vcpkg 与 Ruvia 一致跟踪最新端口，不固定 baseline；CI 每次克隆最新 vcpkg 提交。
Ruvia、ZLMediaKit、FAAC、nanopb 不在 vcpkg 中，仍由 CMake FetchContent 固定完整提交获取，
Ruvia 使用带 SHA-256 校验的归档，其依赖由本项目清单统一提供。
`ports/vcpkg/` 只保留需要调整的 overlay 端口：`usrsctp` 在上游端口基础上将内部 `timingsafe_bcmp` 重命名为 `usrsctp_timingsafe_bcmp`。

## 工具链

- CMake 3.28 或更新版本、Git、Bun 1.3.14，以及已 bootstrap 的 vcpkg，`VCPKG_ROOT` 指向其根目录。
- Linux：Ubuntu 24.04、GCC 13（`gcc-13` / `g++-13`）、Ninja、NASM、pkg-config、Bison、Flex、Make、curl、zip、unzip、tar；与固定版本 Ruvia 的 Linux CI 工具链一致。
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
CC=gcc-13 CXX=g++-13 cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_COMPILE_WARNING_AS_ERROR=ON
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

当前固定 Ruvia `0e71d3bcc48aceb2dd475a9ecef9a6303b81197a`，直接使用经 SHA-256 校验的上游归档，不应用本地 Ruvia 补丁。
该版本由上游修复模型比较、字符扫描和响应写出的 Clang 告警，并继续清理内部无用实现；项目继续使用上游公开 API。
`file_response_options::path_` 借用文件路径，调用 `c.file()` 时须传入仍然存活的具名路径；
`firmware-download` 回归测试覆盖路径生命周期、中文文件名、完整下载、HEAD 和 Range。

管理端、后台媒体代理及测试 Controller 使用无框架基类的 `final` 类，并通过
`RUVIA_ROUTES_BEGIN(Type)` 声明路由；Middleware 直接满足公开概念，不再继承框架类型。
既有路由、权限及字段校验顺序保持一致，入口通过公开 `ruvia::exception_text` 记录 UTF-8 失败原因。

`document_root` 默认驻留不超过 64 KiB 的文件，
总预算为 16 MiB；部署重启会重建静态快照，验收须覆盖入口资源、条件请求和固件 Range 下载。
连接、请求体、HTTP 客户端及 WebSocket 接收使用公开的 `http_io_buffer` 管理准备、提交与消费，
项目不依赖此前的字符串覆盖写入实现；静态根目录绑定已打开的目录句柄，文件响应读取后复核身份。
无法从上下文推断类型的 PostgreSQL 无符号参数使用 `numeric`，保留完整 `uint64_t` 范围；
升级验收包含实际数据库表达式、分页与时间字段读写，以及语句失败后连接的继续使用。

服务通过 HTTP/1.1 提供接口，项目继续通过 Nginx 终止 TLS 并反代后端；不再安装已无调用的 `ngtcp2`。
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
