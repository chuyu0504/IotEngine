if(NOT DEFINED DEPENDENCY_BUILD_DIR OR NOT IS_DIRECTORY "${DEPENDENCY_BUILD_DIR}")
    message(FATAL_ERROR "DEPENDENCY_BUILD_DIR must point to the configured build tree")
endif()
if(NOT DEFINED DESTINATION OR "${DESTINATION}" STREQUAL "")
    message(FATAL_ERROR "DESTINATION is required")
endif()

set(LICENSE_DESTINATION "${DESTINATION}/licenses")
file(MAKE_DIRECTORY "${LICENSE_DESTINATION}")
# Generate the release notice alongside the licenses collected below.
file(WRITE "${DESTINATION}/THIRD_PARTY_NOTICES.md" [=[
# iot-engine 第三方依赖与发布说明

本文件随 CI 二进制制品发布。`licenses/` 目录包含构建时实际安装或拉取的第三方许可证
原文；版本以本制品对应提交中的 `CMakeLists.txt`、`vcpkg.json`、`bun.lock` 和
`.github/workflows/build.yml` 为准。

## 后端边界

| 依赖 | 使用边界 | 链接/许可要点 |
| --- | --- | --- |
| Ruvia | 协程、HTTP、PostgreSQL、Redis、Model | 静态，MIT |
| ZLMediaKit / ZLToolKit | 仅 `mk_api` C SDK；媒体网络与 Worker 为内部实现 | 静态，MIT |
| JsonCpp | 仅 ZLMediaKit 私有传递依赖；iot-engine 业务源码不得包含或调用 | 静态，Public Domain/MIT |
| FFmpeg + x264 | 转码、封装、H.264 | 构建启用 GPL 与 version3；发布物受 GPL 条款约束 |
| FAAC | AAC 编码 | 静态，LGPL-2.0 |
| SRT / libSRTP / usrsctp | SRT、SRTP、WebRTC data channel | MPL-2.0 / BSD 类许可证 |
| OpenSSL | TLS、HMAC | 静态，Apache-2.0 |
| pugixml | SIP XML | 静态，MIT |
| Protobuf / nanopb schema | 边缘节点线协议 | BSD 类 / zlib 类许可证 |
| spdlog、fmt、Asio、hiredis、libpq、Brotli、zlib、zstd 等 | 日志与 Ruvia 传递依赖 | 许可证原文由构建脚本从 vcpkg 安装树收集 |

ZLMediaKit 的 C++ API、独立 Server/Player/Test 可执行面、MySQL、Python 和调试分配器
没有进入 iot-engine。FAAC、FFmpeg、HLS、MP4、RTP Proxy、SRT、SCTP、WebRTC、
OpenSSL、x264 和 VideoStack 媒体能力均在配置阶段做强制校验。

## 发布要求

当前二进制静态包含启用了 GPL/version3 的 FFmpeg 和 GPL x264。向第三方分发前，发布
负责人必须完成 GPL 兼容性审查，并随发布提供相应许可证、版权声明和完整对应源代码
或有效的书面源代码要约；不能只发布 UPX 压缩后的单个可执行文件。本文件是依赖清单，
不替代各许可证原文，也不构成法律意见。

内部部署仍应保留本文件和 `licenses/`，以便准确追溯制品使用的依赖与构建边界。
]=])

function(stage_license name relative_path)
    set(source "${DEPENDENCY_BUILD_DIR}/${relative_path}")
    if(NOT EXISTS "${source}")
        message(FATAL_ERROR "Required third-party license is missing: ${source}")
    endif()
    file(COPY_FILE "${source}" "${LICENSE_DESTINATION}/${name}.txt"
         ONLY_IF_DIFFERENT)
endfunction()

stage_license("ruvia" "_deps/ruvia-src/LICENSE")
stage_license("nanopb" "_deps/nanopb-src/LICENSE.txt")
stage_license("faac" "_deps/faac_source-src/COPYING")
stage_license("zlmediakit" "_deps/zlmediakit-src/LICENSE")
stage_license("zltoolkit" "_deps/zlmediakit-src/3rdpart/ZLToolKit/LICENSE")
stage_license("jsoncpp" "_deps/zlmediakit-src/3rdpart/jsoncpp/LICENSE")

file(GLOB vcpkg_copyrights
    LIST_DIRECTORIES FALSE
    "${DEPENDENCY_BUILD_DIR}/vcpkg_installed/*/share/*/copyright")
if(NOT vcpkg_copyrights)
    message(FATAL_ERROR "No vcpkg license files found in ${DEPENDENCY_BUILD_DIR}")
endif()
foreach(copyright IN LISTS vcpkg_copyrights)
    get_filename_component(package_directory "${copyright}" DIRECTORY)
    get_filename_component(package_name "${package_directory}" NAME)
    file(COPY_FILE "${copyright}"
         "${LICENSE_DESTINATION}/vcpkg-${package_name}.txt"
         ONLY_IF_DIFFERENT)
endforeach()
