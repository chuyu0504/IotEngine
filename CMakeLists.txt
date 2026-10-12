cmake_minimum_required(VERSION 3.28)

# Dependencies come from the vcpkg manifest. Select the toolchain from
# VCPKG_ROOT unless the caller supplied one explicitly.
if(NOT DEFINED CMAKE_TOOLCHAIN_FILE)
    if("$ENV{VCPKG_ROOT}" STREQUAL "")
        message(FATAL_ERROR "VCPKG_ROOT is not set")
    endif()
    file(TO_CMAKE_PATH "$ENV{VCPKG_ROOT}" IOT_ENGINE_VCPKG_ROOT)
    set(CMAKE_TOOLCHAIN_FILE "${IOT_ENGINE_VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
        CACHE FILEPATH "Toolchain file selected from VCPKG_ROOT")
    if(NOT EXISTS "${CMAKE_TOOLCHAIN_FILE}")
        message(FATAL_ERROR "VCPKG_ROOT does not contain scripts/buildsystems/vcpkg.cmake: $ENV{VCPKG_ROOT}")
    endif()
    unset(IOT_ENGINE_VCPKG_ROOT)
endif()
# MSVC builds use the static runtime, so dependencies use the matching triplet.
if(CMAKE_HOST_WIN32 AND NOT DEFINED VCPKG_TARGET_TRIPLET)
    set(VCPKG_TARGET_TRIPLET x64-windows-static CACHE STRING "vcpkg target triplet")
endif()
list(PREPEND VCPKG_OVERLAY_PORTS "${CMAKE_CURRENT_SOURCE_DIR}/ports/vcpkg")
list(REMOVE_DUPLICATES VCPKG_OVERLAY_PORTS)
list(APPEND VCPKG_INSTALL_OPTIONS --no-print-usage)
list(REMOVE_DUPLICATES VCPKG_INSTALL_OPTIONS)

# ZLMediaKit and its Find*.cmake modules use CMAKE_BUILD_TYPE even with
# multi-config generators. Set the dependency-discovery configuration before
# project() loads the vcpkg toolchain, otherwise Visual Studio can bind Release
# targets to /MTd libraries from debug/lib.
if(CMAKE_GENERATOR MATCHES "Visual Studio|Xcode|Ninja Multi-Config")
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING
        "Dependency-discovery configuration for multi-config generators" FORCE)
endif()

project(iot_engine VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

include(CheckIncludeFile)
include(FetchContent)
include("${CMAKE_CURRENT_SOURCE_DIR}/ports/cmake/project-targets.cmake")

# CMAKE_COMPILE_WARNING_AS_ERROR is a CI policy for targets owned by this
# repository. Do not propagate it into fetched third-party targets.
set(IOT_ENGINE_COMPILE_WARNING_AS_ERROR "${CMAKE_COMPILE_WARNING_AS_ERROR}")
set(CMAKE_COMPILE_WARNING_AS_ERROR OFF)

set(RUVIA_ENABLE_POSTGRESQL ON CACHE BOOL "" FORCE)
set(RUVIA_ENABLE_REDIS ON CACHE BOOL "" FORCE)
set(RUVIA_ENABLE_JWT ON CACHE BOOL "" FORCE)

# Ruvia resolves its packages from this project's vcpkg manifest.
FetchContent_Declare(ruvia
    URL https://github.com/hyird/Ruvia/archive/1a8fb217dd8fc13b5ffa05a16706ce2dc8a5f3b3.tar.gz
    URL_HASH SHA256=fd220eecab74a2612e3d9d75544b2085622b1d0856833ffedef8a403b0c033ac
    PATCH_COMMAND
        "${CMAKE_COMMAND}"
        "-DSOURCE_DIR=<SOURCE_DIR>"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/ports/cmake/patch-ruvia.cmake")
FetchContent_MakeAvailable(ruvia)

FetchContent_Declare(nanopb
    GIT_REPOSITORY https://github.com/nanopb/nanopb.git
    GIT_TAG cad3c18ef15a663e30e3e43e3a752b66378adec1
    # The server uses the standard C++ Protobuf runtime. Fetch nanopb only for the
    # exact nanopb.proto option schema shared with the OpenWrt nanopb generator.
    SOURCE_SUBDIR _runtime_sources_only)
FetchContent_MakeAvailable(nanopb)

# ZLMediaKit's C SDK exposes PCM-to-AAC input only when FAAC is present. FAAC
# does not ship a CMake build or a vcpkg port, so build its upstream static
# library directly from the source list used by its own MSVC project.
FetchContent_Declare(faac_source
    GIT_REPOSITORY https://github.com/knik0/faac.git
    GIT_TAG 3aec0c1cef9442fe2f8eb86a5b475c68e94f3306
    GIT_SHALLOW TRUE)
FetchContent_MakeAvailable(faac_source)
add_library(faac STATIC
    "${faac_source_SOURCE_DIR}/libfaac/bitstream.c"
    "${faac_source_SOURCE_DIR}/libfaac/blockswitch.c"
    "${faac_source_SOURCE_DIR}/libfaac/channels.c"
    "${faac_source_SOURCE_DIR}/libfaac/fft.c"
    "${faac_source_SOURCE_DIR}/libfaac/filtbank.c"
    "${faac_source_SOURCE_DIR}/libfaac/frame.c"
    "${faac_source_SOURCE_DIR}/libfaac/huff2.c"
    "${faac_source_SOURCE_DIR}/libfaac/huffdata.c"
    "${faac_source_SOURCE_DIR}/libfaac/quantize.c"
    "${faac_source_SOURCE_DIR}/libfaac/stereo.c"
    "${faac_source_SOURCE_DIR}/libfaac/tns.c"
    "${faac_source_SOURCE_DIR}/libfaac/util.c")
target_include_directories(faac
    PUBLIC "${faac_source_SOURCE_DIR}/include"
    PRIVATE "${faac_source_SOURCE_DIR}/libfaac")
target_compile_definitions(faac PRIVATE PACKAGE_VERSION="1.31.1")
check_include_file("immintrin.h" IOT_ENGINE_HAVE_IMMINTRIN_H)
if(IOT_ENGINE_HAVE_IMMINTRIN_H)
    target_compile_definitions(faac PRIVATE HAVE_IMMINTRIN_H=1)
endif()
set_property(TARGET faac PROPERTY COMPILE_WARNING_AS_ERROR OFF)
if(MSVC)
    target_compile_definitions(faac PRIVATE _CRT_SECURE_NO_WARNINGS)
else()
    target_link_libraries(faac PUBLIC m)
endif()
set(FAAC_INCLUDE_DIR "${faac_source_SOURCE_DIR}/include" CACHE PATH "" FORCE)
set(FAAC_LIBRARY faac CACHE STRING "" FORCE)

# ZLMediaKit runs the GB28181 media data plane in its own EventPollerPool and
# WorkThreadPool. Link mk_api statically so the release remains one executable;
# linkage does not change the media worker isolation boundary.
set(ENABLE_API ON CACHE BOOL "" FORCE)
set(ENABLE_API_STATIC_LIB ON CACHE BOOL "" FORCE)
# These switches build separate ZLM executables/install surfaces, not embedded
# mk_api capabilities. Keep the application boundary on the C SDK only.
set(ENABLE_CXX_API OFF CACHE BOOL "" FORCE)
set(ENABLE_SERVER OFF CACHE BOOL "" FORCE)
set(ENABLE_TESTS OFF CACHE BOOL "" FORCE)
set(ENABLE_PLAYER OFF CACHE BOOL "" FORCE)
# These are auxiliary integration/debug surfaces rather than media codecs or
# protocols. iot-engine owns persistence through Ruvia/PostgreSQL and does not
# expose ZLM's Python, MySQL, allocator, or C++ API boundaries.
set(ENABLE_MYSQL OFF CACHE BOOL "" FORCE)
set(ENABLE_PYTHON OFF CACHE BOOL "" FORCE)
set(ENABLE_ASAN OFF CACHE BOOL "" FORCE)
set(ENABLE_MEM_DEBUG OFF CACHE BOOL "" FORCE)
set(ENABLE_JEMALLOC_STATIC OFF CACHE BOOL "" FORCE)
set(ENABLE_TCMALLOC OFF CACHE BOOL "" FORCE)
set(ENABLE_FAAC ON CACHE BOOL "" FORCE)
set(ENABLE_FFMPEG ON CACHE BOOL "" FORCE)
set(ENABLE_HLS ON CACHE BOOL "" FORCE)
set(ENABLE_MP4 ON CACHE BOOL "" FORCE)
set(ENABLE_RTPPROXY ON CACHE BOOL "" FORCE)
set(ENABLE_SRT ON CACHE BOOL "" FORCE)
set(ENABLE_SCTP ON CACHE BOOL "" FORCE)
set(ENABLE_WEBRTC ON CACHE BOOL "" FORCE)
set(ENABLE_OPENSSL ON CACHE BOOL "" FORCE)
set(ENABLE_X264 ON CACHE BOOL "" FORCE)
set(ENABLE_VIDEOSTACK ON CACHE BOOL "" FORCE)
set(ENABLE_OBJCOPY OFF CACHE BOOL "" FORCE)
set(DISABLE_REPORT ON CACHE BOOL "" FORCE)
FetchContent_Declare(zlmediakit
    GIT_REPOSITORY https://github.com/ZLMediaKit/ZLMediaKit.git
    GIT_TAG 4b07053aa5f6d35505e8c548c2a08a978143fcf1
    # A fixed commit stops being the shallow clone tip as upstream advances. Fetch the complete
    # history so clean CI checkouts can continue resolving this reproducible pin.
    GIT_SHALLOW FALSE
    # The pinned revision still records Gitee submodule URLs. Override only the selected
    # submodules at clone time so CI uses their GitHub mirrors without changing any pinned SHA.
    GIT_CONFIG
        submodule.ZLToolKit.url=https://github.com/ZLMediaKit/ZLToolKit.git
        submodule.3rdpart/jsoncpp.url=https://github.com/open-source-parsers/jsoncpp.git
        submodule.3rdpart/media-server.url=https://github.com/ireader/media-server.git
    GIT_SUBMODULES
        3rdpart/ZLToolKit
        3rdpart/jsoncpp
        3rdpart/media-server
    GIT_SUBMODULES_RECURSE TRUE
    PATCH_COMMAND
        "${CMAKE_COMMAND}"
        "-DSOURCE_DIR=<SOURCE_DIR>"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/ports/cmake/patch-zlmediakit.cmake")

if(DEFINED CMAKE_MESSAGE_LOG_LEVEL)
    set(IOT_ENGINE_MESSAGE_LOG_LEVEL "${CMAKE_MESSAGE_LOG_LEVEL}")
    set(IOT_ENGINE_MESSAGE_LOG_LEVEL_DEFINED TRUE)
endif()
set(CMAKE_MESSAGE_LOG_LEVEL WARNING)
FetchContent_MakeAvailable(zlmediakit)
if(IOT_ENGINE_MESSAGE_LOG_LEVEL_DEFINED)
    set(CMAKE_MESSAGE_LOG_LEVEL "${IOT_ENGINE_MESSAGE_LOG_LEVEL}")
else()
    unset(CMAKE_MESSAGE_LOG_LEVEL)
endif()
unset(IOT_ENGINE_MESSAGE_LOG_LEVEL)
unset(IOT_ENGINE_MESSAGE_LOG_LEVEL_DEFINED)

# ZLMediaKit forces verbose makefiles on; restore concise project output after
# loading the dependency. Users can still request verbose output per build.
set(CMAKE_VERBOSE_MAKEFILE OFF CACHE BOOL
    "Show full compiler and linker commands" FORCE)

# Keep warnings from the pinned ZLMediaKit sources visible by default except
# for the specific MSVC diagnostics already audited in this revision. The
# project's own targets retain the compiler's complete warning set.
if(MSVC)
    target_compile_options(flv PRIVATE /wd4828)
    target_compile_options(mov PRIVATE /wd4828)
    target_compile_options(ZLToolKit PRIVATE /wd4244 /wd4297)
    target_compile_options(zlmediakit PRIVATE /wd4244 /wd4804)
    target_compile_options(ext-codec PRIVATE /wd4267)
    target_compile_options(srt PRIVATE /wd4244)
    target_compile_options(webrtc PRIVATE /wd4244 /wd4834)
endif()

set(CMAKE_COMPILE_WARNING_AS_ERROR "${IOT_ENGINE_COMPILE_WARNING_AS_ERROR}")
unset(IOT_ENGINE_COMPILE_WARNING_AS_ERROR)

get_target_property(IOT_ENGINE_ZLM_DEFINITIONS zlmediakit COMPILE_DEFINITIONS)
foreach(IOT_ENGINE_ZLM_FEATURE
        ENABLE_FAAC
        ENABLE_FFMPEG
        ENABLE_HLS
        ENABLE_MP4
        ENABLE_OPENSSL
        ENABLE_RTPPROXY
        ENABLE_SRT
        ENABLE_WEBRTC
        ENABLE_X264
        ENABLE_VIDEOSTACK)
    if(NOT "${IOT_ENGINE_ZLM_FEATURE}" IN_LIST IOT_ENGINE_ZLM_DEFINITIONS)
        message(FATAL_ERROR
            "Embedded ZLMediaKit feature was not enabled: ${IOT_ENGINE_ZLM_FEATURE}")
    endif()
endforeach()
get_target_property(IOT_ENGINE_ZLM_WEBRTC_DEFINITIONS webrtc COMPILE_DEFINITIONS)
if(NOT "ENABLE_SCTP" IN_LIST IOT_ENGINE_ZLM_WEBRTC_DEFINITIONS)
    message(FATAL_ERROR "Embedded ZLMediaKit WebRTC data channel support was not enabled")
endif()
unset(IOT_ENGINE_ZLM_DEFINITIONS)
unset(IOT_ENGINE_ZLM_WEBRTC_DEFINITIONS)
unset(IOT_ENGINE_ZLM_FEATURE)

target_compile_definitions(mk_api INTERFACE MediaKitApi_STATIC)
if(MSVC)
    # mk_common.h only includes the generated export header when this is set.
    # Without it, static consumers are incorrectly compiled with dllimport.
    target_compile_definitions(mk_api INTERFACE GENERATE_EXPORT)
endif()

# Imported targets found inside Ruvia and ZLMediaKit are directory-scoped;
# resolve the packages used directly by this project here.
find_package(OpenSSL 3.5 REQUIRED COMPONENTS SSL Crypto)
find_package(PostgreSQL REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(pugixml CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

set(EDGENODE_PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/service/features/edge")
set(EDGENODE_GENERATED_DIR "${CMAKE_BINARY_DIR}/generated/edgenode")
set(EDGENODE_PROTO_SOURCE "${EDGENODE_PROTO_DIR}/edge.proto")
set(WEB_TERMINAL_PROTO_SOURCE "${EDGENODE_PROTO_DIR}/terminal/terminal.proto")
set(EDGENODE_NANOPB_PROTO_SOURCE
    "${nanopb_SOURCE_DIR}/generator/proto/nanopb.proto")
set(EDGENODE_PROTO_CPP "${EDGENODE_GENERATED_DIR}/edge.pb.cc")
set(EDGENODE_PROTO_H "${EDGENODE_GENERATED_DIR}/edge.pb.h")
set(EDGENODE_NANOPB_PROTO_CPP "${EDGENODE_GENERATED_DIR}/nanopb.pb.cc")
set(EDGENODE_NANOPB_PROTO_H "${EDGENODE_GENERATED_DIR}/nanopb.pb.h")
set(WEB_TERMINAL_PROTO_CPP "${EDGENODE_GENERATED_DIR}/terminal.pb.cc")
set(WEB_TERMINAL_PROTO_H "${EDGENODE_GENERATED_DIR}/terminal.pb.h")
add_custom_command(
    OUTPUT
        "${EDGENODE_PROTO_CPP}"
        "${EDGENODE_PROTO_H}"
        "${EDGENODE_NANOPB_PROTO_CPP}"
        "${EDGENODE_NANOPB_PROTO_H}"
        "${WEB_TERMINAL_PROTO_CPP}"
        "${WEB_TERMINAL_PROTO_H}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${EDGENODE_GENERATED_DIR}"
    COMMAND protobuf::protoc
            "--cpp_out=${EDGENODE_GENERATED_DIR}"
            "--proto_path=${EDGENODE_PROTO_DIR}/terminal"
            "--proto_path=${EDGENODE_PROTO_DIR}"
            "--proto_path=${nanopb_SOURCE_DIR}/generator/proto"
            "${EDGENODE_PROTO_SOURCE}"
            "${WEB_TERMINAL_PROTO_SOURCE}"
            "${EDGENODE_NANOPB_PROTO_SOURCE}"
    DEPENDS
        "${EDGENODE_PROTO_SOURCE}"
        "${WEB_TERMINAL_PROTO_SOURCE}"
        "${EDGENODE_NANOPB_PROTO_SOURCE}"
        protobuf::protoc
    COMMENT "Generating edgenode C++ Protobuf protocol"
    VERBATIM)

add_library(edgenode-protocol STATIC
    "${EDGENODE_PROTO_CPP}"
    "${EDGENODE_NANOPB_PROTO_CPP}"
    "${WEB_TERMINAL_PROTO_CPP}")
target_include_directories(edgenode-protocol PUBLIC "${EDGENODE_GENERATED_DIR}")
target_link_libraries(edgenode-protocol PUBLIC protobuf::libprotobuf)
iot_engine_configure_target(edgenode-protocol)


add_executable(iot-engine
    service/server.cpp
    service/features/messaging/postgres_notifier/postgres_notifier.transport.cpp
    service/features/packet_log/packet_log.transport.cpp
    service/features/gb28181/device/device.runtime.cpp
    service/features/gb28181/media/media.runtime.cpp
    service/features/gb28181/media/media.transport.cpp
    service/features/gb28181/gb28181.runtime.cpp
    service/features/gb28181/sip/sip.protocol.cpp
    service/features/gb28181/sip/sip.transport.cpp)
target_include_directories(iot-engine PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/service/features/gb28181"
    "${EDGENODE_GENERATED_DIR}")
target_link_libraries(iot-engine PRIVATE
    ruvia::web
    PostgreSQL::PostgreSQL
    pugixml::pugixml
    spdlog::spdlog
    edgenode-protocol
    mk_api
    OpenSSL::Crypto)
iot_engine_configure_target(iot-engine)
if(MSVC)
    target_compile_definitions(iot-engine PRIVATE
        NOMINMAX
        WIN32_LEAN_AND_MEAN)
    target_compile_options(iot-engine PRIVATE /bigobj /FS)
endif()

enable_testing()
iot_engine_add_test(base64
    SOURCES tests/base64-test.cpp
    LIBRARIES OpenSSL::Crypto)

iot_engine_add_test(derived-point LARGE
    SOURCES tests/derived-point-test.cpp
    LIBRARIES ruvia::web OpenSSL::Crypto)
iot_engine_add_test(request-validation LARGE
    SOURCES tests/request-validation-test.cpp
    LIBRARIES ruvia::web)

iot_engine_add_test(industrial-protocol
    SOURCES tests/industrial-protocol-test.cpp
    LIBRARIES ruvia::web OpenSSL::Crypto)

iot_engine_add_test(mqtt-protocol
    SOURCES tests/mqtt-protocol-test.cpp
    LIBRARIES ruvia::web OpenSSL::Crypto)

iot_engine_add_test(mqtt-transport
    SOURCES tests/mqtt-transport-test.cpp
    LIBRARIES ruvia::web OpenSSL::Crypto)
iot_engine_add_test(mqtt-configuration LARGE
    SOURCES tests/mqtt-configuration-test.cpp
    LIBRARIES ruvia::web OpenSSL::Crypto edgenode-protocol)

iot_engine_add_test(context-operation-memory LARGE
    SOURCES tests/context-operation-memory-test.cpp
    LIBRARIES ruvia::web)

iot_engine_add_test(collector-protocol LARGE
    SOURCES
        tests/collector-protocol-test.cpp
        service/features/packet_log/packet_log.transport.cpp
    LIBRARIES ruvia::web spdlog::spdlog edgenode-protocol OpenSSL::Crypto)

iot_engine_add_test(access LARGE
    SOURCES tests/access-test.cpp
    LIBRARIES ruvia::web edgenode-protocol OpenSSL::Crypto)

iot_engine_add_test(access-webhook
    SOURCES tests/access-webhook-test.cpp)

iot_engine_add_test(storage-policy LARGE
    SOURCES tests/storage-policy-test.cpp
    LIBRARIES ruvia::web OpenSSL::Crypto)

iot_engine_add_test(edge-protocol
    SOURCES tests/edge-protocol-test.cpp
    LIBRARIES edgenode-protocol ruvia::web OpenSSL::Crypto)

iot_engine_add_test(edge-serial-debug
    SOURCES tests/edge-serial-debug-test.cpp
    LIBRARIES edgenode-protocol ruvia::web)

iot_engine_add_test(telemetry-contract
    SOURCES tests/telemetry-contract-test.cpp
    LIBRARIES ruvia::web OpenSSL::Crypto)

iot_engine_add_test(vpn-cidr
    SOURCES tests/vpn-cidr-test.cpp
    LIBRARIES ruvia::web edgenode-protocol OpenSSL::Crypto)

iot_engine_add_test(edge-config LARGE
    SOURCES tests/edge-config-test.cpp
    LIBRARIES ruvia::web edgenode-protocol OpenSSL::Crypto spdlog::spdlog)

iot_engine_add_test(api-upload
    SOURCES tests/api-upload-test.cpp
    LIBRARIES ruvia::web OpenSSL::Crypto)

iot_engine_add_test(edge-debug-request LARGE
    SOURCES tests/edge-debug-request-test.cpp
    LIBRARIES ruvia::web edgenode-protocol OpenSSL::Crypto)

iot_engine_add_test(edge-metadata
    SOURCES tests/edge-metadata-test.cpp
    LIBRARIES ruvia::web)

iot_engine_add_test(collector-state
    SOURCES tests/collector-state-test.cpp
    LIBRARIES ruvia::web OpenSSL::Crypto)

find_program(REDIS_SERVER_EXECUTABLE NAMES redis-server redis-server.exe)
if(NOT REDIS_SERVER_EXECUTABLE)
    set(REDIS_SERVER_EXECUTABLE "")
endif()
iot_engine_add_test(collector-route LARGE
    SOURCES tests/collector-route-test.cpp
    LIBRARIES ruvia::web OpenSSL::Crypto
    DEFINITIONS
        "IOT_TEST_REDIS_SERVER=\"${REDIS_SERVER_EXECUTABLE}\""
        "IOT_TEST_BUILD_DIRECTORY=\"${CMAKE_CURRENT_BINARY_DIR}\""
    MSVC_DEFINITIONS NOMINMAX WIN32_LEAN_AND_MEAN)
set_tests_properties(collector-route PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 60)

iot_engine_add_test(edge-service LARGE
    SOURCES tests/edge-service-test.cpp
    LIBRARIES ruvia::web edgenode-protocol OpenSSL::Crypto
    DEFINITIONS IOT_ENGINE_TESTING)

iot_engine_add_test(edge-projector NO_PROJECT_INCLUDE
    SOURCES tests/edge-projector-test.cpp)

iot_engine_add_test(architecture
    SOURCES tests/architecture-test.cpp
    LIBRARIES ruvia::web)

iot_engine_add_test(worker-isolation LARGE
    SOURCES tests/worker-isolation-test.cpp
    LIBRARIES ruvia::web edgenode-protocol OpenSSL::Crypto spdlog::spdlog)

iot_engine_add_test(alert-runtime LARGE
    SOURCES tests/alert-runtime-test.cpp
    LIBRARIES ruvia::web
    DEFINITIONS IOT_ENGINE_TESTING)

foreach(test_name
        configuration-service
        telemetry-latest
        telemetry-persistence
        command-service
        link-service
        protocol-service
        access-service)
    iot_engine_add_test("${test_name}"
        SOURCES "tests/${test_name}-test.cpp")
endforeach()

iot_engine_add_test(link-public-ip LARGE
    SOURCES tests/link-public-ip-test.cpp
    LIBRARIES ruvia::web edgenode-protocol OpenSSL::Crypto)

iot_engine_add_test(device-service LARGE
    SOURCES tests/device-service-test.cpp
    LIBRARIES ruvia::web edgenode-protocol OpenSSL::Crypto)

iot_engine_add_test(system-service
    SOURCES tests/system-service-test.cpp
    LIBRARIES ruvia::web)

iot_engine_add_test(gb28181-media
    SOURCES
        tests/gb28181-media-test.cpp
        service/features/packet_log/packet_log.transport.cpp
        service/features/gb28181/media/media.transport.cpp
    LIBRARIES
        ruvia::web
        spdlog::spdlog
        mk_api
        OpenSSL::Crypto
    INCLUDE_DIRECTORIES
        "${CMAKE_CURRENT_SOURCE_DIR}/service/features/gb28181"
    MSVC_DEFINITIONS
        NOMINMAX
        WIN32_LEAN_AND_MEAN)

iot_engine_add_test(model-migration LARGE
    SOURCES tests/model-migration-test.cpp
    LIBRARIES ruvia::web)

iot_engine_add_test(gb28181-request LARGE
    SOURCES tests/gb28181-request-test.cpp
    LIBRARIES ruvia::web)

iot_engine_add_test(gb28181-media-proxy
    SOURCES
        tests/gb28181-media-proxy-test.cpp
        service/features/packet_log/packet_log.transport.cpp
        service/features/gb28181/media/media.transport.cpp
    LIBRARIES ruvia::web spdlog::spdlog mk_api OpenSSL::Crypto
    MSVC_DEFINITIONS NOMINMAX WIN32_LEAN_AND_MEAN)

iot_engine_add_test(gb28181-projector LARGE
    SOURCES
        tests/gb28181-projector-test.cpp
        service/features/packet_log/packet_log.transport.cpp
        service/features/gb28181/media/media.transport.cpp
        service/features/gb28181/device/device.runtime.cpp
        service/features/gb28181/media/media.runtime.cpp
    LIBRARIES ruvia::web spdlog::spdlog mk_api OpenSSL::Crypto
    INCLUDE_DIRECTORIES
        "${CMAKE_CURRENT_SOURCE_DIR}/service/features/gb28181"
    DEFINITIONS IOT_ENGINE_TESTING)

iot_engine_add_test(gb28181-sip
    SOURCES
        tests/gb28181-sip-test.cpp
        service/features/gb28181/device/device.runtime.cpp
        service/features/gb28181/media/media.transport.cpp
        service/features/gb28181/sip/sip.protocol.cpp
        service/features/gb28181/sip/sip.transport.cpp
        service/features/gb28181/gb28181.runtime.cpp
        service/features/gb28181/media/media.runtime.cpp
    LIBRARIES
        ruvia::web
        pugixml::pugixml
        spdlog::spdlog
        mk_api
        OpenSSL::Crypto
    INCLUDE_DIRECTORIES
        "${CMAKE_CURRENT_SOURCE_DIR}/service/features/gb28181"
    MSVC_DEFINITIONS
        NOMINMAX
        WIN32_LEAN_AND_MEAN)

find_program(BUN_EXECUTABLE NAMES bun REQUIRED)

add_custom_target(iot-engine-web ALL
    COMMAND "${BUN_EXECUTABLE}" run build
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    COMMENT "Building frontend into ${CMAKE_BINARY_DIR}/web"
    VERBATIM)

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.env")
    add_custom_target(iot-engine-env ALL
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different
                "${CMAKE_CURRENT_SOURCE_DIR}/.env"
                "${CMAKE_BINARY_DIR}/.env"
        COMMENT "Copying .env into ${CMAKE_BINARY_DIR}"
        VERBATIM)
endif()
