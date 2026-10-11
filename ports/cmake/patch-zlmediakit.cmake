if(NOT DEFINED SOURCE_DIR)
    message(FATAL_ERROR "SOURCE_DIR is required")
endif()

function(require_pinned_revision REPOSITORY EXPECTED_REVISION)
    execute_process(
        COMMAND git -C "${REPOSITORY}" rev-parse HEAD
        RESULT_VARIABLE REVISION_RESULT
        OUTPUT_VARIABLE ACTUAL_REVISION
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET)
    if(NOT REVISION_RESULT EQUAL 0 OR NOT ACTUAL_REVISION STREQUAL EXPECTED_REVISION)
        message(FATAL_ERROR
            "Unexpected dependency revision in ${REPOSITORY}: expected ${EXPECTED_REVISION}, got ${ACTUAL_REVISION}")
    endif()
endfunction()

function(patch_pinned_source SOURCE_FILE ORIGINAL_TEXT PATCHED_TEXT DESCRIPTION)
    if(NOT EXISTS "${SOURCE_FILE}")
        message(FATAL_ERROR "Missing pinned dependency source for ${DESCRIPTION}: ${SOURCE_FILE}")
    endif()

    file(READ "${SOURCE_FILE}" SOURCE_CONTENT)
    # Protect fully patched occurrences only when they contain the old text.
    # Other replacements may shorten text or share a replacement with another
    # field; their remaining original occurrences must still be processed.
    string(FIND "${PATCHED_TEXT}" "${ORIGINAL_TEXT}" EMBEDDED_ORIGINAL)
    set(WORK_CONTENT "${SOURCE_CONTENT}")
    if(NOT EMBEDDED_ORIGINAL EQUAL -1)
        string(REPLACE "${PATCHED_TEXT}" "__IOT_PINNED_PATCH_COMPLETE__" WORK_CONTENT "${WORK_CONTENT}")
    endif()
    string(FIND "${SOURCE_CONTENT}" "${PATCHED_TEXT}" PATCHED_INDEX)
    string(FIND "${WORK_CONTENT}" "${ORIGINAL_TEXT}" ORIGINAL_INDEX)
    if(ORIGINAL_INDEX EQUAL -1)
        if(NOT PATCHED_INDEX EQUAL -1)
            return()
        endif()
        message(FATAL_ERROR "Unable to find expected source text for ${DESCRIPTION} in ${SOURCE_FILE}")
    endif()

    string(REPLACE
        "${ORIGINAL_TEXT}"
        "${PATCHED_TEXT}"
        PATCHED_CONTENT
        "${WORK_CONTENT}")
    string(REPLACE "__IOT_PINNED_PATCH_COMPLETE__" "${PATCHED_TEXT}" PATCHED_CONTENT "${PATCHED_CONTENT}")
    file(WRITE "${SOURCE_FILE}" "${PATCHED_CONTENT}")
    message(STATUS "Patched pinned dependency source: ${DESCRIPTION}")
endfunction()

require_pinned_revision("${SOURCE_DIR}" "4b07053aa5f6d35505e8c548c2a08a978143fcf1")
require_pinned_revision(
    "${SOURCE_DIR}/3rdpart/ZLToolKit"
    "d7d601200f65f3111dbf9d20ed7f14d2654bbd6e")
require_pinned_revision(
    "${SOURCE_DIR}/3rdpart/media-server"
    "21c4451ff2e4c4bb1c817e606c8b4e5deac1e719")

# The pinned media-server revision is compiled with the repository warning
# policy on Linux and therefore needs source-level fixes for its audited
# signedness and unused-value diagnostics. Keep these replacements exact: a
# changed dependency revision must fail the patch step instead of silently
# carrying an unreviewed compatibility patch forward.
set(MEDIA_SERVER_ROOT "${SOURCE_DIR}/3rdpart/media-server")

# IceTransport's global token subscribes before config.cpp's std::string
# broadcast names have necessarily been initialized. Initialize the listener
# with the function-local shared owner, after application startup, instead.
patch_pinned_source(
    "${SOURCE_DIR}/webrtc/IceTransport.cpp"
    [=[        static auto instance = std::make_shared<PortManager>();
        return *instance;]=]
    [=[        static auto instance = std::make_shared<PortManager>();
        static onceToken listener([]() { instance->addListenConfigReload(); });
        return *instance;]=]
    "ZLMediaKit lazy ICE port configuration listener")
patch_pinned_source(
    "${SOURCE_DIR}/webrtc/IceTransport.cpp"
    [=[onceToken PortManager_token([](){
    PortManager<0>::Instance().addListenConfigReload();
    PortManager<1>::Instance().addListenConfigReload();
});]=]
    [=[// PortManager::Instance registers listeners after static initialization.]=]
    "ZLMediaKit remove pre-main ICE listener registration")

patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/aom-av1.c"
    "obu_type = (data[i] >> 3) & 0x0F;\n"
    "obu_type = (data[i] >> 3) & 0x0F;\n\t\t(void)obu_type;\n"
    "libflv AV1 OBU type parse")

patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/flv-muxer.c"
    "assert(m + n <= flv->capacity);"
    "assert((size_t)m + (size_t)n <= flv->capacity);"
    "libflv muxer capacity assertion")

patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/flv-reader.c"
    "n -= sizeof(data)"
    "n -= (int)sizeof(data)"
    "libflv reader chunk decrement")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/flv-reader.c"
    "n >= sizeof(data)"
    "n >= (int)sizeof(data)"
    "libflv reader chunk size")

patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/flv-writer.c"
    "assert(n + 2 <= sizeof(vec) / sizeof(vec[0]));"
    "assert(n >= 0 && n + 2 <= (int)(sizeof(vec) / sizeof(vec[0])));"
    "libflv writer vector bound")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/flv-writer.c"
    "i + 2 < sizeof(vec)/sizeof(vec[0])"
    "i + 2 < (int)(sizeof(vec)/sizeof(vec[0]))"
    "libflv writer vector index")

patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/hevc-annexbtomp4.c"
    "sps_temporal_id_nesting_flag = ptr[2] & 0x01;\n"
    "sps_temporal_id_nesting_flag = ptr[2] & 0x01;\n\t(void)sps_temporal_id_nesting_flag;\n"
    "libflv HEVC SPS temporal flag parse")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/hevc-annexbtomp4.c"
    "if (sodb < 3)\n\t\treturn 0xFF; (void)hevc;"
    "if (sodb < 3)\n\t\t{\n\t\t\treturn 0xFF;\n\t\t}\n\t\t(void)hevc;"
    "libflv HEVC PPS error branch")

patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/mpeg4-aac-asc.c"
    "element_instance_tag = mpeg4_bits_copy(pce, bits, 4);"
    "element_instance_tag = mpeg4_bits_copy(pce, bits, 4);\n\t(void)element_instance_tag;"
    "libflv AAC PCE element tag")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/mpeg4-aac-asc.c"
    "\n\t\ttag = mpeg4_bits_copy(pce, bits, 4);"
    "\n\t\ttag = mpeg4_bits_copy(pce, bits, 4);\n\t\t(void)tag;"
    "libflv AAC PCE channel tag")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/mpeg4-aac-asc.c"
    [=[if (0x0F == samplingFrequencyIndex)
		samplingFrequency = mpeg4_bits_read_uint32(bits, 24);]=]
    [=[if (0x0F == samplingFrequencyIndex)
	{
		samplingFrequency = mpeg4_bits_read_uint32(bits, 24);
		(void)samplingFrequency;
	}]=]
    "libflv AAC explicit sampling frequency")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/mpeg4-aac-asc.c"
    "streamCnt = 0;"
    "streamCnt = 0;\n\t\t(void)streamCnt;"
    "libflv AAC stream counter")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/mpeg4-aac-asc.c"
    "numSubFrames = (uint8_t)mpeg4_bits_read_n(bits, 6);"
    "numSubFrames = (uint8_t)mpeg4_bits_read_n(bits, 6);\n\t\t(void)numSubFrames;"
    "libflv AAC LATM subframe count")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/mpeg4-aac-asc.c"
    "*/ (uint8_t)mpeg4_bits_read_n"
    "*/ (void)mpeg4_bits_read_n"
    "libflv AAC LATM discarded fields")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/mpeg4-aac-asc.c"
    "*/ (uint16_t)mpeg4_bits_read_n"
    "*/ (void)mpeg4_bits_read_n"
    "libflv AAC LATM discarded lengths")


patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/mpeg4-aac.c"
    [=[i < ARRAYOF(s_frequency)]=]
    [=[i < (int)(ARRAYOF(s_frequency))]=]
    "libflv AAC frequency lookup bound")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/mpeg4-aac.c"
    [=[i >= ARRAYOF(s_frequency)]=]
    [=[i >= (int)(ARRAYOF(s_frequency))]=]
    "libflv AAC frequency lookup result")



patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/mpeg4-vvc.c"
    "i < sizeof(vvc->native_ptl.general_constraint_info)/sizeof(vvc->native_ptl.general_constraint_info[0])"
    "i < (int)(sizeof(vvc->native_ptl.general_constraint_info)/sizeof(vvc->native_ptl.general_constraint_info[0]))"
    "libflv VVC constraint information bound")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/mpeg4-vvc.c"
    "i < sizeof(vvc->native_ptl.sublayer_level_idc)/sizeof(vvc->native_ptl.sublayer_level_idc[0])"
    "i < (int)(sizeof(vvc->native_ptl.sublayer_level_idc)/sizeof(vvc->native_ptl.sublayer_level_idc[0]))"
    "libflv VVC sublayer information bound")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/mpeg4-vvc.c"
    "i < sizeof(vvc->native_ptl.sublayer_level_idc) / sizeof(vvc->native_ptl.sublayer_level_idc[0])"
    "i < (int)(sizeof(vvc->native_ptl.sublayer_level_idc) / sizeof(vvc->native_ptl.sublayer_level_idc[0]))"
    "libflv VVC saved sublayer information bound")

patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/opus-head.c"
    "pad += n == 255 ? 254 : n;"
    "pad += n == 255 ? 254 : n;\n\t\t(void)pad;"
    "libflv Opus padding parse")

patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/vvc-annexbtomp4.c"
    "vps_max_layers_minus1 = (ptr[3] >> 2) & 0x3F;"
    "vps_max_layers_minus1 = (ptr[3] >> 2) & 0x3F;\n\t(void)vps_max_layers_minus1;"
    "libflv VVC VPS layer parse")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/vvc-annexbtomp4.c"
    "vps_max_sub_layers_minus1 = ((ptr[3] & 0x3) << 2) | ((ptr[4] >> 7) & 0x01);"
    "vps_max_sub_layers_minus1 = ((ptr[3] & 0x3) << 2) | ((ptr[4] >> 7) & 0x01);\n\t(void)vps_max_sub_layers_minus1;"
    "libflv VVC VPS sublayer parse")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/vvc-annexbtomp4.c"
    "sps_max_sub_layers_minus1 = (ptr[3] >> 5) & 0x07;"
    "sps_max_sub_layers_minus1 = (ptr[3] >> 5) & 0x07;\n\t(void)sps_max_sub_layers_minus1;"
    "libflv VVC SPS sublayer parse")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/vvc-annexbtomp4.c"
    "if (sodb < 12)\n\t\treturn 0xFF; (void)vvc;"
    "if (sodb < 12)\n\t\t{\n\t\t\treturn 0xFF;\n\t\t}\n\t\t(void)vvc;"
    "libflv VVC PPS error branch")

patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libflv/source/xiph-flac.c"
    "\n        n = 4;"
    "\n        n = 4;\n        (void)n;"
    "libflv FLAC marker parse")

set(ROOT_CMAKE "${SOURCE_DIR}/CMakeLists.txt")
file(READ "${ROOT_CMAKE}" ROOT_CONTENT)
string(REPLACE
    "update_cached_list(MK_LINK_LIBRARIES \${OPENSSL_LIBRARIES})"
    "update_cached_list(MK_LINK_LIBRARIES OpenSSL::SSL OpenSSL::Crypto)"
    PATCHED_ROOT_CONTENT
    "${ROOT_CONTENT}")
if(PATCHED_ROOT_CONTENT STREQUAL ROOT_CONTENT)
    message(STATUS "ZLMediaKit OpenSSL imported targets already patched")
else()
    file(WRITE "${ROOT_CMAKE}" "${PATCHED_ROOT_CONTENT}")
    message(STATUS "Patched ZLMediaKit OpenSSL links to stay configuration-aware")
endif()

set(TOOLKIT_CMAKE "${SOURCE_DIR}/3rdpart/ZLToolKit/CMakeLists.txt")
file(READ "${TOOLKIT_CMAKE}" TOOLKIT_CONTENT)
string(REPLACE
    "update_cached_list(TK_LINK_LIBRARIES \${OPENSSL_LIBRARIES})"
    "update_cached_list(TK_LINK_LIBRARIES OpenSSL::SSL OpenSSL::Crypto)"
    PATCHED_TOOLKIT_CONTENT
    "${TOOLKIT_CONTENT}")
if(PATCHED_TOOLKIT_CONTENT STREQUAL TOOLKIT_CONTENT)
    message(STATUS "ZLToolKit OpenSSL imported targets already patched")
else()
    file(WRITE "${TOOLKIT_CMAKE}" "${PATCHED_TOOLKIT_CONTENT}")
    message(STATUS "Patched ZLToolKit OpenSSL links to stay configuration-aware")
endif()

set(API_CMAKE "${SOURCE_DIR}/api/CMakeLists.txt")
file(READ "${API_CMAKE}" CONTENT)
string(REPLACE
    "\${CMAKE_SOURCE_DIR}/resource.rc"
    "\${CMAKE_CURRENT_SOURCE_DIR}/../resource.rc"
    PATCHED_CONTENT
    "${CONTENT}")
if(PATCHED_CONTENT STREQUAL CONTENT)
    message(STATUS "ZLMediaKit MSVC resource path already patched")
else()
    file(WRITE "${API_CMAKE}" "${PATCHED_CONTENT}")
    message(STATUS "Patched ZLMediaKit MSVC resource path for subproject builds")
endif()

set(API_PRIVATE_LINK_MARKER
    "target_link_libraries(mk_api PRIVATE -Wl,--start-group")
string(FIND "${PATCHED_CONTENT}" "${API_PRIVATE_LINK_MARKER}" API_PRIVATE_LINK_INDEX)
if(API_PRIVATE_LINK_INDEX EQUAL -1)
    string(REPLACE
        "target_link_libraries(mk_api -Wl,--start-group"
        "target_link_libraries(mk_api PRIVATE -Wl,--start-group"
        PATCHED_API_LINK_CONTENT
        "${PATCHED_CONTENT}")
    string(REPLACE
        "target_link_libraries(mk_api log -Wl,--start-group"
        "target_link_libraries(mk_api PRIVATE log -Wl,--start-group"
        PATCHED_API_LINK_CONTENT
        "${PATCHED_API_LINK_CONTENT}")
    string(REPLACE
        "target_link_libraries(mk_api \${LINK_LIBRARIES})"
        "target_link_libraries(mk_api PRIVATE \${LINK_LIBRARIES})"
        PATCHED_API_LINK_CONTENT
        "${PATCHED_API_LINK_CONTENT}")
else()
    set(PATCHED_API_LINK_CONTENT "${PATCHED_CONTENT}")
endif()
if(PATCHED_API_LINK_CONTENT STREQUAL PATCHED_CONTENT)
    message(STATUS "ZLMediaKit C API private link boundary already patched")
else()
    file(WRITE "${API_CMAKE}" "${PATCHED_API_LINK_CONTENT}")
    message(STATUS "Patched ZLMediaKit C API dependencies to stay private")
endif()

set(EXT_CODEC_CMAKE "${SOURCE_DIR}/ext-codec/CMakeLists.txt")
file(READ "${EXT_CODEC_CMAKE}" EXT_CODEC_CONTENT)
set(PATCHED_EXT_CODEC_CONTENT "${EXT_CODEC_CONTENT}")
set(NOMINMAX_BLOCK
    "if(MSVC)\n  target_compile_definitions(ext-codec PRIVATE NOMINMAX)\nendif()")
set(DUPLICATE_NOMINMAX_BLOCK "${NOMINMAX_BLOCK}\n${NOMINMAX_BLOCK}")
string(FIND "${PATCHED_EXT_CODEC_CONTENT}" "${NOMINMAX_BLOCK}" NOMINMAX_INDEX)
if(NOMINMAX_INDEX EQUAL -1)
    string(REPLACE
        "target_compile_definitions(ext-codec PUBLIC \${COMPILE_DEFINITIONS})"
        "target_compile_definitions(ext-codec PUBLIC \${COMPILE_DEFINITIONS})\n${NOMINMAX_BLOCK}"
        PATCHED_EXT_CODEC_CONTENT
        "${PATCHED_EXT_CODEC_CONTENT}")
else()
    while(TRUE)
        string(FIND
            "${PATCHED_EXT_CODEC_CONTENT}"
            "${DUPLICATE_NOMINMAX_BLOCK}"
            DUPLICATE_NOMINMAX_INDEX)
        if(DUPLICATE_NOMINMAX_INDEX EQUAL -1)
            break()
        endif()
        string(REPLACE
            "${DUPLICATE_NOMINMAX_BLOCK}"
            "${NOMINMAX_BLOCK}"
            PATCHED_EXT_CODEC_CONTENT
            "${PATCHED_EXT_CODEC_CONTENT}")
    endwhile()
endif()

string(FIND
    "${PATCHED_EXT_CODEC_CONTENT}"
    "media-server/libflv/include"
    LIBFLV_INCLUDE_INDEX)
if(LIBFLV_INCLUDE_INDEX EQUAL -1)
    string(REPLACE
        "\"$<BUILD_INTERFACE:\${CMAKE_CURRENT_SOURCE_DIR}>\"\n        PUBLIC"
        "\"$<BUILD_INTERFACE:\${CMAKE_CURRENT_SOURCE_DIR}>\"\n        \"${SOURCE_DIR}/3rdpart/media-server/libflv/include\"\n        PUBLIC"
        PATCHED_EXT_CODEC_CONTENT
        "${PATCHED_EXT_CODEC_CONTENT}")
endif()
if(PATCHED_EXT_CODEC_CONTENT STREQUAL EXT_CODEC_CONTENT)
    message(STATUS "ZLMediaKit ext-codec integration already patched")
else()
    file(WRITE "${EXT_CODEC_CMAKE}" "${PATCHED_EXT_CODEC_CONTENT}")
    message(STATUS "Patched ZLMediaKit ext-codec integration for static MSVC builds")
endif()

set(THIRDPARTY_CMAKE "${SOURCE_DIR}/3rdpart/CMakeLists.txt")
file(READ "${THIRDPARTY_CMAKE}" THIRDPARTY_CONTENT)
string(FIND
    "${THIRDPARTY_CONTENT}"
    "Build libflv for ext-codec even when MP4 recording is disabled."
    LIBFLV_FALLBACK_INDEX)
if(LIBFLV_FALLBACK_INDEX EQUAL -1)
    set(LIBFLV_FALLBACK [=[
# Build libflv for ext-codec even when MP4 recording is disabled.
if(NOT ENABLE_MP4)
  set(MediaServer_FLV_ROOT ${MediaServer_ROOT}/libflv)
  aux_source_directory(${MediaServer_FLV_ROOT}/include FLV_SRC_LIST)
  aux_source_directory(${MediaServer_FLV_ROOT}/source FLV_SRC_LIST)
  add_library(flv STATIC ${FLV_SRC_LIST})
  add_library(MediaServer::flv ALIAS flv)
  target_compile_options(flv PRIVATE ${COMPILE_OPTIONS_DEFAULT})
  target_include_directories(flv
          PRIVATE
          "$<BUILD_INTERFACE:${MediaServer_FLV_ROOT}/include>"
          PUBLIC
          "$<BUILD_INTERFACE:${MediaServer_FLV_ROOT}/include>")
  update_cached_list(MK_LINK_LIBRARIES MediaServer::flv)
endif()

]=])
    string(REPLACE
        "# 添加 mpeg 用于支持 ts 生成"
        "${LIBFLV_FALLBACK}# 添加 mpeg 用于支持 ts 生成"
        PATCHED_THIRDPARTY_CONTENT
        "${THIRDPARTY_CONTENT}")
else()
    set(PATCHED_THIRDPARTY_CONTENT "${THIRDPARTY_CONTENT}")
endif()
if(PATCHED_THIRDPARTY_CONTENT STREQUAL THIRDPARTY_CONTENT)
    message(STATUS "ZLMediaKit libflv fallback already patched")
else()
    file(WRITE "${THIRDPARTY_CMAKE}" "${PATCHED_THIRDPARTY_CONTENT}")
    message(STATUS "Patched ZLMediaKit libflv fallback for ENABLE_MP4=OFF")
endif()

set(MK_RECORDER_SOURCE "${SOURCE_DIR}/api/source/mk_recorder.cpp")
file(READ "${MK_RECORDER_SOURCE}" MK_RECORDER_CONTENT)
set(PATCHED_MK_RECORDER_CONTENT "${MK_RECORDER_CONTENT}")
set(MUXER_INCLUDE "#include \"Common/MultiMediaSourceMuxer.h\"")
set(DUPLICATE_MUXER_INCLUDE "${MUXER_INCLUDE}\n${MUXER_INCLUDE}")
string(FIND "${PATCHED_MK_RECORDER_CONTENT}" "${MUXER_INCLUDE}" MUXER_INCLUDE_INDEX)
if(MUXER_INCLUDE_INDEX EQUAL -1)
    string(REPLACE
        "#include \"Record/Recorder.h\"\n"
        "#include \"Record/Recorder.h\"\n${MUXER_INCLUDE}\n"
        PATCHED_MK_RECORDER_CONTENT
        "${PATCHED_MK_RECORDER_CONTENT}")
else()
    while(TRUE)
        string(FIND
            "${PATCHED_MK_RECORDER_CONTENT}"
            "${DUPLICATE_MUXER_INCLUDE}"
            DUPLICATE_MUXER_INCLUDE_INDEX)
        if(DUPLICATE_MUXER_INCLUDE_INDEX EQUAL -1)
            break()
        endif()
        string(REPLACE
            "${DUPLICATE_MUXER_INCLUDE}"
            "${MUXER_INCLUDE}"
            PATCHED_MK_RECORDER_CONTENT
            "${PATCHED_MK_RECORDER_CONTENT}")
    endwhile()
endif()
if(PATCHED_MK_RECORDER_CONTENT STREQUAL MK_RECORDER_CONTENT)
    message(STATUS "ZLMediaKit recorder includes already patched")
else()
    file(WRITE "${MK_RECORDER_SOURCE}" "${PATCHED_MK_RECORDER_CONTENT}")
    message(STATUS "Patched ZLMediaKit recorder includes for ENABLE_MP4=OFF")
endif()

set(MK_RTP_SERVER_SOURCE "${SOURCE_DIR}/api/source/mk_rtp_server.cpp")
file(READ "${MK_RTP_SERVER_SOURCE}" MK_RTP_SERVER_CONTENT)
set(PATCHED_MK_RTP_SERVER_CONTENT "${MK_RTP_SERVER_CONTENT}")
string(FIND
    "${PATCHED_MK_RTP_SERVER_CONTENT}"
    "#include <memory>"
    MK_RTP_MEMORY_INCLUDE_INDEX)
if(MK_RTP_MEMORY_INCLUDE_INDEX EQUAL -1)
    string(REPLACE
        "#include \"Util/logger.h\""
        "#include \"Util/logger.h\"\n#include <memory>"
        PATCHED_MK_RTP_SERVER_CONTENT
        "${PATCHED_MK_RTP_SERVER_CONTENT}")
endif()
string(REPLACE
    "RtpServer::Ptr *server = new RtpServer::Ptr(new RtpServer);"
    "std::unique_ptr<RtpServer::Ptr> server(new RtpServer::Ptr(new RtpServer));"
    PATCHED_MK_RTP_SERVER_CONTENT
    "${PATCHED_MK_RTP_SERVER_CONTENT}")
string(REPLACE
    "return (mk_rtp_server)server;"
    "return (mk_rtp_server)server.release();"
    PATCHED_MK_RTP_SERVER_CONTENT
    "${PATCHED_MK_RTP_SERVER_CONTENT}")
if(PATCHED_MK_RTP_SERVER_CONTENT STREQUAL MK_RTP_SERVER_CONTENT)
    message(STATUS "ZLMediaKit RTP C API exception ownership already patched")
else()
    file(WRITE "${MK_RTP_SERVER_SOURCE}" "${PATCHED_MK_RTP_SERVER_CONTENT}")
    message(STATUS "Patched ZLMediaKit RTP C API exception ownership")
endif()

set(MK_COMMON_SOURCE "${SOURCE_DIR}/api/source/mk_common.cpp")
file(READ "${MK_COMMON_SOURCE}" MK_COMMON_CONTENT)
set(PATCHED_MK_COMMON_CONTENT "${MK_COMMON_CONTENT}")
string(FIND
    "${PATCHED_MK_COMMON_CONTENT}"
    "Drain asynchronous server destruction before returning"
    MK_COMMON_DRAIN_INDEX)
if(MK_COMMON_DRAIN_INDEX EQUAL -1)
    string(REPLACE
        "    stopAllTcpServer();\n}"
        [=[    stopAllTcpServer();

    // Drain asynchronous server destruction before returning. TcpServer and
    // UdpServer clone deleters post their final delete to the owning poller;
    // returning before those tasks run lets process shutdown race live objects.
    const auto drain = [](TaskExecutorGetterImp &pool) {
        pool.for_each([](const TaskExecutor::Ptr &executor) {
            const auto poller = std::static_pointer_cast<EventPoller>(executor);
            if (!poller->isCurrentThread())
                poller->sync([]() {});
        });
    };
    drain(EventPollerPool::Instance());
    drain(WorkThreadPool::Instance());
    // Destructors executed by the first barrier can enqueue cross-pool cleanup.
    drain(EventPollerPool::Instance());
    drain(WorkThreadPool::Instance());
}]=]
        PATCHED_MK_COMMON_CONTENT
        "${PATCHED_MK_COMMON_CONTENT}")
endif()
string(FIND
    "${PATCHED_MK_COMMON_CONTENT}"
    "Drain asynchronous server destruction before returning"
    PATCHED_MK_COMMON_DRAIN_INDEX)
if(PATCHED_MK_COMMON_DRAIN_INDEX EQUAL -1)
    message(FATAL_ERROR "Unable to patch ZLMediaKit synchronous server shutdown")
endif()
if(PATCHED_MK_COMMON_CONTENT STREQUAL MK_COMMON_CONTENT)
    message(STATUS "ZLMediaKit synchronous server shutdown already patched")
else()
    file(WRITE "${MK_COMMON_SOURCE}" "${PATCHED_MK_COMMON_CONTENT}")
    message(STATUS "Patched ZLMediaKit synchronous server shutdown")
endif()

patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmov/source/mov-avc1.c"
    "entry->extra_data_size < box->size"
    "(uint64_t)entry->extra_data_size < box->size"
    "libmov AVC extra data capacity")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmov/source/mov-mehd.c"
    [=[    (void)box;]=]
    [=[    (void)box;
    (void)fragment_duration;]=]
    "libmov movie extends duration read")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmov/source/mov-opus.c"
    "entry->extra_data_size < box->size + 8"
    "(uint64_t)entry->extra_data_size < box->size + 8"
    "libmov Opus extra data capacity")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmov/source/mov-tag.c"
    "i < sizeof(s_tags) / sizeof(s_tags[0])"
    "i < (int)(sizeof(s_tags) / sizeof(s_tags[0]))"
    "libmov object tag lookup bound")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmov/source/mov-vpcc.c"
    "entry->extra_data_size < box->size-4"
    "(uint64_t)entry->extra_data_size < box->size-4"
    "libmov VP codec extra data capacity")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmov/source/mov-writer.c"
    "for (i = 1; object == MOV_OBJECT_CHAPTER && i < mov->mvhd.next_track_ID; i++)\n\t\tmov->tracks[i - 1].chpl_track = mov->mvhd.next_track_ID;"
    "for (i = 1; object == MOV_OBJECT_CHAPTER && i < mov->mvhd.next_track_ID; i++)\n\t{\n\t\tmov->tracks[i - 1].chpl_track = mov->mvhd.next_track_ID;\n\t}"
    "libmov subtitle chapter loop")

patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmpeg/include/mpeg-muxer.h"
    [=[#include "mpeg-ts.h"
#include "mpeg-ts-proto.h"]=]
    [=[#include "mpeg-ts.h" // Public TS declarations also cover the muxer interface.]=]
    "libmpeg deprecated compatibility header")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmpeg/source/mpeg-element-descriptor.c"
    "i < desc.num_sub_profiles && i < sizeof(desc.sub_profile_idc)/sizeof(desc.sub_profile_idc[0])"
    "i < desc.num_sub_profiles && i < (int)(sizeof(desc.sub_profile_idc)/sizeof(desc.sub_profile_idc[0]))"
    "libmpeg VVC descriptor profile bound")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmpeg/source/mpeg-element-descriptor.c"
    "\ttime_t clock;\n"
    "\t/* The parsed calendar fields are validated through mktime below. */\n"
    "libmpeg clock descriptor state")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmpeg/source/mpeg-element-descriptor.c"
    "\tclock = mktime(&t) * 1000;"
    "\t(void)mktime(&t);"
    "libmpeg clock descriptor normalization")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmpeg/source/mpeg-pack-header.c"
    "\tuint8_t stuffing_length;\n\tsize_t header_length;"
    "\tuint8_t stuffing_length;"
    "libmpeg pack header local state")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmpeg/source/mpeg-pack-header.c"
    "\t\theader_length = 14 + stuffing_length;\n\t\tmpeg_bits_skip(reader, stuffing_length);"
    "\t\tmpeg_bits_skip(reader, stuffing_length);"
    "libmpeg pack header stuffing skip")

patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmpeg/source/mpeg-packet.c"
    "pkt->size - size < n"
    "pkt->size - size < (size_t)n"
    "libmpeg H26x trailing NALU bound")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmpeg/source/mpeg-packet.c"
    "assert(r >= 0 && r <= *consume)"
    "assert(r >= 0 && (size_t)r <= *consume)"
    "libmpeg packet consume assertion")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmpeg/source/mpeg-packet.c"
    "r < 0 || r > *consume"
    "r < 0 || (size_t)r > *consume"
    "libmpeg packet consume bound")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmpeg/source/mpeg-packet.c"
    "r >= sizeof(sc_codecid) / sizeof(sc_codecid[0])"
    "r >= (int)(sizeof(sc_codecid) / sizeof(sc_codecid[0]))"
    "libmpeg codec guess bound")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmpeg/source/mpeg-ps-dec.c"
    "assert(r <= ps->buffer.len + (bytes - i));"
    "assert((size_t)r <= ps->buffer.len + (bytes - i));"
    "libmpeg PS buffered header assertion")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmpeg/source/mpeg-ps-dec.c"
    "if (r >= ps->buffer.len)"
    "if ((size_t)r >= ps->buffer.len)"
    "libmpeg PS buffered header bound")
patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmpeg/source/mpeg-ts-h264.c"
    "count < sizeof(h26x[0])/sizeof(h26x[0][0])"
    "count < (int)(sizeof(h26x[0])/sizeof(h26x[0][0]))"
    "libmpeg H26x parameter set bound")

patch_pinned_source(
    "${SOURCE_DIR}/3rdpart/ZLToolKit/src/Network/sockutil.cpp"
    "using getsockname_type = decltype(getsockname);"
    "#if defined(_WIN32)\nusing getsockname_type = decltype(getsockname);\n#else\nusing getsockname_type = int (*)(int, struct sockaddr *, socklen_t *);\n#endif"
    "ZLToolKit socket callback type")
patch_pinned_source(
    "${SOURCE_DIR}/3rdpart/ZLToolKit/src/Poller/Pipe.cpp"
    "#include <fcntl.h>\n"
    "#include <fcntl.h>\n#include <vector>\n"
    "ZLToolKit pipe buffer include")
patch_pinned_source(
    "${SOURCE_DIR}/3rdpart/ZLToolKit/src/Poller/Pipe.cpp"
    "char buf[nread + 1];\n        buf[nread] = '\\0';\n        nread = pipe->read(buf, sizeof(buf));\n        if (cb) {\n            cb(nread, buf);\n        }"
    "std::vector<char> buf(static_cast<size_t>(nread) + 1);\n        buf[static_cast<size_t>(nread)] = '\\0';\n        nread = pipe->read(buf.data(), static_cast<int>(buf.size()));\n        if (cb) {\n            cb(nread, buf.data());\n        }"
    "ZLToolKit POSIX pipe buffer")

patch_pinned_source(
    "${SOURCE_DIR}/ext-codec/MP2VRtp.cpp"
    "payload_size <= (ssize_t)kMP2VHeaderSize"
    "payload_size <= kMP2VHeaderSize"
    "ZLMediaKit MPEG video RTP header bound")
patch_pinned_source(
    "${SOURCE_DIR}/ext-codec/MP2VRtp.cpp"
    "payload_size <= (ssize_t)header_size"
    "payload_size <= header_size"
    "ZLMediaKit MPEG video RTP extension bound")
patch_pinned_source(
    "${SOURCE_DIR}/src/Codec/Transcode.cpp"
    "if ((ret = avfilter_link(buffersrc_ctx, 0, drawtext_ctx1, 0) < 0 || avfilter_link(drawtext_ctx1, 0, buffersink_ctx, 0))< 0) {"
    "if ((ret = avfilter_link(buffersrc_ctx, 0, drawtext_ctx1, 0)) < 0 ||\n        (ret = avfilter_link(drawtext_ctx1, 0, buffersink_ctx, 0)) < 0) {"
    "ZLMediaKit drawtext filter link result")
patch_pinned_source(
    "${SOURCE_DIR}/src/Record/MP4.cpp"
    "if (offset > _memory.size())"
    "if (static_cast<uint64_t>(offset) > _memory.size())"
    "ZLMediaKit MP4 memory seek bound")
patch_pinned_source(
    "${SOURCE_DIR}/src/Record/MP4.cpp"
    "if (_offset >= _memory.size())"
    "if (_offset < 0 || static_cast<uint64_t>(_offset) >= _memory.size())"
    "ZLMediaKit MP4 memory read bound")

patch_pinned_source(
    "${MEDIA_SERVER_ROOT}/libmov/source/mov-writer.c"
    [=[    if (0 != mov_add_subtitle(track, &mov->mvhd, 1000, object, extra_data, extra_data_size))
        return -ENOMEM;]=]
    [=[    if (0 != mov_add_subtitle(track, &mov->mvhd, 1000, object, extra_data, extra_data_size))
    {
        return -ENOMEM;
    }]=]
    "libmov subtitle failure branch")
