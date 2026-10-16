#pragma once

#include <algorithm>
#include <array>
#include <string_view>
#include <string>

namespace service::command {

inline constexpr std::array<std::string_view, 2> kCommandResultAcceptingStatuses{
    "DISPATCHING", "AWAITING_RESULT"};

inline std::string_view expiredDispatchState(bool claimPersisted) {
    return claimPersisted ? "UNKNOWN" : "REJECTED";
}

inline bool terminalState(std::string_view state) {
    return state == "SUCCEEDED" || state == "REJECTED" || state == "UNKNOWN" ||
           state == "READBACK_MISMATCH" || state == "FAILED";
}

inline bool retryableFailure(std::string_view reason) {
    static constexpr std::array<std::string_view, 12> permanentMarkers{
        "invalid", "required", "not_supported", "mismatch", "conflict", "queue_full",
        "busy", "negative_ack", "protocol_route_mismatch", "stale_session_epoch",
        "readback", "write_ack_missing"};
    const auto contains = [reason](std::string_view marker) {
        return reason.find(marker) != std::string_view::npos;
    };
    if (std::any_of(permanentMarkers.begin(), permanentMarkers.end(), contains))
        return false;
    return contains("timeout") || contains("temporarily_unavailable") || contains("redis");
}

// A timeout or failed readback is not evidence that a physical write did not happen.
inline std::string_view collectorResultState(bool success, std::string_view reason) {
    if (!success && reason.starts_with("device_negative_ack:")) return "FAILED";
    if (reason.find("write_ack_missing") != std::string_view::npos)
        return "UNKNOWN";
    if (reason.find("readback_mismatch") != std::string_view::npos)
        return "READBACK_MISMATCH";
    if (reason.find("readback") != std::string_view::npos)
        return "UNKNOWN";
    if (success) return "SUCCEEDED";
    if (reason == "command_invalid" || reason.starts_with("command_invalid:") ||
        reason == "stale_session_epoch" || reason == "dispatch_deadline_expired" ||
        reason == "connection_route_missing" || reason.ends_with("_device_offline"))
        return "REJECTED";
    return "UNKNOWN";
}

} // namespace service::command
