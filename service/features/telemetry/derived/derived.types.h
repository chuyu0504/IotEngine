#pragma once

#include <functional>
#include <set>
#include <string>

#include "service/features/telemetry/derived/derived.protocol.h"

namespace service::telemetry::derived {

// 一台设备当前派生配置的解析结果与状态键。配置原文不变时复用，配置更新后由持有方重建，
// 避免每个样本重新解析配置、排序派生点和计算状态键摘要。
struct DeviceCalculation final {
    std::string configuration;
    std::string stateKey;
    Calculation calculation;
    std::set<std::string, std::less<>> inputIds;
    bool multipleSources = false;
};

} // namespace service::telemetry::derived
