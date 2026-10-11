#pragma once
#include <windows.h>
#include <string>
#include <vector>
namespace iotvpn::service_control::services {
struct WindowsServiceSnapshot {
    bool present{}, running{}, delayed{}, failureNonCrash{};
    DWORD type{}, startType{}, errorControl{}, sidType{}, resetPeriod{};
    std::wstring binary, display, account, dependencies, rebootMessage, command;
    std::vector<SC_ACTION> actions;
};
WindowsServiceSnapshot capture(const std::wstring& name);
void restore(const std::wstring& name, const WindowsServiceSnapshot& snapshot);
void validateExisting(const std::wstring& name, const std::wstring& expectedBinaryPath);
void ensureStopped(const std::wstring& name);
void installOrUpdate(const std::wstring& name, const std::wstring& display, const std::wstring& binary, bool delayed, const std::vector<std::wstring>& dependencies);
void start(const std::wstring& name);
void remove(const std::wstring& name);
}
