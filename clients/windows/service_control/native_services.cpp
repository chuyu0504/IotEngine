#include "native_services.h"
#include "../common/win32.h"
#include <chrono>
#include <thread>

namespace iotvpn::service_control::services {
namespace {
ServiceHandle manager(DWORD access = SC_MANAGER_CONNECT) {
    const auto result = OpenSCManagerW(nullptr, nullptr, access);
    if (!result) win32Error("Cannot open service manager");
    return ServiceHandle(result);
}
ServiceHandle open(SC_HANDLE scm, const std::wstring& name, DWORD access) {
    auto value = OpenServiceW(scm, name.c_str(), access);
    if (!value && GetLastError() != ERROR_SERVICE_DOES_NOT_EXIST) win32Error("Cannot open service");
    return ServiceHandle(value);
}
SERVICE_STATUS_PROCESS status(SC_HANDLE handle) {
    SERVICE_STATUS_PROCESS result{}; DWORD size{};
    if (!QueryServiceStatusEx(handle, SC_STATUS_PROCESS_INFO, reinterpret_cast<BYTE*>(&result), sizeof(result), &size)) win32Error("Cannot query service state");
    return result;
}
void waitFor(SC_HANDLE handle, DWORD wanted) {
    const auto deadline = GetTickCount64() + 35000;
    for (;;) {
        auto current = status(handle);
        if (current.dwCurrentState == wanted) return;
        if (wanted == SERVICE_RUNNING && current.dwCurrentState == SERVICE_STOPPED) throw std::runtime_error("网络服务启动失败，请检查系统事件日志。");
        if (GetTickCount64() >= deadline) throw std::runtime_error("等待网络服务超时。");
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }
}
std::vector<BYTE> query2(SC_HANDLE service, DWORD level) {
    DWORD size{}; QueryServiceConfig2W(service, level, nullptr, 0, &size);
    if (GetLastError() != ERROR_INSUFFICIENT_BUFFER) win32Error("Cannot query service configuration");
    std::vector<BYTE> bytes(size);
    if (!QueryServiceConfig2W(service, level, bytes.data(), size, &size)) win32Error("Cannot query service configuration");
    return bytes;
}
void config2(SC_HANDLE service, DWORD level, void* data) {
    if (!ChangeServiceConfig2W(service, level, data)) win32Error("Cannot configure network service");
}
void configure(SC_HANDLE handle, const WindowsServiceSnapshot& value) {
    if (!ChangeServiceConfigW(handle, value.type, value.startType, value.errorControl, value.binary.c_str(), nullptr, nullptr,
        value.dependencies.c_str(), L"LocalSystem", nullptr, value.display.c_str())) win32Error("Cannot update network service");
    SERVICE_DELAYED_AUTO_START_INFO delayed{value.delayed}; config2(handle, SERVICE_CONFIG_DELAYED_AUTO_START_INFO, &delayed);
    SERVICE_SID_INFO sid{value.sidType}; config2(handle, SERVICE_CONFIG_SERVICE_SID_INFO, &sid);
    SERVICE_FAILURE_ACTIONS_FLAG flag{value.failureNonCrash}; config2(handle, SERVICE_CONFIG_FAILURE_ACTIONS_FLAG, &flag);
    SERVICE_FAILURE_ACTIONSW failure{value.resetPeriod, const_cast<LPWSTR>(value.rebootMessage.c_str()), const_cast<LPWSTR>(value.command.c_str()),
        static_cast<DWORD>(value.actions.size()), const_cast<SC_ACTION*>(value.actions.data())};
    config2(handle, SERVICE_CONFIG_FAILURE_ACTIONS, &failure);
}
}
WindowsServiceSnapshot capture(const std::wstring& name) {
    auto scm = manager(); auto service = open(scm.value, name, SERVICE_QUERY_CONFIG | SERVICE_QUERY_STATUS);
    if (!service) return {};
    WindowsServiceSnapshot result; result.present = true; result.running = status(service.value).dwCurrentState != SERVICE_STOPPED;
    DWORD size{}; QueryServiceConfigW(service.value, nullptr, 0, &size);
    if (GetLastError() != ERROR_INSUFFICIENT_BUFFER) win32Error("Cannot inspect network service");
    std::vector<BYTE> bytes(size); auto config = reinterpret_cast<QUERY_SERVICE_CONFIGW*>(bytes.data());
    if (!QueryServiceConfigW(service.value, config, size, &size)) win32Error("Cannot inspect network service");
    result.type = config->dwServiceType; result.startType = config->dwStartType; result.errorControl = config->dwErrorControl;
    result.binary = config->lpBinaryPathName; result.display = config->lpDisplayName; result.account = config->lpServiceStartName;
    for (auto p = config->lpDependencies; p && *p; p += wcslen(p) + 1) { result.dependencies.append(p); result.dependencies += L'\0'; }
    result.dependencies += L'\0';
    auto delayed = query2(service.value, SERVICE_CONFIG_DELAYED_AUTO_START_INFO); result.delayed = reinterpret_cast<SERVICE_DELAYED_AUTO_START_INFO*>(delayed.data())->fDelayedAutostart;
    auto sid = query2(service.value, SERVICE_CONFIG_SERVICE_SID_INFO); result.sidType = reinterpret_cast<SERVICE_SID_INFO*>(sid.data())->dwServiceSidType;
    auto flag = query2(service.value, SERVICE_CONFIG_FAILURE_ACTIONS_FLAG); result.failureNonCrash = reinterpret_cast<SERVICE_FAILURE_ACTIONS_FLAG*>(flag.data())->fFailureActionsOnNonCrashFailures;
    auto failure = query2(service.value, SERVICE_CONFIG_FAILURE_ACTIONS); auto actions = reinterpret_cast<SERVICE_FAILURE_ACTIONSW*>(failure.data());
    result.resetPeriod = actions->dwResetPeriod;
    if (actions->lpRebootMsg) result.rebootMessage = actions->lpRebootMsg;
    if (actions->lpCommand) result.command = actions->lpCommand;
    if (actions->cActions) result.actions.assign(actions->lpsaActions, actions->lpsaActions + actions->cActions);
    return result;
}
void validateExisting(const std::wstring& name, const std::wstring& binary) {
    const auto old = capture(name);
    if (old.present && (_wcsicmp(old.binary.c_str(), binary.c_str()) || _wcsicmp(old.account.c_str(), L"LocalSystem") || old.type != SERVICE_WIN32_OWN_PROCESS))
        throw std::runtime_error("同名服务不属于此安装位置，已停止操作。");
}
void ensureStopped(const std::wstring& name) {
    auto scm = manager(); auto service = open(scm.value, name, SERVICE_QUERY_STATUS | SERVICE_STOP);
    if (!service) return;
    const auto before = status(service.value); const auto current = before.dwCurrentState;
    Handle process(before.dwProcessId ? OpenProcess(SYNCHRONIZE, FALSE, before.dwProcessId) : nullptr);
    if (current != SERVICE_STOPPED && current != SERVICE_STOP_PENDING) {
        SERVICE_STATUS out{};
        if (!ControlService(service.value, SERVICE_CONTROL_STOP, &out) && GetLastError() != ERROR_SERVICE_NOT_ACTIVE) win32Error("Cannot stop network service");
    }
    waitFor(service.value, SERVICE_STOPPED);
    if (process && WaitForSingleObject(process.value, 10000) != WAIT_OBJECT_0)
        throw std::runtime_error("服务已停止，但进程尚未释放程序文件，请稍后重试。");
}
void start(const std::wstring& name) {
    auto scm = manager(); auto service = open(scm.value, name, SERVICE_QUERY_STATUS | SERVICE_START);
    if (!service) throw std::runtime_error("网络服务未安装。");
    if (status(service.value).dwCurrentState != SERVICE_RUNNING && !StartServiceW(service.value, 0, nullptr) && GetLastError() != ERROR_SERVICE_ALREADY_RUNNING) win32Error("Cannot start network service");
    waitFor(service.value, SERVICE_RUNNING);
}
void remove(const std::wstring& name) {
    ensureStopped(name);
    { auto scm = manager(); auto service = open(scm.value, name, DELETE);
      if (service && !DeleteService(service.value) && GetLastError() != ERROR_SERVICE_MARKED_FOR_DELETE) win32Error("Cannot remove network service"); }
    const auto deadline = GetTickCount64() + 35000;
    for (;;) {
        auto scm = manager(); ServiceHandle service(OpenServiceW(scm.value, name.c_str(), SERVICE_QUERY_STATUS));
        if (!service && GetLastError() == ERROR_SERVICE_DOES_NOT_EXIST) return;
        if (GetTickCount64() >= deadline) throw std::runtime_error("服务等待系统释放，请重启后重试。");
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}
void restore(const std::wstring& name, const WindowsServiceSnapshot& saved) {
    if (!saved.present) { remove(name); return; }
    auto scm = manager(SC_MANAGER_CONNECT | SC_MANAGER_CREATE_SERVICE);
    auto service = open(scm.value, name, SERVICE_ALL_ACCESS);
    if (!service) {
        ServiceHandle created(CreateServiceW(scm.value, name.c_str(), saved.display.c_str(), SERVICE_ALL_ACCESS, saved.type, saved.startType, saved.errorControl,
            saved.binary.c_str(), nullptr, nullptr, saved.dependencies.c_str(), L"LocalSystem", nullptr));
        if (!created) win32Error("Cannot restore network service");
        configure(created.value, saved);
    } else configure(service.value, saved);
}
void installOrUpdate(const std::wstring& name, const std::wstring& display, const std::wstring& binary, bool delayed, const std::vector<std::wstring>& dependencies) {
    validateExisting(name, binary);
    WindowsServiceSnapshot target; target.present = true; target.type = SERVICE_WIN32_OWN_PROCESS; target.startType = delayed ? SERVICE_AUTO_START : SERVICE_DEMAND_START;
    target.errorControl = SERVICE_ERROR_NORMAL; target.binary = binary; target.display = display; target.delayed = delayed; target.sidType = SERVICE_SID_TYPE_UNRESTRICTED;
    target.failureNonCrash = delayed; target.resetPeriod = 86400;
    if (delayed) target.actions = {{SC_ACTION_RESTART, 5000}, {SC_ACTION_RESTART, 15000}, {SC_ACTION_RESTART, 60000}};
    else target.actions = {{SC_ACTION_NONE, 0}};
    for (const auto& dep : dependencies) { target.dependencies += dep; target.dependencies += L'\0'; } target.dependencies += L'\0';
    restore(name, target);
}
}
