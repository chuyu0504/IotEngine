#include "safe_files.h"

#include "../common/win32.h"
#include <aclapi.h>
#include <sddl.h>

namespace fs = std::filesystem;
namespace iotvpn::service_control::safe_files {
namespace {
struct Descriptor { PSECURITY_DESCRIPTOR value{}; ~Descriptor() { LocalFree(value); } };
void rejectReparse(const fs::path& path) {
    const DWORD attr = GetFileAttributesW(path.c_str());
    if (attr == INVALID_FILE_ATTRIBUTES) {
        auto error = GetLastError();
        if (error != ERROR_FILE_NOT_FOUND && error != ERROR_PATH_NOT_FOUND) win32Error("Cannot inspect installation path", error);
    } else if (attr & FILE_ATTRIBUTE_REPARSE_POINT) throw std::runtime_error("安装路径包含链接或重解析点，已停止操作。");
}
std::wstring aclText(bool usersRead, bool currentUserOnly) {
    if (!currentUserOnly) return L"O:BAG:BAD:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)" + std::wstring(usersRead ? L"(A;OICI;GRGX;;;BU)" : L"");
    Handle token;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token.value)) win32Error("Cannot read setup identity");
    DWORD size = 0; GetTokenInformation(token.value, TokenUser, nullptr, 0, &size);
    std::vector<BYTE> data(size);
    if (!GetTokenInformation(token.value, TokenUser, data.data(), size, &size)) win32Error("Cannot read setup identity");
    LPWSTR sid = nullptr;
    if (!ConvertSidToStringSidW(reinterpret_cast<TOKEN_USER*>(data.data())->User.Sid, &sid)) win32Error("Cannot encode setup identity");
    const std::wstring user(sid); LocalFree(sid);
    return L"O:" + user + L"D:P(A;OICI;FA;;;" + user + L")(A;OICI;FA;;;SY)";
}
void applyAcl(const fs::path& path, PSECURITY_DESCRIPTOR descriptor) {
    PSID owner{}; BOOL unused{}; PACL acl{};
    if (!GetSecurityDescriptorOwner(descriptor, &owner, &unused) || !GetSecurityDescriptorDacl(descriptor, &unused, &acl, &unused))
        win32Error("Cannot read setup permissions");
    const DWORD error = SetNamedSecurityInfoW(const_cast<LPWSTR>(path.c_str()), SE_FILE_OBJECT,
        OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION, owner, nullptr, acl, nullptr);
    if (error != ERROR_SUCCESS) win32Error("Cannot protect installation files", error);
}
}
void checkAncestors(const fs::path& path) {
    auto current = fs::absolute(path).lexically_normal();
    for (;;) { rejectReparse(current); auto parent = current.parent_path(); if (parent == current) break; current = parent; }
}
void checkTree(const fs::path& path) {
    checkAncestors(path);
    if (!fs::exists(path)) return;
    if (!fs::is_directory(path)) throw std::runtime_error("安装路径不是目录。");
    for (const auto& entry : fs::recursive_directory_iterator(path)) rejectReparse(entry.path());
}
void createProtectedDirectory(const fs::path& path, bool usersRead, bool currentUserOnly, bool requireNew) {
    checkTree(path);
    Descriptor descriptor;
    const auto sddl = aclText(usersRead, currentUserOnly);
    if (!ConvertStringSecurityDescriptorToSecurityDescriptorW(sddl.c_str(), SDDL_REVISION_1, &descriptor.value, nullptr))
        win32Error("Cannot create setup permissions");
    SECURITY_ATTRIBUTES attributes{sizeof(attributes), descriptor.value, FALSE};
    if ((requireNew || !fs::exists(path)) && !CreateDirectoryW(path.c_str(), &attributes)) win32Error("Cannot create protected setup directory");
    applyAcl(path, descriptor.value);
    for (const auto& entry : fs::recursive_directory_iterator(path)) applyAcl(entry.path(), descriptor.value);
}
}
