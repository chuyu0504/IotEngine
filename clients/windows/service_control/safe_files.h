#pragma once
#include <filesystem>
#include <string>

namespace iotvpn::service_control::safe_files {
void checkAncestors(const std::filesystem::path& path);
void checkTree(const std::filesystem::path& path);
void createProtectedDirectory(const std::filesystem::path& path, bool usersRead, bool currentUserOnly = false, bool requireNew = false);
}
