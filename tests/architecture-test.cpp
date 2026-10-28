#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <iterator>
#include <optional>
#include <regex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "service/common/message.h"
#include "service/features/observability/observability.runtime.h"
#include "service/utils/worker_task.h"

namespace {

void require(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

std::filesystem::path repositoryRoot() {
    return std::filesystem::path(__FILE__).parent_path().parent_path();
}

std::string source(std::string_view relative) {
    const auto path = std::filesystem::path(__FILE__).parent_path().parent_path() /
        std::filesystem::path(relative);
    std::ifstream input(path, std::ios::binary);
    require(input.good(), "cannot read architecture source");
    return { std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>() };
}

void requireFeatureRule(bool condition, const std::filesystem::path& path, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message) + ": " + path.generic_string());
    }
}

bool isSnakeCase(std::string_view value) {
    if (value.empty() || value.front() < 'a' || value.front() > 'z' ||
        value.back() == '_') {
        return false;
    }
    bool previousUnderscore = false;
    for (const auto character : value) {
        if (character == '_') {
            if (previousUnderscore) {
                return false;
            }
            previousUnderscore = true;
            continue;
        }
        if ((character < 'a' || character > 'z') &&
            (character < '0' || character > '9')) {
            return false;
        }
        previousUnderscore = false;
    }
    return true;
}

template <std::size_t Size>
bool contains(const std::array<std::string_view, Size>& values, std::string_view value) {
    for (const auto candidate : values) {
        if (candidate == value) {
            return true;
        }
    }
    return false;
}

bool isArchitectureSource(const std::filesystem::path& path) {
    static constexpr std::array<std::string_view, 7> extensions{
        ".h",
        ".hpp",
        ".hh",
        ".hxx",
        ".cpp",
        ".cc",
        ".cxx",
    };
    return contains(extensions, path.extension().string());
}

std::string pathKey(const std::filesystem::path& path) {
    return path.lexically_relative(repositoryRoot()).generic_string();
}

void requireExpectedModule(std::string_view domain, std::string_view module, const std::filesystem::path& path) {
    requireFeatureRule(isSnakeCase(domain), path, "invalid module directory name");
    requireFeatureRule(domain == "system" ? isSnakeCase(module) : module.empty(), path, "only system modules may have a grouping directory");
}

bool whitelistedModuleFile(std::string_view file, std::string_view owner) {
    static constexpr std::array<std::string_view, 5> suffixes{
        ".types.h",
        ".service.h",
        ".entity.h",
        ".error.h",
        ".controller.h",
    };
    for (const auto suffix : suffixes) {
        if (file == std::string(owner) + std::string(suffix)) {
            return true;
        }
    }
    return false;
}

void testServiceTopLevelLayout() {
    const auto root = repositoryRoot() / "service";
    static constexpr std::array<std::string_view, 7> allowed{
        "common",
        "config",
        "features",
        "middleware",
        "modules",
        "utils",
        "server.cpp",
    };
    require(std::filesystem::is_directory(root), "service directory is missing");
    for (const auto& entry : std::filesystem::directory_iterator(root)) {
        requireFeatureRule(contains(allowed, entry.path().filename().string()), entry.path(), "service top-level entry is not whitelisted");
    }
    for (const auto requiredDirectory : { "common", "config", "features", "middleware", "modules", "utils" }) {
        requireFeatureRule(std::filesystem::is_directory(root / requiredDirectory), root / requiredDirectory, "required service top-level directory is missing");
    }
    requireFeatureRule(std::filesystem::is_regular_file(root / "server.cpp"), root / "server.cpp", "service/server.cpp is missing");
}

bool whitelistedFeatureFile(std::string_view file, std::string_view owner) {
    static constexpr std::array<std::string_view, 12> suffixes{
        ".runtime.h",
        ".runtime.cpp",
        ".service.h",
        ".types.h",
        ".entity.h",
        ".config.h",
        ".protocol.h",
        ".protocol.cpp",
        ".transport.h",
        ".transport.cpp",
        ".error.h",
        ".proto",
    };
    for (const auto suffix : suffixes) {
        if (file == std::string(owner) + std::string(suffix)) {
            return true;
        }
    }
    return false;
}

std::vector<std::filesystem::path> featureFiles() {
    const auto root = repositoryRoot() / "service" / "features";
    require(std::filesystem::is_directory(root), "service/features directory is missing");
    std::vector<std::filesystem::path> result;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (entry.is_regular_file()) {
            result.push_back(entry.path());
        }
    }
    return result;
}

bool declaresPrivateExecutor(const std::string& code) {
    static const std::regex declaration(
        R"(^\s*(?:(?:static|inline)\s+)*(?:std::(?:jthread|thread)|asio::io_context|ruvia::event_loop_pool)\s+[A-Za-z_]\w*\s*[;{(=])",
        std::regex_constants::ECMAScript
    );
    for (std::size_t start = 0; start < code.size();) {
        const auto end = code.find('\n', start);
        if (std::regex_search(code.substr(start, end - start), declaration)) {
            return true;
        }
        if (end == std::string::npos) {
            break;
        }
        start = end + 1;
    }
    return false;
}

void testModuleExecutorDetection() {
    for (const auto* declaration : {
             "    std::thread outbound_;",
             "std::jthread worker([] {});",
             "asio::io_context io;",
             "ruvia::event_loop_pool pool({.loop_count_ = 1});" }) {
        require(declaresPrivateExecutor(declaration), "module executor declaration was missed");
    }
    for (const auto* allowed : {
             "// std::thread example;",
             "std::thread::id owner;",
             "auto client = context.http_client();" }) {
        require(!declaresPrivateExecutor(allowed), "non-owning worker usage was rejected");
    }
}

void testModuleFileLayout() {
    const auto root = repositoryRoot() / "service" / "modules";
    require(std::filesystem::is_directory(root), "service/modules directory is missing");
    const auto validateModuleFiles = [](const std::filesystem::path& modulePath,
                                        std::string_view moduleName) {
        for (const auto& entry : std::filesystem::directory_iterator(modulePath)) {
            requireFeatureRule(entry.is_regular_file(), entry.path(), "module files may not contain nested directories");
            const auto file = entry.path().filename().string();
            requireFeatureRule(file.starts_with(std::string(moduleName) + "."), entry.path(), "module file name must use its module prefix");
            requireFeatureRule(whitelistedModuleFile(file, moduleName), entry.path(), "module file name or extension is not whitelisted");
            const auto code = source(entry.path().generic_string());
            requireFeatureRule(!declaresPrivateExecutor(code), entry.path(), "module must use its accepting Worker instead of owning a thread or event loop");
        }
    };

    for (const auto& entry : std::filesystem::directory_iterator(root)) {
        requireFeatureRule(entry.is_directory(), entry.path(), "module entries must be directories");
        const auto entryName = entry.path().filename().string();
        requireFeatureRule(isSnakeCase(entryName), entry.path(), "module directory must use snake_case");
        if (entryName == "system") {
            for (const auto& module : std::filesystem::directory_iterator(entry.path())) {
                requireFeatureRule(module.is_directory(), module.path(), "modules must be directories under system");
                const auto moduleName = module.path().filename().string();
                requireFeatureRule(isSnakeCase(moduleName), module.path(), "module directory must use snake_case");
                requireExpectedModule(entryName, moduleName, module.path());
                validateModuleFiles(module.path(), moduleName);
            }
        } else {
            requireExpectedModule(entryName, std::string_view{}, entry.path());
            validateModuleFiles(entry.path(), entryName);
        }
    }
}

void testFeatureFileLayout() {
    const auto root = repositoryRoot() / "service" / "features";
    const auto files = featureFiles();
    require(!files.empty(), "service/features contains no source files");
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (entry.is_directory()) {
            requireFeatureRule(isSnakeCase(entry.path().filename().string()), entry.path(), "feature directory must use snake_case");
        }
    }
    for (const auto& path : files) {
        const auto relative = path.lexically_relative(root);
        std::vector<std::string> parts;
        for (const auto& part : relative) {
            parts.emplace_back(part.string());
        }
        requireFeatureRule(parts.size() <= 3, path, "feature components may not be nested beyond one directory");
        requireFeatureRule(parts.size() == 2 || parts.size() == 3, path, "feature files must be directly under a feature or component");

        const auto& feature = parts.front();
        const auto& owner = parts.size() == 2 ? feature : parts[1];
        const auto& file = parts.back();
        const auto prefix = owner + ".";
        requireFeatureRule(file.starts_with(prefix), path, "feature file name must use its feature/component prefix");
        requireFeatureRule(whitelistedFeatureFile(file, owner), path, "feature file name or extension is not whitelisted");
    }
}

void testCommonFileLayout() {
    const auto root = repositoryRoot() / "service" / "common";
    require(std::filesystem::is_directory(root), "service/common directory is missing");
    for (const auto& entry : std::filesystem::directory_iterator(root)) {
        requireFeatureRule(entry.is_regular_file(), entry.path(), "service/common may contain foundation files only, no directories");
    }
}

struct IncludeGraph {
    std::vector<std::filesystem::path> files;
    std::unordered_map<std::string, std::filesystem::path> paths;
    std::unordered_map<std::string, std::vector<std::string>> includes;
};

std::vector<std::filesystem::path> serviceSourceFiles() {
    const auto root = repositoryRoot() / "service";
    require(std::filesystem::is_directory(root), "service directory is missing");
    std::vector<std::filesystem::path> result;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (entry.is_regular_file() && isArchitectureSource(entry.path())) {
            result.push_back(entry.path());
        }
    }
    return result;
}

std::optional<std::string> includeTarget(std::string_view line) {
    auto cursor = line.find_first_not_of(" \t\r");
    if (cursor == std::string_view::npos || line[cursor] != '#') {
        return std::nullopt;
    }
    ++cursor;
    while (cursor < line.size() &&
           std::isspace(static_cast<unsigned char>(line[cursor]))) {
        ++cursor;
    }
    constexpr std::string_view directive = "include";
    if (line.substr(cursor, directive.size()) != directive) {
        return std::nullopt;
    }
    cursor += directive.size();
    if (cursor < line.size() && !std::isspace(static_cast<unsigned char>(line[cursor])) &&
        line[cursor] != '"' && line[cursor] != '<') {
        return std::nullopt;
    }
    while (cursor < line.size() &&
           std::isspace(static_cast<unsigned char>(line[cursor]))) {
        ++cursor;
    }
    if (cursor == line.size() || (line[cursor] != '"' && line[cursor] != '<')) {
        return std::nullopt;
    }
    const auto closing = line[cursor] == '"' ? '"' : '>';
    ++cursor;
    const auto end = line.find(closing, cursor);
    if (end == std::string_view::npos) {
        return std::nullopt;
    }
    return std::string(line.substr(cursor, end - cursor));
}

struct SourceStructure {
    std::vector<std::string> includes;
    bool includeAfterDeclaration{ false };
    bool declaresRoutes{ false };
};

SourceStructure sourceStructure(std::string_view input) {
    // 先执行预处理续行，再屏蔽注释和字面量；两份视图保持位置一致，
    // include 路径从保留字面量的视图读取，结构只从代码 token 判断。
    std::string text;
    text.reserve(input.size());
    for (std::size_t cursor = 0; cursor < input.size(); ++cursor) {
        if (input[cursor] == '\\' && cursor + 1 < input.size()) {
            if (input[cursor + 1] == '\n') {
                ++cursor;
                continue;
            }
            if (input[cursor + 1] == '\r' && cursor + 2 < input.size() &&
                input[cursor + 2] == '\n') {
                cursor += 2;
                continue;
            }
        }
        text.push_back(input[cursor]);
    }
    auto tokens = text;
    const auto mask = [](std::string& value, std::size_t begin, std::size_t end) {
        for (auto cursor = begin; cursor < end; ++cursor) {
            if (value[cursor] != '\n') {
                value[cursor] = ' ';
            }
        }
    };
    for (std::size_t cursor = 0; cursor < text.size();) {
        const auto begin = cursor;
        if (text.compare(cursor, 2, "//") == 0) {
            const auto end = text.find('\n', cursor + 2);
            cursor = end == std::string::npos ? text.size() : end;
            mask(tokens, begin, cursor);
            mask(text, begin, cursor);
        } else if (text.compare(cursor, 2, "/*") == 0) {
            const auto end = text.find("*/", cursor + 2);
            cursor = end == std::string::npos ? text.size() : end + 2;
            mask(tokens, begin, cursor);
            mask(text, begin, cursor);
        } else if (text.compare(cursor, 2, "R\"") == 0) {
            const auto opening = text.find('(', cursor + 2);
            if (opening == std::string::npos || opening - cursor > 18) {
                ++cursor;
                continue;
            }
            const auto delimiter = text.substr(cursor + 2, opening - cursor - 2);
            const auto closing = ")" + delimiter + "\"";
            const auto end = text.find(closing, opening + 1);
            cursor = end == std::string::npos ? text.size() : end + closing.size();
            mask(tokens, begin, cursor);
        } else if (text[cursor] == '"' || text[cursor] == '\'') {
            // 数字分隔符不是字符字面量。
            auto tokenStart = cursor;
            while (tokenStart > 0 &&
                   (std::isalnum(static_cast<unsigned char>(text[tokenStart - 1])) ||
                    text[tokenStart - 1] == '_' || text[tokenStart - 1] == '\'')) {
                --tokenStart;
            }
            if (text[cursor] == '\'' && tokenStart < cursor &&
                std::isdigit(static_cast<unsigned char>(text[tokenStart]))) {
                ++cursor;
                continue;
            }
            const auto quote = text[cursor++];
            while (cursor < text.size()) {
                if (text[cursor] == '\\' && cursor + 1 < text.size()) {
                    cursor += 2;
                } else if (text[cursor++] == quote) {
                    break;
                }
            }
            mask(tokens, begin, cursor);
        } else {
            ++cursor;
        }
    }

    static const std::regex routeDeclaration(
        R"(\bRUVIA_(?:CONTROLLER_GROUP|ROUTES_BEGIN|ROUTES_END|GROUP_BEGIN|GROUP_END|GET(?:_STREAM|_SSE|_WS(?:_OPTIONS)?)?|POST(?:_STREAM)?|PUT(?:_STREAM)?|PATCH(?:_STREAM)?|DELETE|HEAD|OPTIONS|CONNECT(?:_PROTOCOL)?(?:_OPTIONS)?|METHOD|ALL|ON)\b)"
    );

    struct ConditionalBranch {
        bool before;
        bool previous;
    };

    std::vector<ConditionalBranch> branches;
    bool declarationSeen = false;
    SourceStructure result;
    for (std::size_t start = 0; start < tokens.size();) {
        const auto end = tokens.find('\n', start);
        const auto length = end == std::string::npos ? tokens.size() - start : end - start;
        const auto line = std::string_view(tokens).substr(start, length);
        auto cursor = line.find_first_not_of(" \t\r");
        if (cursor != std::string_view::npos && line[cursor] == '#') {
            cursor = line.find_first_not_of(" \t\r", cursor + 1);
            const auto directiveEnd = cursor == std::string_view::npos
                ? cursor
                : line.find_first_not_of("abcdefghijklmnopqrstuvwxyz_", cursor);
            const auto directive = cursor == std::string_view::npos
                ? std::string_view{}
                : line.substr(cursor, directiveEnd - cursor);
            if (directive == "define") {
                result.declaresRoutes |= std::regex_search(line.begin(), line.end(), routeDeclaration);
            }
            if (directive == "include") {
                result.includeAfterDeclaration |= declarationSeen;
                if (const auto target = includeTarget(std::string_view(text).substr(start, length))) {
                    result.includes.push_back(*target);
                }
            } else if (directive == "if" || directive == "ifdef" || directive == "ifndef") {
                branches.push_back({ declarationSeen, false });
            } else if ((directive == "else" || directive == "elif") && !branches.empty()) {
                auto& branch = branches.back();
                branch.previous |= declarationSeen;
                declarationSeen = branch.before;
            } else if (directive == "endif" && !branches.empty()) {
                declarationSeen |= branches.back().previous;
                branches.pop_back();
            }
        } else if (cursor != std::string_view::npos) {
            declarationSeen = true;
            result.declaresRoutes |= std::regex_search(line.begin(), line.end(), routeDeclaration);
        }
        if (end == std::string::npos) {
            break;
        }
        start = end + 1;
    }
    return result;
}

void testSourceStructureDetection() {
    const auto header = sourceStructure(
        "/* #include \"ignored.h\" */\n"
        "#pragma once\n#define FLAG \\\n 1\n"
        "#if defined(PLATFORM)\n#include \"first.h\"\nstruct First {};\n"
        "#else\n#include \"second.h\"\nstruct Second {};\n#endif\n"
        "constexpr auto text = R\"code(\n#include \"ignored.h\"\n"
        "RUVIA_ROUTES_BEGIN(Ignored)\n)code\";\n"
        "constexpr auto letter = L'\\'';\nconstexpr auto count = 1'000;\n"
        "// RUVIA_GET(\"/ignored\", ignored)\n"
    );
    require(header.includes == std::vector<std::string>{ "first.h", "second.h" }, "include lexer reads comments or raw string contents");
    require(!header.includeAfterDeclaration, "mutually exclusive preprocessor branches reject header includes");
    require(!header.declaresRoutes, "route lexer reads comments or string literals");
    for (const auto* code : {
             "struct Value {};\n#include \"late.h\"\n",
             "constexpr auto letter = L'#';\n#include <late.h>\n",
             "constexpr auto count = 1'000;\n#include <late.h>\n",
             "#if FLAG\nstruct Value {};\n#endif\n#include <late.h>\n",
             "namespace example {\n#if FLAG\n# include HEADER\n#endif\n}\n" }) {
        require(sourceStructure(code).includeAfterDeclaration, "declaration followed by an include was missed");
    }
    for (const auto* code : {
             "RUVIA_ROUTES_BEGIN(Endpoint)\nRUVIA_ROUTES_END",
             "RUVIA_GET(\"/events\", events)",
             "RUVIA_CONNECT_PROTOCOL_OPTIONS(\"protocol\", \"/\", stream, options)",
             "RUVIA_GROUP_BEGIN(\"/group\")\nRUVIA_GROUP_END" }) {
        require(sourceStructure(code).declaresRoutes, "structural routing macro was missed");
    }
    require(!sourceStructure("void route();\nvoid events();\n").declaresRoutes, "business function spelling was mistaken for a route declaration");
    require(!sourceStructure("#ifdef RUVIA_ROUTES_END\n#endif\n").declaresRoutes, "preprocessor macro availability check was mistaken for a route declaration");
}

std::optional<std::string> resolveInclude(
    const std::filesystem::path& including,
    std::string_view spelling,
    const std::unordered_map<std::string, std::filesystem::path>& known
) {
    std::string normalized(spelling);
    for (auto& character : normalized) {
        if (character == '\\') {
            character = '/';
        }
    }

    std::vector<std::filesystem::path> candidates;
    const auto target = std::filesystem::path(normalized);
    if (normalized.starts_with("service/")) {
        candidates.push_back(repositoryRoot() / target);
    } else {
        candidates.push_back(including.parent_path() / target);
        candidates.push_back(repositoryRoot() / "service" / target);
        candidates.push_back(repositoryRoot() / target);
    }

    for (const auto& candidate : candidates) {
        const auto lookup = [&](const std::filesystem::path& path)
            -> std::optional<std::string> {
            const auto key = pathKey(path.lexically_normal());
            if (known.contains(key)) {
                return key;
            }
            return std::nullopt;
        };
        if (const auto result = lookup(candidate)) {
            return result;
        }
        if (!candidate.has_extension()) {
            for (const auto suffix : { ".h", ".hpp", ".hh", ".hxx", ".cpp", ".cc", ".cxx" }) {
                auto withSuffix = candidate;
                withSuffix += suffix;
                if (const auto result = lookup(withSuffix)) {
                    return result;
                }
            }
        }
    }
    return std::nullopt;
}

bool isLayerReference(std::string_view spelling) {
    std::string normalized(spelling);
    for (auto& character : normalized) {
        if (character == '\\') {
            character = '/';
        }
    }
    return normalized.starts_with("service/modules/") ||
        normalized.starts_with("service/features/");
}

IncludeGraph buildIncludeGraph() {
    IncludeGraph graph;
    graph.files = serviceSourceFiles();
    require(!graph.files.empty(), "service contains no C++ source files");
    for (const auto& path : graph.files) {
        const auto key = pathKey(path);
        graph.paths.emplace(key, path);
        graph.includes.emplace(key, std::vector<std::string>{});
    }

    for (const auto& path : graph.files) {
        std::ifstream input(path, std::ios::binary);
        requireFeatureRule(input.good(), path, "cannot read service source for include graph");
        const auto from = pathKey(path);
        const std::string content{
            std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()
        };
        const auto structure = sourceStructure(content);
        requireFeatureRule(!structure.includeAfterDeclaration, path, "include appears after a declaration");
        const bool routeOwner = (from.starts_with("service/modules/") && from.ends_with(".controller.h")) ||
            (from.starts_with("service/features/") && from.ends_with(".transport.h"));
        requireFeatureRule(!structure.declaresRoutes || routeOwner, path, "routing macros declared outside a module controller or feature transport header");
        for (const auto& target : structure.includes) {
            const auto resolved = resolveInclude(path, target, graph.paths);
            if (!resolved) {
                requireFeatureRule(!isLayerReference(target), path, "service layer include target does not exist");
                continue;
            }
            auto& edges = graph.includes.at(from);
            if (std::find(edges.begin(), edges.end(), *resolved) == edges.end()) {
                edges.push_back(*resolved);
            }
        }
    }
    return graph;
}

bool isModulePath(std::string_view key) {
    return key.starts_with("service/modules/");
}

bool isFeaturePath(std::string_view key) {
    return key.starts_with("service/features/");
}

bool isRuntimePath(std::string_view key) {
    const auto separator = key.find_last_of('/');
    const auto filename = key.substr(separator == std::string_view::npos ? 0 : separator + 1);
    return filename.ends_with(".runtime.h") || filename.ends_with(".runtime.cpp");
}

bool isImplementationPath(std::string_view key) {
    return isRuntimePath(key) || key.ends_with(".service.h") ||
        key.ends_with(".controller.h") || key.ends_with(".transport.h") ||
        key.ends_with(".transport.cpp");
}

std::string_view featureOwner(std::string_view key) {
    constexpr std::string_view prefix = "service/features/";
    if (!key.starts_with(prefix)) {
        return {};
    }
    const auto end = key.find('/', prefix.size());
    return key.substr(0, end);
}

std::optional<std::string_view> directIncludeViolation(std::string_view owner, std::string_view target) {
    const auto directory = [](std::string_view key) {
        return key.substr(0, key.find_last_of('/'));
    };
    if (isModulePath(owner) && owner.ends_with(".controller.h") &&
        isModulePath(target) && directory(owner) != directory(target) &&
        (target.ends_with(".service.h") || target.ends_with(".types.h") || target.ends_with(".entity.h"))) {
        return "module controller directly includes another module's service, types or entity";
    }
    if (isFeaturePath(owner) && isRuntimePath(owner) && isFeaturePath(target) &&
        featureOwner(owner) != featureOwner(target) && isImplementationPath(target)) {
        return "feature runtime directly includes another feature's implementation";
    }
    if ((owner.ends_with(".transport.h") || owner.ends_with(".transport.cpp")) &&
        (target.ends_with(".service.h") || isRuntimePath(target) || target.ends_with(".controller.h"))) {
        return "transport directly includes service, runtime or controller implementation";
    }
    if (isFeaturePath(owner) && target.starts_with("service/middleware/")) {
        return "feature directly includes request middleware";
    }
    return std::nullopt;
}

void testDirectIncludeBoundaryDetection() {
    for (const auto& [owner, target] : {
             std::pair{ "service/modules/alpha/alpha.controller.h", "service/modules/beta/beta.service.h" },
             std::pair{ "service/modules/alpha/alpha.controller.h", "service/modules/beta/beta.types.h" },
             std::pair{ "service/modules/system/alpha/alpha.controller.h", "service/modules/system/beta/beta.entity.h" },
             std::pair{ "service/features/alpha/alpha.runtime.h", "service/features/beta/beta.service.h" },
             std::pair{ "service/features/alpha/alpha.runtime.cpp", "service/features/beta/socket/socket.transport.h" },
             std::pair{ "service/features/alpha/socket/socket.transport.h", "service/features/alpha/alpha.runtime.h" },
             std::pair{ "service/features/alpha/socket/socket.transport.cpp", "service/features/alpha/alpha.service.h" },
             std::pair{ "service/features/alpha/alpha.types.h", "service/middleware/auth.h" } }) {
        require(directIncludeViolation(owner, target).has_value(), "direct include boundary violation was missed");
    }
    for (const auto& [owner, target] : {
             std::pair{ "service/modules/alpha/alpha.controller.h", "service/modules/alpha/alpha.service.h" },
             std::pair{ "service/modules/system/alpha/alpha.controller.h", "service/modules/system/alpha/alpha.types.h" },
             std::pair{ "service/modules/alpha/alpha.service.h", "service/modules/beta/beta.service.h" },
             std::pair{ "service/features/alpha/alpha.runtime.h", "service/features/alpha/socket/socket.transport.h" },
             std::pair{ "service/features/alpha/alpha.runtime.h", "service/features/beta/beta.protocol.h" },
             std::pair{ "service/features/alpha/socket/socket.transport.h", "service/features/alpha/alpha.protocol.h" } }) {
        require(!directIncludeViolation(owner, target), "allowed direct include was rejected");
    }
}

void requireNoLayerReachability(const IncludeGraph& graph, bool fromModules) {
    for (const auto& path : graph.files) {
        const auto start = pathKey(path);
        if (fromModules ? !isModulePath(start) : !isFeaturePath(start)) {
            continue;
        }
        std::unordered_set<std::string> visited{ start };
        std::vector<std::string> pending{ start };
        while (!pending.empty()) {
            const auto current = std::move(pending.back());
            pending.pop_back();
            const auto edge = graph.includes.find(current);
            if (edge == graph.includes.end()) {
                continue;
            }
            for (const auto& target : edge->second) {
                if (fromModules ? isFeaturePath(target) : isModulePath(target)) {
                    throw std::runtime_error(
                        std::string(fromModules ? "modules include graph reaches features: " : "features include graph reaches modules: ") +
                        start + " -> " + target
                    );
                }
                if (visited.insert(target).second) {
                    pending.push_back(target);
                }
            }
        }
    }
}

void testIncludeGraphBoundaries() {
    const auto graph = buildIncludeGraph();
    requireNoLayerReachability(graph, true);
    requireNoLayerReachability(graph, false);
    for (const auto& [owner, imports] : graph.includes) {
        const bool infrastructure = owner.starts_with("service/common/") || owner.starts_with("service/utils/");
        for (const auto& target : imports) {
            if (const auto violation = directIncludeViolation(owner, target)) {
                throw std::runtime_error(std::string(*violation) + ": " + owner + " -> " + target);
            }
            if (infrastructure) {
                requireFeatureRule(!isModulePath(target) && !isFeaturePath(target), graph.paths.at(owner), "shared infrastructure depends on business implementation");
            }
        }
    }

    for (const auto& path : graph.files) {
        const auto start = pathKey(path);
        const bool pureDefinition = start.ends_with(".entity.h") ||
            start.ends_with(".types.h") ||
            start.ends_with(".config.h") || start.ends_with(".protocol.h");
        if ((!isFeaturePath(start) && !isModulePath(start)) ||
            (!start.ends_with(".service.h") && !pureDefinition)) {
            continue;
        }
        std::unordered_set<std::string> visited{ start };
        std::vector<std::string> pending{ start };
        while (!pending.empty()) {
            const auto current = std::move(pending.back());
            pending.pop_back();
            const auto edge = graph.includes.find(current);
            if (edge == graph.includes.end()) {
                continue;
            }
            for (const auto& target : edge->second) {
                if (pureDefinition && (target.ends_with(".service.h") || target.ends_with(".runtime.h") || target.ends_with(".controller.h") || target.ends_with(".transport.h"))) {
                    throw std::runtime_error("pure definition transitively includes I/O implementation: " + start + " -> " + target);
                }
                if (target != start && isRuntimePath(target)) {
                    throw std::runtime_error("feature service transitively includes runtime: " + start + " -> " + target);
                }
                if (visited.insert(target).second) {
                    pending.push_back(target);
                }
            }
        }
    }

    for (const auto& path : graph.files) {
        const auto start = pathKey(path);
        std::unordered_map<std::string, unsigned char> state;
        std::function<void(const std::string&)> visit = [&](const std::string& current) {
            state[current] = 1;
            const auto edge = graph.includes.find(current);
            if (edge != graph.includes.end()) {
                for (const auto& target : edge->second) {
                    const auto targetState = state.contains(target) ? state.at(target) : 0;
                    if (targetState == 1) {
                        throw std::runtime_error("service include cycle: " + current + " -> " + target);
                    }
                    if (targetState == 0) {
                        visit(target);
                    }
                }
            }
            state[current] = 2;
        };
        visit(start);
    }
}

std::string withoutEntityScriptDefinitions(const std::string& content) {
    // 实体可声明纯 Redis 映射文本；只排除完整 constexpr 原始字面量声明，
    // 客户端调用和其他包含命令的表达式仍保留在 I/O 边界检查中。
    static const std::regex scriptDefinition(
        R"definition(\b(?:static|inline)\s+constexpr\s+std::string_view\s+[A-Za-z_]\w*\s*=\s*R"lua\([\s\S]*?\)lua"\s*;)definition"
    );
    return std::regex_replace(content, scriptDefinition, " ");
}

void testStorageOwnership() {
    const std::regex databaseAccess(R"((?:\.|->)\s*db\s*\(|\bdb_query\s+[A-Za-z_]\w*\s*[({;]|\brepository\s*<)");
    const std::regex hashAccess(R"((?:\.|->)\s*(?:hget|hget_all|hset|hdel|hkeys|hexists)\s*\(|::\s*(?:setHash|getHash|eraseHash)\s*\()");
    const std::regex rawHashAccess(
        R"re(["'](?:HGET|HGETALL|HMGET|HSET|HSETNX|HMSET|HDEL|HKEYS|HVALS|HEXISTS|HLEN|HSCAN|HINCRBY|HINCRBYFLOAT)["'])re",
        std::regex::icase
    );
    require(std::regex_search(std::string{ "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])" }, rawHashAccess), "storage ownership check misses Lua Hash writes");
    require(std::regex_search(std::string{ "command(redis, {\"hgetall\", key})" }, rawHashAccess), "storage ownership check misses raw Hash reads");
    require(!std::regex_search(std::string{ "redis.call('GET', KEYS[1]); redis.call('XACK', KEYS[2], ARGV[1])" }, rawHashAccess), "storage ownership check mistakes lock and queue commands for Hash records");
    const std::regex redisExecution(R"((?:\.|->)\s*(?:command|eval|pipeline)\s*\()");
    const std::string scriptDefinition =
        "static constexpr std::string_view bindScript = R\"lua(\n"
        "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])\n)lua\";";
    require(!std::regex_search(withoutEntityScriptDefinitions(scriptDefinition), rawHashAccess), "pure constexpr entity mapping was mistaken for Redis I/O");
    for (const auto* execution : { "redis.command({\"HSET\", key, field, value});",
                                   "return redis . eval(bindScript, keys, args);",
                                   "auto request = redis->pipeline();" }) {
        require(std::regex_search(withoutEntityScriptDefinitions(scriptDefinition + execution), redisExecution), "entity mapping allowance hides a Redis client execution");
    }
    for (const auto* expression : { "const auto script = R\"lua(redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]))lua\";",
                                    "return R\"lua(redis.call('HGET', KEYS[1], ARGV[1]))lua\";",
                                    "command(redis, {\"HSET\", key, field, value});" }) {
        require(std::regex_search(withoutEntityScriptDefinitions(expression), rawHashAccess), "entity mapping allowance hides a non-declarative Hash operation");
    }
    const std::regex implicitIdentityGeneration(R"(\b(?:nextUuid|nextUuidV7|nextMessageId)\s*\(|\bruntime\s*::\s*instanceId\s*\()");
    require(std::regex_search(std::string{ "message::nextMessageId ()" }, implicitIdentityGeneration), "identity boundary check misses a message UUID call");
    require(std::regex_search(std::string{ "service::common::nextUuidV7()" }, implicitIdentityGeneration), "identity boundary check misses a direct UUID call");
    require(!std::regex_search(std::string{ "uuidBytes(messageId, bytes)" }, implicitIdentityGeneration), "identity boundary check rejects pure UUID conversion");
    require(std::regex_search(std::string{ "service::runtime::instanceId()" }, implicitIdentityGeneration), "identity boundary check misses implicit process state");
    require(!std::regex_search(source("service/common/message.h"), implicitIdentityGeneration), "message contract implicitly reads or generates a runtime identity");
    const std::regex entityDeclaration(R"(\bRUVIA_(?:DB|REDIS)_ENTITY\s*\(\s*([A-Za-z_][A-Za-z_0-9]*))");
    for (const auto& path : serviceSourceFiles()) {
        const auto key = pathKey(path);
        if (!isModulePath(key) && !isFeaturePath(key)) {
            continue;
        }
        const auto content = source(key);
        const bool isEntity = key.ends_with(".entity.h");
        const bool isPureDefinition = isEntity || key.ends_with(".types.h") ||
            key.ends_with(".config.h") ||
            key.ends_with(".protocol.h") || key.ends_with(".protocol.cpp");
        if (isPureDefinition) {
            requireFeatureRule(!std::regex_search(content, implicitIdentityGeneration), path, "pure definition implicitly generates a runtime identity");
            requireFeatureRule(content.find("co_await") == std::string::npos, path, "pure definition or protocol performs asynchronous I/O");
        }
        if (isEntity) {
            requireFeatureRule(!std::regex_search(content, redisExecution), path, "storage entity executes Redis I/O instead of declaring its mapping");
        }
        requireFeatureRule(isEntity || !std::regex_search(content, entityDeclaration), path, "storage entity declared outside its entity file");
        const auto hashOperationContent = isEntity ? withoutEntityScriptDefinitions(content) : content;
        if (std::regex_search(content, databaseAccess) || std::regex_search(content, hashAccess) ||
            std::regex_search(hashOperationContent, rawHashAccess)) {
            requireFeatureRule(key.ends_with(".service.h"), path, "database or persistent Redis Hash operation outside business service");
            const auto entityPath = path.parent_path() /
                (path.parent_path().filename().string() + ".entity.h");
            requireFeatureRule(std::filesystem::is_regular_file(entityPath), path, "database service has no owning entity file");
        }
    }
}

void testFeatureDomainBoundaries() {
    const auto files = featureFiles();
    static constexpr std::array<std::string_view, 6> forbiddenReferences{
        "service/domains/",
        "service/modules/",
        "../domains/",
        "../modules/",
        "../../domains/",
        "../../modules/",
    };
    for (const auto& path : files) {
        std::ifstream input(path, std::ios::binary);
        requireFeatureRule(input.good(), path, "cannot read feature source");
        const std::string content{ std::istreambuf_iterator<char>(input),
                                   std::istreambuf_iterator<char>() };
        for (const auto reference : forbiddenReferences) {
            requireFeatureRule(content.find(reference) == std::string::npos, path, "feature directly references a domain/module path");
        }
        if (path.filename().string().ends_with(".service.h")) {
            requireFeatureRule(content.find(".runtime.h") == std::string::npos, path, "feature service directly includes a runtime header");
        }
    }
}

void testLifecycleOrder() {
    service::observability::RuntimeDiagnostics metrics;
    service::observability::ComponentLifecycle runtime(metrics);
    std::vector<std::string> calls;
    runtime.add({ .name = "database", .start = [&] {
                     calls.emplace_back("start-database");
                 },
                  .stop = [&] {
                      calls.emplace_back("stop-database");
                  } });
    runtime.add({ .name = "consumer", .dependencies = { "database" }, .start = [&] {
                     calls.emplace_back("start-consumer");
                 },
                  .stop = [&] {
                      calls.emplace_back("stop-consumer");
                  } });
    runtime.start();
    require(metrics.areComponentsReady(), "runtime did not become ready");
    runtime.stop();
    const std::vector<std::string> expected{ "start-database", "start-consumer", "stop-consumer", "stop-database" };
    require(calls == expected, "runtime lifecycle order is incorrect");
}

void testLifecycleRollback() {
    service::observability::RuntimeDiagnostics metrics;
    service::observability::ComponentLifecycle runtime(metrics);
    bool stopped = false;
    runtime.add({ .name = "first", .start = [] {
                 },
                  .stop = [&] {
                      stopped = true;
                  } });
    runtime.add({ .name = "broken", .dependencies = { "first" }, .start = [] {
                     throw std::runtime_error("expected");
                 },
                  .stop = [] {
                  } });
    try {
        runtime.start();
        require(false, "runtime accepted a failed component");
    } catch (const std::runtime_error&) {
    }
    require(stopped, "runtime did not roll back started components");
    require(!metrics.areComponentsReady(), "failed runtime reported ready");
}

void testMessageEnvelope() {
    service::message::EgressPacket packet;
    packet.messageId = "event-1";
    packet.connectionId = "connection-1";
    packet.payload = { 0x01 };
    const auto fields = service::message::egressFields(packet);
    const auto field = [&](std::string_view name) {
        for (const auto& item : fields) {
            if (item.name == name) {
                return item.value;
            }
        }
        return std::string{};
    };
    require(field("event_id") == packet.messageId, "message envelope has no event id");
    require(field("schema_version") == "2", "message envelope has no schema version");
    require(field("event_type") == "packet.egress", "message envelope has no event type");
}

void testOperationalAlerts() {
    service::observability::RuntimeDiagnostics metrics;
    metrics.setComponentStatus("outbox", service::observability::ComponentState::Ready);
    require(metrics.setAlertState("outbox_dead_lettered", true, "value=1, threshold=1"), "activating an alert did not report a transition");
    require(metrics.healthJson().find("\"status\":\"degraded\"") != std::string::npos, "active operational alert does not degrade health status");
    require(metrics.prometheus().find("iot_engine_alert_active{alert=\"outbox_dead_lettered\"} 1") != std::string::npos, "active operational alert is missing from metrics");
    require(metrics.setAlertState("outbox_dead_lettered", false, "value=0, threshold=1"), "clearing an alert did not report a transition");
    require(metrics.healthJson().find("\"status\":\"ready\"") != std::string::npos, "cleared operational alert did not restore health status");
}

void testWorkerTaskSignals() {
    std::promise<void> promise;
    auto ready = promise.get_future();
    service::utils::WorkerReady signal(promise);
    require(!signal.signalled(), "worker task starts ready before initialization");
    require(ready.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout, "worker readiness completed before initialization");
    signal.set_value();
    ready.get();
    require(signal.signalled(), "worker readiness signal was not recorded");

    ruvia::stop_source stop;
    std::promise<void> completed;
    auto completion = completed.get_future();
    completed.set_value();
    service::utils::stopWorkerTask(stop, completion);
    require(stop.token().stop_requested(), "worker task shutdown did not cancel its owner token");
    require(!completion.valid(), "worker task shutdown retained an already joined completion");
}

} // namespace

int main() {
    try {
        testServiceTopLevelLayout();
        testModuleExecutorDetection();
        testModuleFileLayout();
        testFeatureFileLayout();
        testCommonFileLayout();
        testSourceStructureDetection();
        testDirectIncludeBoundaryDetection();
        testIncludeGraphBoundaries();
        testFeatureDomainBoundaries();
        testStorageOwnership();
        testLifecycleOrder();
        testLifecycleRollback();
        testMessageEnvelope();
        testOperationalAlerts();
        testWorkerTaskSignals();
        std::cout << "architecture tests passed\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "architecture test failed: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
