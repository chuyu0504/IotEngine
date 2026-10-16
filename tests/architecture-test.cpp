#include <algorithm>
#include <array>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
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

#include "service/config/lifecycle.h"
#include "service/common/message.h"

namespace {

void require(bool condition, std::string_view message) {
    if (!condition)
        throw std::runtime_error(std::string(message));
}

std::filesystem::path repositoryRoot() {
    return std::filesystem::path(__FILE__).parent_path().parent_path();
}

std::string source(std::string_view relative) {
    const auto path = std::filesystem::path(__FILE__).parent_path().parent_path() /
                      std::filesystem::path(relative);
    std::ifstream input(path, std::ios::binary);
    require(input.good(), "cannot read architecture source");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void requireFeatureRule(bool condition, const std::filesystem::path& path,
                        std::string_view message) {
    if (!condition)
        throw std::runtime_error(std::string(message) + ": " + path.generic_string());
}

bool isSnakeCase(std::string_view value) {
    if (value.empty() || value.front() < 'a' || value.front() > 'z' ||
        value.back() == '_')
        return false;
    bool previousUnderscore = false;
    for (const auto character : value) {
        if (character == '_') {
            if (previousUnderscore)
                return false;
            previousUnderscore = true;
            continue;
        }
        if ((character < 'a' || character > 'z') &&
            (character < '0' || character > '9'))
            return false;
        previousUnderscore = false;
    }
    return true;
}

template <std::size_t Size>
bool contains(const std::array<std::string_view, Size>& values, std::string_view value) {
    for (const auto candidate : values)
        if (candidate == value)
            return true;
    return false;
}

bool isArchitectureSource(const std::filesystem::path& path) {
    static constexpr std::array<std::string_view, 7> extensions{
        ".h", ".hpp", ".hh", ".hxx", ".cpp", ".cc", ".cxx",
    };
    return contains(extensions, path.extension().string());
}

std::string pathKey(const std::filesystem::path& path) {
    return path.lexically_relative(repositoryRoot()).generic_string();
}

void requireExpectedModule(std::string_view domain, std::string_view module,
                           const std::filesystem::path& path) {
    requireFeatureRule(isSnakeCase(domain), path, "invalid module directory name");
    requireFeatureRule(domain == "system" ? isSnakeCase(module) : module.empty(),
                       path, "only system modules may have a grouping directory");
}

bool whitelistedModuleFile(std::string_view file, std::string_view owner) {
    static constexpr std::array<std::string_view, 5> suffixes{
        ".types.h", ".service.h", ".entity.h", ".error.h",
        ".controller.h",
    };
    for (const auto suffix : suffixes) {
        if (file == std::string(owner) + std::string(suffix))
            return true;
    }
    return false;
}

void testServiceTopLevelLayout() {
    const auto root = repositoryRoot() / "service";
    static constexpr std::array<std::string_view, 7> allowed{
        "common", "config", "features", "middleware", "modules", "utils", "server.cpp",
    };
    require(std::filesystem::is_directory(root), "service directory is missing");
    for (const auto& entry : std::filesystem::directory_iterator(root))
        requireFeatureRule(contains(allowed, entry.path().filename().string()), entry.path(),
                           "service top-level entry is not whitelisted");
    for (const auto requiredDirectory : {"common", "config", "features", "middleware",
                                         "modules", "utils"})
        requireFeatureRule(std::filesystem::is_directory(root / requiredDirectory),
                           root / requiredDirectory,
                           "required service top-level directory is missing");
    requireFeatureRule(std::filesystem::is_regular_file(root / "server.cpp"),
                       root / "server.cpp", "service/server.cpp is missing");
}

bool whitelistedFeatureFile(std::string_view file, std::string_view owner) {
    static constexpr std::array<std::string_view, 12> suffixes{
        ".runtime.h",   ".runtime.cpp",  ".service.h",   ".types.h",
        ".entity.h",    ".config.h",     ".protocol.h",  ".protocol.cpp",
        ".transport.h", ".transport.cpp", ".error.h",     ".proto",
    };
    for (const auto suffix : suffixes) {
        if (file == std::string(owner) + std::string(suffix))
            return true;
    }
    return false;
}

std::vector<std::filesystem::path> featureFiles() {
    const auto root = repositoryRoot() / "service" / "features";
    require(std::filesystem::is_directory(root), "service/features directory is missing");
    std::vector<std::filesystem::path> result;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (entry.is_regular_file())
            result.push_back(entry.path());
    }
    return result;
}

bool declaresPrivateExecutor(const std::string& code) {
    static const std::regex declaration(
        R"(^\s*(?:(?:static|inline)\s+)*(?:std::(?:jthread|thread)|asio::io_context|ruvia::event_loop_pool)\s+[A-Za-z_]\w*\s*[;{(=])",
        std::regex_constants::ECMAScript);
    for (std::size_t start = 0; start < code.size();) {
        const auto end = code.find('\n', start);
        if (std::regex_search(code.substr(start, end - start), declaration)) return true;
        if (end == std::string::npos) break;
        start = end + 1;
    }
    return false;
}

void testModuleExecutorDetection() {
    for (const auto* declaration : {
            "    std::thread outbound_;", "std::jthread worker([] {});",
            "asio::io_context io;", "ruvia::event_loop_pool pool({.loop_count_ = 1});"})
        require(declaresPrivateExecutor(declaration), "module executor declaration was missed");
    for (const auto* allowed : {
            "// std::thread example;", "std::thread::id owner;",
            "auto client = context.get_http_client();"})
        require(!declaresPrivateExecutor(allowed), "non-owning worker usage was rejected");
}

void testModuleFileLayout() {
    const auto root = repositoryRoot() / "service" / "modules";
    require(std::filesystem::is_directory(root), "service/modules directory is missing");
    const auto validateModuleFiles = [](const std::filesystem::path& modulePath,
                                        std::string_view moduleName) {
        for (const auto& entry : std::filesystem::directory_iterator(modulePath)) {
            requireFeatureRule(entry.is_regular_file(), entry.path(),
                               "module files may not contain nested directories");
            const auto file = entry.path().filename().string();
            requireFeatureRule(file.starts_with(std::string(moduleName) + "."), entry.path(),
                               "module file name must use its module prefix");
            requireFeatureRule(whitelistedModuleFile(file, moduleName), entry.path(),
                               "module file name or extension is not whitelisted");
            const auto code = source(entry.path().generic_string());
            requireFeatureRule(!declaresPrivateExecutor(code), entry.path(),
                               "module must use its accepting Worker instead of owning a thread or event loop");
        }
    };

    for (const auto& entry : std::filesystem::directory_iterator(root)) {
        requireFeatureRule(entry.is_directory(), entry.path(),
                           "module entries must be directories");
        const auto entryName = entry.path().filename().string();
        requireFeatureRule(isSnakeCase(entryName), entry.path(),
                           "module directory must use snake_case");
        if (entryName == "system") {
            for (const auto& module : std::filesystem::directory_iterator(entry.path())) {
                requireFeatureRule(module.is_directory(), module.path(),
                                   "modules must be directories under system");
                const auto moduleName = module.path().filename().string();
                requireFeatureRule(isSnakeCase(moduleName), module.path(),
                                   "module directory must use snake_case");
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
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root))
        if (entry.is_directory())
            requireFeatureRule(isSnakeCase(entry.path().filename().string()), entry.path(),
                               "feature directory must use snake_case");
    for (const auto& path : files) {
        const auto relative = path.lexically_relative(root);
        std::vector<std::string> parts;
        for (const auto& part : relative)
            parts.emplace_back(part.string());
        requireFeatureRule(parts.size() <= 3, path,
                           "feature components may not be nested beyond one directory");
        requireFeatureRule(parts.size() == 2 || parts.size() == 3, path,
                           "feature files must be directly under a feature or component");

        const auto& feature = parts.front();
        const auto& owner = parts.size() == 2 ? feature : parts[1];
        const auto& file = parts.back();
        const auto prefix = owner + ".";
        requireFeatureRule(file.starts_with(prefix), path,
                           "feature file name must use its feature/component prefix");
        requireFeatureRule(whitelistedFeatureFile(file, owner), path,
                           "feature file name or extension is not whitelisted");
    }
}

void testCommonFileLayout() {
    const auto root = repositoryRoot() / "service" / "common";
    require(std::filesystem::is_directory(root), "service/common directory is missing");
    for (const auto& entry : std::filesystem::directory_iterator(root))
        requireFeatureRule(entry.is_regular_file(), entry.path(),
                           "service/common may contain foundation files only, no directories");
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
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root))
        if (entry.is_regular_file() && isArchitectureSource(entry.path()))
            result.push_back(entry.path());
    return result;
}

std::optional<std::string> includeTarget(std::string_view line) {
    auto cursor = line.find_first_not_of(" \t\r");
    if (cursor == std::string_view::npos || line[cursor] != '#')
        return std::nullopt;
    ++cursor;
    while (cursor < line.size() &&
           std::isspace(static_cast<unsigned char>(line[cursor])))
        ++cursor;
    constexpr std::string_view directive = "include";
    if (line.substr(cursor, directive.size()) != directive)
        return std::nullopt;
    cursor += directive.size();
    if (cursor < line.size() && !std::isspace(static_cast<unsigned char>(line[cursor])) &&
        line[cursor] != '"' && line[cursor] != '<')
        return std::nullopt;
    while (cursor < line.size() &&
           std::isspace(static_cast<unsigned char>(line[cursor])))
        ++cursor;
    if (cursor == line.size() || (line[cursor] != '"' && line[cursor] != '<'))
        return std::nullopt;
    const auto closing = line[cursor] == '"' ? '"' : '>';
    ++cursor;
    const auto end = line.find(closing, cursor);
    if (end == std::string_view::npos)
        return std::nullopt;
    return std::string(line.substr(cursor, end - cursor));
}

std::optional<std::string> resolveInclude(
    const std::filesystem::path& including,
    std::string_view spelling,
    const std::unordered_map<std::string, std::filesystem::path>& known) {
    std::string normalized(spelling);
    for (auto& character : normalized)
        if (character == '\\')
            character = '/';

    std::vector<std::filesystem::path> candidates;
    const auto target = std::filesystem::path(normalized);
    if (normalized.starts_with("service/"))
        candidates.push_back(repositoryRoot() / target);
    else {
        candidates.push_back(including.parent_path() / target);
        candidates.push_back(repositoryRoot() / "service" / target);
        candidates.push_back(repositoryRoot() / target);
    }

    for (const auto& candidate : candidates) {
        const auto lookup = [&](const std::filesystem::path& path)
            -> std::optional<std::string> {
            const auto key = pathKey(path.lexically_normal());
            if (known.contains(key))
                return key;
            return std::nullopt;
        };
        if (const auto result = lookup(candidate))
            return result;
        if (!candidate.has_extension()) {
            for (const auto suffix : {".h", ".hpp", ".hh", ".hxx", ".cpp", ".cc",
                                      ".cxx"}) {
                auto withSuffix = candidate;
                withSuffix += suffix;
                if (const auto result = lookup(withSuffix))
                    return result;
            }
        }
    }
    return std::nullopt;
}

bool isLayerReference(std::string_view spelling) {
    std::string normalized(spelling);
    for (auto& character : normalized)
        if (character == '\\')
            character = '/';
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
        std::string line;
        while (std::getline(input, line)) {
            const auto target = includeTarget(line);
            if (!target)
                continue;
            const auto resolved = resolveInclude(path, *target, graph.paths);
            if (!resolved) {
                requireFeatureRule(!isLayerReference(*target), path,
                                   "service layer include target does not exist");
                continue;
            }
            auto& edges = graph.includes.at(from);
            if (std::find(edges.begin(), edges.end(), *resolved) == edges.end())
                edges.push_back(*resolved);
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

void requireNoLayerReachability(const IncludeGraph& graph, bool fromModules) {
    for (const auto& path : graph.files) {
        const auto start = pathKey(path);
        if (fromModules ? !isModulePath(start) : !isFeaturePath(start))
            continue;
        std::unordered_set<std::string> visited{start};
        std::vector<std::string> pending{start};
        while (!pending.empty()) {
            const auto current = std::move(pending.back());
            pending.pop_back();
            const auto edge = graph.includes.find(current);
            if (edge == graph.includes.end())
                continue;
            for (const auto& target : edge->second) {
                if (fromModules ? isFeaturePath(target) : isModulePath(target)) {
                    throw std::runtime_error(
                        std::string(fromModules ? "modules include graph reaches features: "
                                                : "features include graph reaches modules: ") +
                        start + " -> " + target);
                }
                if (visited.insert(target).second)
                    pending.push_back(target);
            }
        }
    }
}

void testIncludeGraphBoundaries() {
    const auto graph = buildIncludeGraph();
    requireNoLayerReachability(graph, true);
    requireNoLayerReachability(graph, false);
    for (const auto& [owner, imports] : graph.includes) {
        if (!owner.starts_with("service/common/") && !owner.starts_with("service/utils/"))
            continue;
        for (const auto& target : imports)
            requireFeatureRule(!isModulePath(target) && !isFeaturePath(target), graph.paths.at(owner),
                               "shared infrastructure depends on business implementation");
    }

    for (const auto& path : graph.files) {
        const auto start = pathKey(path);
        const bool pureDefinition = start.ends_with(".entity.h") ||
            start.ends_with(".types.h") ||
            start.ends_with(".config.h") || start.ends_with(".protocol.h");
        if ((!isFeaturePath(start) && !isModulePath(start)) ||
            (!start.ends_with(".service.h") && !pureDefinition))
            continue;
        std::unordered_set<std::string> visited{start};
        std::vector<std::string> pending{start};
        while (!pending.empty()) {
            const auto current = std::move(pending.back());
            pending.pop_back();
            const auto edge = graph.includes.find(current);
            if (edge == graph.includes.end())
                continue;
            for (const auto& target : edge->second) {
                if (pureDefinition && (target.ends_with(".service.h") ||
                    target.ends_with(".runtime.h") || target.ends_with(".controller.h") ||
                    target.ends_with(".transport.h")))
                    throw std::runtime_error("pure definition transitively includes I/O implementation: " +
                                             start + " -> " + target);
                if (target != start && isRuntimePath(target))
                    throw std::runtime_error("feature service transitively includes runtime: " +
                                             start + " -> " + target);
                if (visited.insert(target).second)
                    pending.push_back(target);
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
                    if (targetState == 1)
                        throw std::runtime_error("service include cycle: " + current +
                                                 " -> " + target);
                    if (targetState == 0)
                        visit(target);
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
    const std::regex databaseAccess(R"((?:\.|->)\s*db\s*\(|\bdb_query\b|\bget_repository\s*<)");
    const std::regex hashAccess(R"((?:\.|->)\s*(?:hget|hget_all|hset|hdel|hkeys|hexists)\s*\(|::\s*(?:setHash|getHash|eraseHash)\s*\()");
    const std::regex rawHashAccess(
        R"re(["'](?:HGET|HGETALL|HMGET|HSET|HSETNX|HMSET|HDEL|HKEYS|HVALS|HEXISTS|HLEN|HSCAN|HINCRBY|HINCRBYFLOAT)["'])re",
        std::regex::icase);
    require(std::regex_search(std::string{"redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])"}, rawHashAccess),
            "storage ownership check misses Lua Hash writes");
    require(std::regex_search(std::string{"command(redis, {\"hgetall\", key})"}, rawHashAccess),
            "storage ownership check misses raw Hash reads");
    require(!std::regex_search(std::string{"redis.call('GET', KEYS[1]); redis.call('XACK', KEYS[2], ARGV[1])"}, rawHashAccess),
            "storage ownership check mistakes lock and queue commands for Hash records");
    const std::regex redisExecution(R"((?:\.|->)\s*(?:command|eval|pipeline)\s*\()");
    const std::string scriptDefinition =
        "static constexpr std::string_view bindScript = R\"lua(\n"
        "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])\n)lua\";";
    require(!std::regex_search(withoutEntityScriptDefinitions(scriptDefinition), rawHashAccess),
            "pure constexpr entity mapping was mistaken for Redis I/O");
    for (const auto* execution : {"redis.command({\"HSET\", key, field, value});",
                                  "return redis . eval(bindScript, keys, args);",
                                  "auto request = redis->pipeline();"}) {
        require(std::regex_search(withoutEntityScriptDefinitions(scriptDefinition + execution), redisExecution),
                "entity mapping allowance hides a Redis client execution");
    }
    for (const auto* expression : {"const auto script = R\"lua(redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]))lua\";",
                                   "return R\"lua(redis.call('HGET', KEYS[1], ARGV[1]))lua\";",
                                   "command(redis, {\"HSET\", key, field, value});"}) {
        require(std::regex_search(withoutEntityScriptDefinitions(expression), rawHashAccess),
                "entity mapping allowance hides a non-declarative Hash operation");
    }
    const std::regex implicitIdentityGeneration(R"(\b(?:nextUuidV7|nextMessageId)\s*\(|\bUuidV7Generator\s*::|\bruntime\s*::\s*instanceId\s*\()");
    require(std::regex_search(std::string{"message::nextMessageId ()"}, implicitIdentityGeneration),
            "identity boundary check misses a message UUID call");
    require(std::regex_search(std::string{"service::common::nextUuidV7()"}, implicitIdentityGeneration),
            "identity boundary check misses a direct UUID call");
    require(!std::regex_search(std::string{"uuidBytes(messageId, bytes)"}, implicitIdentityGeneration),
            "identity boundary check rejects pure UUID conversion");
    require(std::regex_search(std::string{"service::runtime::instanceId()"}, implicitIdentityGeneration),
            "identity boundary check misses implicit process state");
    require(!std::regex_search(source("service/common/message.h"), implicitIdentityGeneration),
            "message contract implicitly reads or generates a runtime identity");
    const std::regex entityDeclaration(R"(\bRUVIA_(?:DB|REDIS)_ENTITY\s*\(\s*([A-Za-z_][A-Za-z_0-9]*))");
    for (const auto& path : serviceSourceFiles()) {
        const auto key = pathKey(path);
        if (!isModulePath(key) && !isFeaturePath(key))
            continue;
        const auto content = source(key);
        const bool isEntity = key.ends_with(".entity.h");
        const bool isPureDefinition = isEntity || key.ends_with(".types.h") ||
            key.ends_with(".config.h") ||
            key.ends_with(".protocol.h") || key.ends_with(".protocol.cpp");
        if (isPureDefinition) {
            requireFeatureRule(!std::regex_search(content, implicitIdentityGeneration), path,
                               "pure definition implicitly generates a runtime identity");
            requireFeatureRule(content.find("co_await") == std::string::npos, path,
                               "pure definition or protocol performs asynchronous I/O");
        }
        if (isEntity)
            requireFeatureRule(!std::regex_search(content, redisExecution), path,
                               "storage entity executes Redis I/O instead of declaring its mapping");
        requireFeatureRule(isEntity || !std::regex_search(content, entityDeclaration), path,
                           "storage entity declared outside its entity file");
        const auto hashOperationContent = isEntity ? withoutEntityScriptDefinitions(content) : content;
        if (std::regex_search(content, databaseAccess) || std::regex_search(content, hashAccess) ||
            std::regex_search(hashOperationContent, rawHashAccess)) {
            requireFeatureRule(key.ends_with(".service.h"), path,
                               "database or persistent Redis Hash operation outside business service");
            const auto entityPath = path.parent_path() /
                (path.parent_path().filename().string() + ".entity.h");
            requireFeatureRule(std::filesystem::is_regular_file(entityPath), path,
                               "database service has no owning entity file");
        }
        if (!isEntity)
            continue;
        std::string consumers;
        for (const auto& sibling : std::filesystem::directory_iterator(path.parent_path()))
            if (sibling.is_regular_file() && sibling.path() != path &&
                isArchitectureSource(sibling.path()))
                consumers += source(pathKey(sibling.path()));
        for (auto match = std::sregex_iterator(content.begin(), content.end(), entityDeclaration);
             match != std::sregex_iterator(); ++match) {
            const auto name = (*match)[1].str();
            requireFeatureRule(consumers.find(name + "::") != std::string::npos ||
                                   consumers.find("<" + name + ">") != std::string::npos ||
                                   consumers.find("::" + name + ">") != std::string::npos,
                               path, "entity is not used by its owning component: " + name);
        }
    }
}

void testFeatureDomainBoundaries() {
    const auto files = featureFiles();
    static constexpr std::array<std::string_view, 6> forbiddenReferences{
        "service/domains/", "service/modules/", "../domains/",
        "../modules/",      "../../domains/",  "../../modules/",
    };
    for (const auto& path : files) {
        std::ifstream input(path, std::ios::binary);
        requireFeatureRule(input.good(), path, "cannot read feature source");
        const std::string content{std::istreambuf_iterator<char>(input),
                                  std::istreambuf_iterator<char>()};
        for (const auto reference : forbiddenReferences)
            requireFeatureRule(content.find(reference) == std::string::npos, path,
                               "feature directly references a domain/module path");
        if (path.filename().string().ends_with(".service.h"))
            requireFeatureRule(content.find(".runtime.h") == std::string::npos, path,
                               "feature service directly includes a runtime header");
    }
}

void testLifecycleOrder() {
    service::observability::RuntimeDiagnostics metrics;
    service::application::ComponentLifecycle runtime(metrics);
    std::vector<std::string> calls;
    runtime.add({.name = "database",
                 .start = [&] { calls.emplace_back("start-database"); },
                 .stop = [&] { calls.emplace_back("stop-database"); }});
    runtime.add({.name = "consumer",
                 .dependencies = {"database"},
                 .start = [&] { calls.emplace_back("start-consumer"); },
                 .stop = [&] { calls.emplace_back("stop-consumer"); }});
    runtime.start();
    require(metrics.areComponentsReady(), "runtime did not become ready");
    runtime.stop();
    const std::vector<std::string> expected{"start-database", "start-consumer",
                                             "stop-consumer", "stop-database"};
    require(calls == expected, "runtime lifecycle order is incorrect");
}

void testLifecycleRollback() {
    service::observability::RuntimeDiagnostics metrics;
    service::application::ComponentLifecycle runtime(metrics);
    bool stopped = false;
    runtime.add({.name = "first", .start = [] {}, .stop = [&] { stopped = true; }});
    runtime.add({.name = "broken",
                 .dependencies = {"first"},
                 .start = [] { throw std::runtime_error("expected"); },
                 .stop = [] {}});
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
        for (const auto& item : fields)
            if (item.name == name)
                return item.value;
        return std::string{};
    };
    require(field("event_id") == packet.messageId, "message envelope has no event id");
    require(field("schema_version") == "2", "message envelope has no schema version");
    require(field("event_type") == "packet.egress", "message envelope has no event type");
}

void testExplicitOutbox() {
    const auto schema = source("service/config/schema.h");
    require(schema.find("0023_transactional_outbox") != std::string::npos,
            "transactional outbox migration is missing");
    require(schema.find("0036_live_query_changes") != std::string::npos &&
                schema.find("publish_query_change") != std::string::npos,
            "query changes must be captured transactionally for external SQL writers");
    require(schema.find("0024_outbox_consumer_receipt") != std::string::npos,
            "outbox consumer receipt migration is missing");
    for (const auto path : {"service/modules/device/device.service.h",
                            "service/modules/link/link.service.h",
                            "service/modules/protocol/protocol.service.h",
                            "service/modules/open_access/open_access.service.h"}) {
        const auto content = source(path);
        require(content.find("enqueueConfigEvent(transaction") != std::string::npos,
                "domain write does not enqueue an outbox event in its transaction");
        require(content.find("publishConfigEvent") == std::string::npos,
                "domain still directly publishes config events");
    }
}

void testOutboxOperations() {
    // 运行配置变更的回执检查与写入归 ConfigurationService::applyRuntimeChanges，运行时在其返回后才确认。
    const auto reconciler = source("service/features/configuration/configuration.runtime.h");
    const auto configuration = source("service/features/configuration/configuration.service.h");
    require(configuration.find("idempotency::pending") != std::string::npos, "outbox consumer does not check durable receipts");
    require(configuration.find("idempotency::markProcessed") != std::string::npos, "outbox consumer does not persist durable receipts");
    require(reconciler.find("applyRuntimeChanges") != std::string::npos && reconciler.find("applyRuntimeChanges") < reconciler.find("acknowledgeAndDeleteMany"), "outbox consumer acknowledges before persisting its receipt");
    const auto dispatcher = source("service/features/messaging/messaging.service.h");
    require(dispatcher.find(".delete_from(service::messaging::persistence::OutboxConsumerReceiptEntity::table_name())") != std::string::npos,
            "outbox consumer receipts have no retention cleanup");
}

void testOperationalAlerts() {
    service::observability::RuntimeDiagnostics metrics;
    metrics.setComponentStatus("outbox", service::observability::ComponentState::Ready);
    require(metrics.setAlertState("outbox_dead_lettered", true, "value=1, threshold=1"),
            "activating an alert did not report a transition");
    require(metrics.healthJson().find("\"status\":\"degraded\"") != std::string::npos,
            "active operational alert does not degrade health status");
    require(metrics.prometheus().find(
                "iot_engine_alert_active{alert=\"outbox_dead_lettered\"} 1") !=
                std::string::npos,
            "active operational alert is missing from metrics");
    require(metrics.setAlertState("outbox_dead_lettered", false, "value=0, threshold=1"),
            "clearing an alert did not report a transition");
    require(metrics.healthJson().find("\"status\":\"ready\"") != std::string::npos,
            "cleared operational alert did not restore health status");
}

void testInjectedSharedState() {
    const auto auth = source("service/modules/system/auth/auth.service.h");
    require(auth.find("explicit AuthService(LoginRateLimiter& limiter)") != std::string::npos,
            "auth service does not inject its rate limiter");
    require(auth.find("LoginFailureCounter::key(username)") != std::string::npos,
            "login rate limiting is not shared through Redis");
    require(auth.find("unordered_map") == std::string::npos,
            "login rate limiting still uses process-local state");
    const auto command = source("service/modules/command/command.service.h");
    require(command.find("explicit CommandService(service::device::DeviceAccessService&") !=
                std::string::npos,
            "command service does not inject device authorization");
}

void testSymmetricServiceWorkers() {
    const auto server = source("service/server.cpp");
    require(server.find("workers.front()") == std::string::npos &&
                server.find("workers.back()") == std::string::npos,
            "a Service task is assigned to a privileged Worker");
    require(server.find("registerServiceWorkerLifecycle(*owner, worker, index, count, collectors)") != std::string::npos,
            "Service Workers do not use the same component assembly");
    require(server.find("std::vector<std::shared_ptr<ServiceWorkerComponents>> workers") != std::string::npos,
            "Service Workers share one component owner");

    const auto freshness = source("service/features/telemetry/telemetry.runtime.h");
    const auto reconciler = source("service/features/configuration/configuration.runtime.h");
    const auto webhook = source("service/features/access/access.runtime.h");
    const auto edge = source("service/features/edge/projection/projection.runtime.h");
    for (const auto* component : {&freshness, &reconciler, &webhook, &edge}) {
        require(component->find("workers_.front().post") == std::string::npos &&
                    component->find("workers_.back().post") == std::string::npos,
                "a worker-local task still selects front/back");
        require(component->find("+= serviceWorkerCount_") == std::string::npos,
                "business work is still assigned by fixed worker partitions");
        require(component->find("claimGroupMany") != std::string::npos,
                "a background task cannot recover previously claimed work");
    }
    require(freshness.find("WorkerStreamTask::Freshness") != std::string::npos,
            "freshness task does not wait on its Worker-local wake channel");
    require(webhook.find("stream::event()") != std::string::npos,
             "webhook delivery does not use independent work claims");
    require(webhook.find("stream::sessionChanges()") !=
                std::string::npos &&
                webhook.find("session::ensure(context)") != std::string::npos,
            "access-session projection does not claim shared work with idempotent startup");
    require(edge.find("projector_stream::stream(workerIndex, instance)") != std::string::npos,
             "edge projection is not isolated by accepting Worker");

    for (const auto path : {
             "service/utils/redis.h",
             "service/common/message.h",
             "service/features/access/access.runtime.h",
             "service/features/command/command.runtime.h",
             "service/features/configuration/configuration.runtime.h",
             "service/features/telemetry/telemetry.runtime.h",
             "service/features/edge/edge.runtime.h",
         }) {
        const auto content = source(path);
        require(content.find("repairLegacyReadSet") == std::string::npos &&
                    content.find("eraseDrainedStream") == std::string::npos &&
                    content.find("message::shard") == std::string::npos &&
                    content.find("legacyStream") == std::string::npos,
                "worker queues still contain removed compatibility paths");
    }
}

void testWorkerStreamMultiplexing() {
    const auto server = source("service/server.cpp");
    require(server.find("serviceRedis.blocking_pool_size_per_worker_ = 4") !=
                std::string::npos,
            "Service Workers need bounded multiplexer, live relay, change feed and RPC readers");
    require(server.find("owner->multiplexer->configure(worker, index)") != std::string::npos &&
                server.find("m->start(worker, index)") != std::string::npos,
            "worker-local Stream multiplexer is not in the application lifecycle");
    require(service::message::rpc::Contract::requests("instance", 0) !=
                service::message::rpc::Contract::requests("instance", 1),
            "RPC requests can be consumed by another Service Worker");
    require(service::message::live::workerStream("instance", 0) !=
                service::message::live::workerStream("instance", 1),
            "directed live messages can reach another Service Worker");

    for (const auto path : {
             "service/features/telemetry/telemetry.runtime.h",
             "service/features/command/command.runtime.h",
             "service/features/access/access.runtime.h",
             "service/features/configuration/configuration.runtime.h",
             "service/features/edge/projection/projection.runtime.h",
        }) {
        const auto consumer = source(path);
        require(consumer.find("workerStreamMultiplexer().wait") !=
                        std::string::npos,
                "Service Stream consumer does not use the worker-local multiplexer");
        require(consumer.find("readGroupBlocking") == std::string::npos,
                "Service Stream consumer still owns a Redis blocking connection");
    }
}

} // namespace

int main() {
    try {
        testServiceTopLevelLayout();
        testModuleExecutorDetection();
        testModuleFileLayout();
        testFeatureFileLayout();
        testCommonFileLayout();
        testIncludeGraphBoundaries();
        testFeatureDomainBoundaries();
        testStorageOwnership();
        testLifecycleOrder();
        testLifecycleRollback();
        testMessageEnvelope();
        testExplicitOutbox();
        testOutboxOperations();
        testOperationalAlerts();
        testInjectedSharedState();
        testSymmetricServiceWorkers();
        testWorkerStreamMultiplexing();
        std::cout << "architecture tests passed\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "architecture test failed: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
