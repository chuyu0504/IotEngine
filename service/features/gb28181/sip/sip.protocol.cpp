#include "service/features/gb28181/sip/sip.protocol.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <chrono>
#include <initializer_list>
#include <iomanip>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <unordered_map>

namespace service::gb28181::sip {

namespace {

std::string md5Hex(const std::string& input) {
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int digestLength = 0;

    auto* context = EVP_MD_CTX_new();
    EVP_DigestInit_ex(context, EVP_md5(), nullptr);
    EVP_DigestUpdate(context, input.data(), input.size());
    EVP_DigestFinal_ex(context, digest.data(), &digestLength);
    EVP_MD_CTX_free(context);

    std::ostringstream output;
    output << std::hex << std::setfill('0');
    for (unsigned int i = 0; i < digestLength; ++i) {
        output << std::setw(2) << static_cast<int>(digest[i]);
    }
    return output.str();
}

std::string hmacSha256Hex(std::string_view secret, std::string_view payload) {
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int digestLength{};
    if (HMAC(EVP_sha256(), secret.data(), static_cast<int>(secret.size()), reinterpret_cast<const unsigned char*>(payload.data()), payload.size(), digest.data(), &digestLength) == nullptr) {
        return {};
    }
    std::ostringstream output;
    output << std::hex << std::setfill('0');
    for (unsigned int index = 0; index < digestLength; ++index) {
        output << std::setw(2) << static_cast<unsigned int>(digest[index]);
    }
    return output.str();
}

bool constantEqual(std::string_view left, std::string_view right) {
    return left.size() == right.size() &&
        CRYPTO_memcmp(left.data(), right.data(), left.size()) == 0;
}

std::unordered_map<std::string, std::string> parseDigestParams(std::string value) {
    constexpr auto prefix = "Digest ";
    if (value.rfind(prefix, 0) == 0) {
        value.erase(0, std::char_traits<char>::length(prefix));
    }

    std::unordered_map<std::string, std::string> params;
    std::string key;
    std::string current;
    bool inQuote = false;

    auto flush = [&]() {
        const auto equal = current.find('=');
        if (equal == std::string::npos) {
            current.clear();
            return;
        }
        auto name = current.substr(0, equal);
        auto parsedValue = current.substr(equal + 1);
        while (!name.empty() && std::isspace(static_cast<unsigned char>(name.front()))) {
            name.erase(name.begin());
        }
        while (!name.empty() && std::isspace(static_cast<unsigned char>(name.back()))) {
            name.pop_back();
        }
        if (parsedValue.size() >= 2 && parsedValue.front() == '"' && parsedValue.back() == '"') {
            parsedValue = parsedValue.substr(1, parsedValue.size() - 2);
        }
        params[name] = parsedValue;
        current.clear();
    };

    for (char c : value) {
        if (c == '"') {
            inQuote = !inQuote;
        }
        if (c == ',' && !inQuote) {
            flush();
            continue;
        }
        current.push_back(c);
    }
    flush();

    return params;
}

std::optional<std::int64_t> nonceTimestamp(std::string_view nonce, std::string_view secret) {
    const auto first = nonce.find('.');
    const auto second =
        first == std::string_view::npos ? std::string_view::npos
                                        : nonce.find('.', first + 1);
    if (first == std::string_view::npos || second == std::string_view::npos) {
        return std::nullopt;
    }
    const auto payload = nonce.substr(0, second);
    const auto signature = nonce.substr(second + 1);
    const auto expected = hmacSha256Hex(secret, payload);
    if (!constantEqual(signature, expected)) {
        return std::nullopt;
    }
    std::int64_t timestamp{};
    const auto parsed =
        std::from_chars(nonce.data(), nonce.data() + first, timestamp, 16);
    if (parsed.ec != std::errc{} || parsed.ptr != nonce.data() + first) {
        return std::nullopt;
    }
    return timestamp;
}

std::string trim(std::string value) {
    const auto notSpace = [](unsigned char c) {
        return !std::isspace(c);
    };
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), notSpace));
    value.erase(std::find_if(value.rbegin(), value.rend(), notSpace).base(), value.end());
    return value;
}

std::string lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

std::string sipUriHost(std::string_view host) {
    if (host.find(':') != std::string_view::npos && !host.starts_with('[')) {
        return "[" + std::string(host) + "]";
    }
    return std::string(host);
}

void appendAll(std::string& output, std::initializer_list<std::string_view> parts) {
    for (const auto part : parts) {
        output += part;
    }
}

} // namespace

std::string DigestAuth::makeNonce(const std::string& secret) {
    std::array<unsigned char, 16> bytes{};
    if (RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) != 1) {
        throw std::runtime_error("Unable to create SIP digest nonce");
    }

    const auto timestamp =
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()
        )
            .count();
    std::ostringstream payload;
    payload << std::hex << timestamp << '.';
    for (const auto byte : bytes) {
        payload << std::setw(2) << std::setfill('0')
                << static_cast<unsigned int>(byte);
    }
    const auto value = payload.str();
    return value + "." + hmacSha256Hex(secret, value);
}

std::optional<DigestAuth::Proof>
DigestAuth::verifyRegister(const SipMessage& message, const std::string& realm, const std::string& password, const std::string& nonceSecret, int nonceTtlSeconds) {
    const auto authorization = message.header("Authorization");
    if (authorization.empty() || nonceTtlSeconds <= 0) {
        return std::nullopt;
    }

    const auto params = parseDigestParams(authorization);
    const auto username = params.find("username");
    const auto response = params.find("response");
    const auto nonce = params.find("nonce");
    const auto uri = params.find("uri");

    if (username == params.end() || response == params.end() ||
        nonce == params.end() || uri == params.end() ||
        uri->second != message.requestUri) {
        return std::nullopt;
    }
    if (const auto suppliedRealm = params.find("realm");
        suppliedRealm != params.end() && suppliedRealm->second != realm) {
        return std::nullopt;
    }

    const auto issuedAt = nonceTimestamp(nonce->second, nonceSecret);
    const auto now =
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()
        )
            .count();
    if (!issuedAt || *issuedAt > now + 5 || now - *issuedAt > nonceTtlSeconds) {
        return std::nullopt;
    }

    const auto ha1 = md5Hex(username->second + ":" + realm + ":" + password);
    const auto ha2 = md5Hex(message.method + ":" + uri->second);

    const auto qop = params.find("qop");
    std::string expected;
    std::uint32_t nonceCount = 1;
    if (qop != params.end()) {
        const auto nc = params.find("nc");
        const auto cnonce = params.find("cnonce");
        if (qop->second != "auth" || nc == params.end() || cnonce == params.end()) {
            return std::nullopt;
        }
        const auto parsed = std::from_chars(nc->second.data(), nc->second.data() + nc->second.size(), nonceCount, 16);
        if (parsed.ec != std::errc{} ||
            parsed.ptr != nc->second.data() + nc->second.size() ||
            nonceCount == 0) {
            return std::nullopt;
        }
        expected = md5Hex(ha1 + ":" + nonce->second + ":" + nc->second + ":" + cnonce->second + ":" + qop->second + ":" + ha2);
    } else {
        expected = md5Hex(ha1 + ":" + nonce->second + ":" + ha2);
    }

    if (!constantEqual(expected, response->second)) {
        return std::nullopt;
    }
    return Proof{
        .nonce = nonce->second,
        .username = username->second,
        .nonceCount = nonceCount,
    };
}

std::optional<SipMessage> SipMessage::parse(const std::string& raw) {
    const auto headerEnd = raw.find("\r\n\r\n");
    if (headerEnd == std::string::npos) {
        return std::nullopt;
    }

    SipMessage message;

    std::istringstream lines(raw.substr(0, headerEnd));
    if (!std::getline(lines, message.startLine)) {
        return std::nullopt;
    }
    if (!message.startLine.empty() && message.startLine.back() == '\r') {
        message.startLine.pop_back();
    }

    std::istringstream start(message.startLine);
    if (message.startLine.rfind("SIP/2.0", 0) == 0) {
        std::string version;
        std::string status;
        start >> version >> status;
        const auto [end, error] =
            std::from_chars(status.data(), status.data() + status.size(), message.statusCode);
        if (version != "SIP/2.0" || error != std::errc{} || end != status.data() + status.size() ||
            message.statusCode < 100 || message.statusCode > 699) {
            return std::nullopt;
        }
        std::getline(start, message.reasonPhrase);
        message.reasonPhrase = trim(message.reasonPhrase);
    } else {
        std::string version;
        std::string extra;
        start >> message.method >> message.requestUri >> version;
        if (version != "SIP/2.0" || (start >> extra)) {
            return std::nullopt;
        }
    }

    if (message.method.empty() && message.statusCode == 0) {
        return std::nullopt;
    }

    std::optional<std::size_t> contentLength;
    std::string line;
    while (std::getline(lines, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        const auto colon = line.find(':');
        if (colon == std::string::npos) {
            continue;
        }
        auto name = lower(trim(line.substr(0, colon)));
        auto value = trim(line.substr(colon + 1));
        if (name == "content-length" || name == "l") {
            std::size_t parsed{};
            const auto [end, error] =
                std::from_chars(value.data(), value.data() + value.size(), parsed);
            if (value.empty() || error != std::errc{} || end != value.data() + value.size() ||
                (contentLength && *contentLength != parsed)) {
                return std::nullopt;
            }
            contentLength = parsed;
        }
        message.headers[name] = value;
    }

    const auto body = std::string_view(raw).substr(headerEnd + 4);
    if (contentLength && body.size() != *contentLength) {
        return std::nullopt;
    }
    message.body.assign(body);

    return message;
}

std::string SipMessage::header(const std::string& name) const {
    auto key = lower(name);
    const auto iter = headers.find(key);
    if (iter == headers.end()) {
        return {};
    }
    return iter->second;
}

std::string SipRequestHead::serialize(std::string_view body) const {
    const auto platformAddress = sipUriHost(platformHost) + ":" + std::to_string(platformPort);
    std::string request;
    request.reserve(512 + subject.size() + body.size());
    appendAll(request, { method, " sip:", targetUser, "@", sipUriHost(targetHost), ":", std::to_string(targetPort), " SIP/2.0\r\n" });
    appendAll(request, { "Via: SIP/2.0/", transport, " ", platformAddress, ";branch=", branch, "\r\n" });
    appendAll(request, { "From: <sip:", platformId, "@", domain, ">;tag=", fromTag, "\r\n" });
    if (echoedTo) {
        appendAll(request, { "To: ", *echoedTo, "\r\n" });
    } else {
        appendAll(request, { "To: <sip:", targetUser, "@", domain, ">" });
        if (toTag) {
            appendAll(request, { ";tag=", *toTag });
        }
        request += "\r\n";
    }
    appendAll(request, { "Call-ID: ", callId, "\r\n", "CSeq: ", std::to_string(cseq), " ", method, "\r\n" });
    if (contact) {
        appendAll(request, { "Contact: <sip:", platformId, "@", platformAddress, ">\r\n" });
    }
    request += "Max-Forwards: 70\r\n";
    if (!subject.empty()) {
        appendAll(request, { "Subject: ", subject, "\r\n" });
    }
    request += "User-Agent: gb28181-platform-cpp\r\n";
    if (!contentType.empty()) {
        appendAll(request, { "Content-Type: ", contentType, "\r\n" });
    }
    appendAll(request, { "Content-Length: ", std::to_string(body.size()), "\r\n\r\n", body });
    return request;
}

} // namespace service::gb28181::sip
