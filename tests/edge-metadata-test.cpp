#include <iostream>
#include <stdexcept>
#include <string>

#include "service/common/message.h"
#include "service/features/edge/edge.entity.h"

namespace metadata = service::edge::metadata;

void require(bool condition, const char* message) {
    if (!condition)
        throw std::runtime_error(message);
}

int main() {
    try {
        const metadata::Device input{
            .linkId = "019fd9f6-4be5-7272-a194-9e571bce848d",
            .deviceCode = "REMOTE:IO:01",
            .protocol = "Modbus",
            .storagePolicy = "change",
            .onlineWindowMs = 300000,
        };
        const auto encoded = metadata::encode(input);
        const auto decoded = metadata::decode(encoded);
        require(decoded.has_value(), "metadata round trip failed");
        require(decoded->linkId == input.linkId, "link id changed during metadata round trip");
        require(decoded->deviceCode == input.deviceCode,
                "device code changed during metadata round trip");
        require(decoded->protocol == input.protocol,
                "protocol changed during metadata round trip");
        require(decoded->storagePolicy == input.storagePolicy,
                "storage policy changed during metadata round trip");
        require(decoded->onlineWindowMs == input.onlineWindowMs,
                "online timeout changed during metadata round trip");
        require(!metadata::decode(encoded + "trailing").has_value(),
                "metadata decoder accepted trailing bytes");
        require(!metadata::decode("1:a1:b1:c1:01:0").has_value(),
                "metadata decoder accepted invalid timing values");
        require(!metadata::decode("18446744073709551615:a").has_value(),
                "metadata decoder accepted an overflowing length");
        require(!metadata::decode("2:a").has_value(),
                "metadata decoder accepted a truncated field");
        require(!metadata::decode("1:a1:b1:c3:bad4:1000").has_value(),
                "metadata decoder accepted an invalid storage policy");
        for (std::size_t size = 0; size < 4096; ++size) {
            const std::string malformed(size, static_cast<char>('a' + size % 26));
            require(!metadata::decode(malformed).has_value(),
                    "metadata decoder accepted an unframed payload");
        }
        require(service::message::edge::metadataKey("node") == "iot:edge:metadata:node",
                "metadata Redis key is unstable");
        std::cout << "edge metadata tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "edge metadata test failed: " << error.what() << '\n';
        return 1;
    }
}
