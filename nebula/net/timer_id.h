#pragma once

#include <cstdint>

namespace nebula::net
{

class TimerId
{
public:
    TimerId() = default;

    explicit TimerId(std::uint64_t value)
        : value_(value)
    {
    }

    [[nodiscard]] bool Valid() const noexcept
    {
        return value_ != 0;
    }

    [[nodiscard]] std::uint64_t Value() const noexcept
    {
        return value_;
    }

private:
    std::uint64_t value_{0};
};

}  // namespace nebula::net
