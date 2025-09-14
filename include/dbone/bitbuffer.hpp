#pragma once
#include <vector>
#include <cstdint>
#include <string>
#include <stdexcept>
#include <iomanip>
#include <iostream>

class BitBuffer {
public:

    void printHex(size_t bytesPerLine = 16) const {
        std::cout << "BitBuffer (size=" << data_.size() << " bytes):\n";
        for (size_t i = 0; i < data_.size(); i += bytesPerLine) {
            // print offset
            std::cout << std::setw(6) << std::setfill('0') << std::hex << i << " : ";
            // print hex values
            for (size_t j = 0; j < bytesPerLine && i + j < data_.size(); j++) {
                std::cout << std::setw(2) << std::setfill('0')
                          << static_cast<int>(data_[i + j]) << " ";
            }
            std::cout << "\n";
        }
        std::cout << std::dec; // reset back to decimal
    }

    // ---- Writing API ----
    void putBit(bool value) {
        if (writeBitOffset_ == 0) {
            data_.push_back(0);
        }
        if (value) {
            data_.back() |= (1u << writeBitOffset_);
        }
        writeBitOffset_ = (writeBitOffset_ + 1) % 8;
    }

    void putBits(uint64_t value, int count) {
        for (int i = 0; i < count; ++i) {
            putBit((value >> i) & 1u);
        }
    }

    void putU8(uint8_t v)   { putBits(v, 8); }
    void putU16(uint16_t v) { putBits(v, 16); }
    void putU32(uint32_t v) { putBits(v, 32); }
    void putI64(int64_t v) {
        for (int i = 7; i >= 0; --i)
            data_.push_back((v >> (i * 8)) & 0xFF);
    }

    void putString(const std::string &s) {
        putU16(static_cast<uint16_t>(s.size()));
        for (unsigned char c : s) putU8(c);
    }

    // Align writer to next byte (pads with 0 bits if needed)
    void alignByte() {
        while (writeBitOffset_ != 0) {
            putBit(false);
        }
    }

    // ---- Reading API ----
    void resetRead() {
        readByteIndex_ = 0;
        readBitOffset_ = 0;
    }

    bool getBit() {
        if (readByteIndex_ >= data_.size()) {
            throw std::out_of_range("BitBuffer::getBit past end");
        }
        bool bit = (data_[readByteIndex_] >> readBitOffset_) & 1u;
        readBitOffset_++;
        if (readBitOffset_ == 8) {
            readBitOffset_ = 0;
            readByteIndex_++;
        }
        return bit;
    }

    uint32_t getBits(int count) {
        uint32_t v = 0;
        for (int i = 0; i < count; ++i) {
            if (getBit()) {
                v |= (1u << i);
            }
        }
        return v;
    }

    uint8_t  getU8()  { return static_cast<uint8_t>(getBits(8)); }
    uint16_t getU16() { return static_cast<uint16_t>(getBits(16)); }
    uint32_t getU32() { return static_cast<uint32_t>(getBits(32)); }

    std::string getString() {
        uint16_t len = getU16();
        std::string s;
        s.reserve(len);
        for (int i = 0; i < len; ++i) {
            s.push_back(static_cast<char>(getU8()));
        }
        return s;
    }

    // ---- Accessors ----
    const std::vector<uint8_t>& bytes() const { return data_; }
    std::vector<uint8_t>&       bytes()       { return data_; }

    size_t size() const { return data_.size(); }

private:
    std::vector<uint8_t> data_;

    // writer state
    int writeBitOffset_ = 0; // 0..7 inside current byte

    // reader state
    size_t readByteIndex_ = 0;
    int    readBitOffset_ = 0; // 0..7 inside current byte
};
