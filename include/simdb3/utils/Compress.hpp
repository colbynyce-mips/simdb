// <Compress> -*- C++ -*-

#pragma once

#include <vector>
#include <zlib.h>

namespace simdb3
{

/// Compression modes of operation.
enum class CompressionModes
{
    COMPRESSED,
    UNCOMPRESSED
};

/// Perform zlib compression on the single data vector of stats values.
template <typename T>
inline void compressDataVec(const std::vector<T>& in, std::vector<char>& out, int compression_level = Z_DEFAULT_COMPRESSION)
{
    if (in.empty()) {
        out.clear();
        return;
    }

    z_stream defstream;
    defstream.zalloc = Z_NULL;
    defstream.zfree = Z_NULL;
    defstream.opaque = Z_NULL;

    auto num_bytes_before = in.size() * sizeof(T);
    defstream.avail_in = (uInt)(num_bytes_before);
    defstream.next_in = (Bytef*)(in.data());

    // Compression can technically result in a larger output, although it is not
    // likely except for possibly very small input vectors. There is no deterministic
    // value for the maximum number of bytes after decompression, but we can choose
    // a very safe minimum.
    auto max_bytes_after = num_bytes_before * 2;
    if (max_bytes_after < 1000) {
        max_bytes_after = 1000;
    }
    out.resize(max_bytes_after);

    defstream.avail_out = (uInt)(out.size());
    defstream.next_out = (Bytef*)(out.data());

    deflateInit(&defstream, compression_level);
    deflate(&defstream, Z_FINISH);
    deflateEnd(&defstream);

    auto num_bytes_after = (int)defstream.total_out;
    out.resize(num_bytes_after);
}

} // namespace simdb3
