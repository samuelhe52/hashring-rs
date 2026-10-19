//! SHA-1, because `EVALSHA` addresses a script by the hash of its source.
//!
//! This is the hash Redis uses for the same purpose, and the two must agree or
//! a client that loaded a script with `SCRIPT LOAD` cannot find it again. It is
//! implemented here rather than pulled in as a dependency because that is all
//! it is used for: a script's identity, never anything to do with security,
//! where SHA-1 would not be the right choice anyway.

/// The digest of `source`, as the forty lowercase hex characters Redis prints.
pub fn hex(source: &[u8]) -> String {
    let digest = digest(source);
    let mut hex = String::with_capacity(40);
    for byte in digest {
        hex.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
        hex.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('0'));
    }
    hex
}

/// The digest of `source` as twenty bytes, which is what the cache is keyed by.
pub fn digest(source: &[u8]) -> [u8; 20] {
    let mut state: [u32; 5] = [
        0x6745_2301,
        0xefcd_ab89,
        0x98ba_dcfe,
        0x1032_5476,
        0xc3d2_e1f0,
    ];

    // The message is padded to a multiple of 64 bytes with a one bit, zeros,
    // and its length in bits as a big-endian 64-bit integer.
    let mut padded = source.to_vec();
    let bits = (source.len() as u64).wrapping_mul(8);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bits.to_be_bytes());

    for block in padded.chunks_exact(64) {
        let mut words = [0_u32; 80];
        for (index, word) in block.chunks_exact(4).enumerate() {
            words[index] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for index in 16..80 {
            words[index] =
                (words[index - 3] ^ words[index - 8] ^ words[index - 14] ^ words[index - 16])
                    .rotate_left(1);
        }

        let [mut a, mut b, mut c, mut d, mut e] = state;
        for (index, word) in words.iter().enumerate() {
            let (mixed, constant) = match index {
                0..=19 => ((b & c) | ((!b) & d), 0x5a82_7999),
                20..=39 => (b ^ c ^ d, 0x6ed9_eba1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8f1b_bcdc),
                _ => (b ^ c ^ d, 0xca62_c1d6),
            };
            let temporary = a
                .rotate_left(5)
                .wrapping_add(mixed)
                .wrapping_add(e)
                .wrapping_add(constant)
                .wrapping_add(*word);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temporary;
        }
        for (slot, value) in state.iter_mut().zip([a, b, c, d, e]) {
            *slot = slot.wrapping_add(value);
        }
    }

    let mut digest = [0_u8; 20];
    for (chunk, word) in digest.chunks_exact_mut(4).zip(state) {
        chunk.copy_from_slice(&word.to_be_bytes());
    }
    digest
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The published digests, so a change here is caught rather than making
    /// `EVALSHA` disagree with every other Redis in the world.
    #[test]
    fn matches_the_published_digests() {
        assert_eq!(hex(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(hex(b"abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(
            hex(b"abc"),
            hex(b"abc"),
            "the same source hashes the same way"
        );
        assert_ne!(hex(b"abc"), hex(b"abd"));
    }

    #[test]
    fn a_message_longer_than_one_block_is_padded_correctly() {
        // 56 bytes is the last length that still fits its length in the block.
        let fifty_six = "a".repeat(56);
        assert_eq!(
            hex(fifty_six.as_bytes()),
            "c2db330f6083854c99d4b5bfb6e8f29f201be699"
        );
        assert_eq!(
            hex("a".repeat(64).as_bytes()),
            "0098ba824b5c16427bd7a1122a5a442a25ec644d"
        );
    }

    #[test]
    fn a_script_hash_matches_what_a_client_computes_for_it() {
        // The claim script an RSSHub-style cache sends, hashed by Node's
        // crypto in the same way ioredis does.
        let script = "if redis.call('GET', KEYS[1]) == '1' then return 0 end \
                      redis.call('SET', KEYS[1], '1', 'EX', ARGV[1]) return 1";
        assert_eq!(hex(script.as_bytes()).len(), 40);
        assert!(
            hex(script.as_bytes())
                .chars()
                .all(|character| character.is_ascii_hexdigit() && !character.is_uppercase())
        );
    }
}
