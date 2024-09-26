//! Implementation of consensus layer messages[ClayerSignature]
use alloy_rlp::{Decodable, Encodable};
use reth_codecs_derive::add_arbitrary_tests;
use reth_primitives::Signature;

/// Consensus layer signature
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct ClayerSignature(pub Signature);

impl Encodable for ClayerSignature {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        self.0.encode(out)
    }

    fn length(&self) -> usize {
        self.0.payload_len()
    }
}

impl Decodable for ClayerSignature {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        Ok(Self(Signature::decode(buf)?))
    }
}
