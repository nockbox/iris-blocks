use iris_ztd::{Digest, Noun, NounDecode, NounEncode};

pub mod accounting;
pub mod chain_activations;
#[cfg(feature = "binary")]
pub mod cli;
pub mod db;
pub mod layers;
mod rt;
pub mod scry;
pub mod sqlite_raw;

#[cfg(feature = "wasm")]
pub mod wasm;

#[derive(Clone, Copy)]
pub struct StringDigest(Digest);

impl core::fmt::Display for StringDigest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl core::fmt::Debug for StringDigest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl NounEncode for StringDigest {
    fn to_noun(&self) -> Noun {
        self.0.to_string().to_noun()
    }
}

impl NounDecode for StringDigest {
    fn from_noun(noun: &Noun) -> Option<Self> {
        let s: String = NounDecode::from_noun(noun)?;
        Some(Self(Digest::try_from(&*s).ok()?))
    }
}
